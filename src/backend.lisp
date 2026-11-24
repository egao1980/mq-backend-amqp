(in-package #:mq-backend-amqp)

(defclass amqp-backend (mq-protocol:mq-backend)
  ((connection :initarg :connection :accessor amqp-backend-connection
               :initform nil)
   (channel :initarg :channel :accessor amqp-backend-channel :initform 1)
   (owns-connection :initarg :owns-connection :initform nil
                    :reader %owns-connection-p))
  (:documentation
   "mq-protocol backend over an AMQP 0-9-1 connection. CONNECT-AMQP-BACKEND /
WITH-AMQP-BACKEND open one to a broker; MAKE-AMQP-BACKEND wraps a given
connection or stream (OCTET-PIPE in tests)."))

(defun make-amqp-backend (&key connection stream (channel 1))
  (make-instance 'amqp-backend
                 :connection (or connection
                                 (and stream
                                      (make-amqp-connection :stream stream
                                                            :channel channel)))
                 :channel channel))

(defun connect-amqp-backend (&rest args &key (channel 1) &allow-other-keys)
  "Open a connection to a broker (OPEN-AMQP-CONNECTION takes ARGS: :host
:port :user :password :vhost :tls :url :heartbeat ...) and return a backend
that owns it; CLOSE releases it."
  (make-instance 'amqp-backend
                 :connection (apply #'open-amqp-connection args)
                 :channel channel
                 :owns-connection t))

(defmacro with-amqp-backend ((var &rest args) &body body)
  "Bind VAR to (CONNECT-AMQP-BACKEND ,@ARGS) around BODY, then close the
connection (Connection.Close, socket released) however BODY exits."
  `(let ((,var (connect-amqp-backend ,@args)))
     (unwind-protect (progn ,@body)
       (close ,var))))

(defmethod close ((backend amqp-backend) &key abort)
  "Close the connection if the backend opened it. → T if it was open."
  (let ((conn (amqp-backend-connection backend)))
    (and conn (%owns-connection-p backend)
         (close-amqp-connection conn :abort abort))))

(defmethod mq-protocol:mq-backend-capabilities ((backend amqp-backend))
  (mq-protocol:make-mq-capabilities
   :publish-p t
   :subscribe-p t
   :poll-p t
   :ack-p t
   :nack-p t
   :transaction-p nil
   :delivery-semantics :at-least-once))

(defun %conn (backend)
  (or (amqp-backend-connection backend)
      (error 'mq-protocol:mq-connection-error
             :backend backend
             :message "amqp-backend has no connection")))

(defun %dest-string (destination)
  (cond
    ((null destination) "")
    ((stringp destination) destination)
    ((symbolp destination) (string-downcase (symbol-name destination)))
    (t (princ-to-string destination))))

(defun %destination-pair (destination message)
  (cond
    ((and (consp destination) (not (keywordp (car destination))))
     (values (car destination) (cdr destination)))
    (t
     (values (or (mq-protocol:mq-message-exchange message) "")
             (or (mq-protocol:mq-message-topic message)
                 (%dest-string destination))))))

(defun %live-p (conn)
  "Does CONN own a socket (a broker on the other end that answers)?"
  (and (%connection-socket conn) t))

(defmacro %with-backend-connection ((conn backend) &body body)
  "Run BODY with CONN, under its lock, after a heartbeat tick and, on a live
connection, with the channel (re)opened. A closed connection signals
MQ-CONNECTION-ERROR. This is one backend operation: broker notifications
read during it are signalled when it (or the outermost one) is done, with
the lock released (%WITH-NOTIFICATIONS)."
  `(let ((,conn (%conn ,backend)))
     (%with-notifications (,conn)
                          (bt2:with-recursive-lock-held ((%connection-lock ,conn))
                            (%check-open ,conn)
                            (when (%live-p ,conn)
                              (amqp-heartbeat-tick ,conn)
                              (%ensure-channel ,conn))
                            (%with-transport-errors ((%endpoint ,conn))
                                                    ,@body)))))

(defun %body-frames (conn payload channel)
  "PAYLOAD split into body frames that fit the negotiated frame-max."
  (let* ((frame-max (amqp-connection-frame-max conn))
         (chunk (if (plusp frame-max) (- frame-max 8) (max 1 (length payload)))))
    (if (zerop (length payload))
        '()
        (loop for start from 0 below (length payload) by chunk
              collect (encode-body-frame
                       (subseq payload start (min (length payload) (+ start chunk)))
                       :channel channel)))))

(defmethod mq-protocol:publish ((backend amqp-backend) destination message &key)
  (let* ((msg (if (mq-protocol:mq-message-p message)
                  message
                  (mq-protocol:make-mq-message :payload message)))
         (payload (mq-protocol:mq-message-payload msg))
         (channel (amqp-backend-channel backend)))
    (%with-backend-connection (conn backend)
                              (multiple-value-bind (exchange routing-key)
                                  (%destination-pair destination msg)
                                (write-amqp-method conn :basic-publish
                                                   :channel channel
                                                   :exchange (or exchange "")
                                                   :routing-key (or routing-key "")
                                                   :mandatory nil
                                                   :immediate nil)
                                (write-amqp-frame
                                 conn
                                 (encode-content-header
                                  :class-id 60
                                  :body-size (length payload)
                                  :channel channel
                                  :properties (append
                                               (and (mq-protocol:mq-message-headers msg)
                                                    (list :headers (mq-protocol:mq-message-headers msg)))
                                               (and (mq-protocol:mq-message-key msg)
                                                    (list :correlation-id
                                                          (princ-to-string (mq-protocol:mq-message-key msg)))))))
                                (dolist (frame (%body-frames conn payload channel))
                                  (write-amqp-frame conn frame))))
    msg))

(defmethod mq-protocol:subscribe ((backend amqp-backend) destination handler &key
                                  (consumer-tag "") no-ack no-wait)
  "Basic.Consume on queue DESTINATION. On a live connection the broker's
Consume-Ok is awaited (unless NO-WAIT) and its consumer tag kept; with
NO-WAIT and no CONSUMER-TAG the client picks the tag (cl-stack.ctag.N), so
every consumer's queue is known. Messages delivered to it are handed out by
POLL-MESSAGES of DESTINATION."
  (let ((tag consumer-tag)
        (queue (%dest-string destination)))
    (%with-backend-connection (conn backend)
                              (when (and no-wait (or (null tag) (string= tag "")))
                                (setf tag (format nil "cl-stack.ctag.~D" (1+ (length (%consumer-queues conn))))))
                              (let ((args (list :channel (amqp-backend-channel backend)
                                                :queue queue
                                                :consumer-tag (or tag "")
                                                :no-local nil
                                                :no-ack no-ack
                                                :exclusive nil
                                                :no-wait no-wait)))
                                (if (and (%live-p conn) (not no-wait))
                                    (setf tag (getf (amqp-method-arguments
                                                     (apply #'amqp-rpc conn :basic-consume '(:basic-consume-ok) args))
                                                    :consumer-tag))
                                    (apply #'write-amqp-method conn :basic-consume args))
                                (when (and tag (string/= tag ""))
                                  (push (cons tag queue) (%consumer-queues conn)))))
    (make-instance 'mq-protocol:mq-subscription
                   :destination destination
                   :handler handler
                   :tag tag)))

(defun amqp-declare-queue (backend queue &key passive durable exclusive auto-delete
                                   arguments)
  "Queue.Declare, waiting for Declare-Ok (pika queue_declare). QUEUE \"\" lets
the broker name it. → (values queue-name message-count consumer-count)."
  (%with-backend-connection (conn backend)
                            (let ((args (amqp-method-arguments
                                         (amqp-rpc conn :queue-declare '(:queue-declare-ok)
                                                        :channel (amqp-backend-channel backend)
                                                        :reserved-1 0 :queue (%dest-string queue) :passive passive
                                                        :durable durable :exclusive exclusive :auto-delete auto-delete
                                                        :no-wait nil :arguments arguments))))
                              (values (getf args :queue) (getf args :message-count) (getf args :consumer-count)))))

(defun amqp-declare-exchange (backend exchange &key (type "direct") passive durable
                                      auto-delete internal arguments)
  "Exchange.Declare, waiting for Declare-Ok. → EXCHANGE."
  (%with-backend-connection (conn backend)
                            (amqp-rpc conn :exchange-declare '(:exchange-declare-ok)
                                           :channel (amqp-backend-channel backend)
                                           :reserved-1 0 :exchange exchange :type type :passive passive
                                           :durable durable :auto-delete auto-delete :internal internal
                                           :no-wait nil :arguments arguments))
  exchange)

(defun amqp-bind-queue (backend queue exchange &key (routing-key "") arguments)
  "Queue.Bind, waiting for Bind-Ok. → QUEUE."
  (%with-backend-connection (conn backend)
                            (amqp-rpc conn :queue-bind '(:queue-bind-ok)
                                           :channel (amqp-backend-channel backend)
                                           :reserved-1 0 :queue (%dest-string queue) :exchange exchange
                                           :routing-key routing-key :no-wait nil :arguments arguments))
  queue)

(defun %basic-get (backend conn queue)
  "One Basic.Get. → an AMQP-DELIVERY, or NIL when the queue is empty. The
Get-Ok and its content frames are one reply (%WITH-REPLY-GUARD)."
  (%with-reply-guard (conn)
                     (let ((reply (amqp-rpc conn :basic-get '(:basic-get-ok :basic-get-empty)
                                                 :channel (amqp-backend-channel backend)
                                                 :reserved-1 0 :queue queue :no-ack nil)))
                       (when (eq :basic-get-ok (amqp-method-name reply))
                         (multiple-value-bind (properties body) (%read-content conn)
                           (%delivery-message reply properties body :queue queue))))))

(defun %delivery-tag (message)
  (if (integerp message)
      message
      (or (mq-protocol:mq-message-delivery-tag message) 0)))

(defun %take-pending (conn queue n)
  "Remove and return up to N delivered messages of QUEUE from CONN's
pending list, oldest first. Messages of other queues stay. The caller
holds the connection lock."
  (let ((taken '()) (kept '()))
    (dolist (msg (%pending-deliveries conn))
      (if (and (< (length taken) n)
               (equal queue (%delivery-queue conn msg)))
          (push msg taken)
          (push msg kept)))
    (setf (%pending-deliveries conn) (nreverse kept))
    (nreverse taken)))

(defun %hold-back (conn messages)
  "Put MESSAGES, fetched but not handed out, back at the head of CONN's
pending list, so the next poll of their queue returns them first."
  (when messages
    (bt2:with-recursive-lock-held ((%connection-lock conn))
      (setf (%pending-deliveries conn)
            (append messages (%pending-deliveries conn))))))

(defmethod mq-protocol:poll-messages ((backend amqp-backend) destination
                                      &key (n 1) timeout)
  "Up to N messages from queue DESTINATION (acknowledge them with ACK /
NACK). Messages already delivered to this connection for DESTINATION (to a
consumer on it, see SUBSCRIBE) come first, then Basic.Get. Messages of
other queues stay for polls of those queues. With TIMEOUT (seconds) an
empty queue is retried until a message arrives or the time is up; without
it one attempt is made. Broker notifications read meanwhile are signalled
before the messages are returned; if a handler unwinds, the messages are
kept for the next poll of DESTINATION, not lost."
  (check-type n (integer 1))
  (let* ((queue (%dest-string destination))
         (conn (%conn backend))
         (deadline (and timeout
                        (+ (get-internal-real-time)
                           (round (* timeout internal-time-units-per-second)))))
         (out '())
         (handed-out nil))
    (let ((*amqp-operation-depth* (1+ *amqp-operation-depth*)))
      (loop
        (%with-backend-connection (conn backend)
                                  (unless (%live-p conn)
                                    (error 'mq-protocol:mq-error
                                           :backend backend
                                           :message "poll-messages needs a broker: open the backend with :host or :url"))
                                  (setf out (append out (%take-pending conn queue (- n (length out)))))
                                  (loop while (< (length out) n)
                                        do (let ((msg (%basic-get backend conn queue)))
                                             (if msg
                                                 (setf out (append out (list msg)))
                                                 (return)))))
        (when (or out
                  (null deadline)
                  (>= (get-internal-real-time) deadline))
          (return))
        (sleep (min 0.05 (/ (max 0 (- deadline (get-internal-real-time)))
                            internal-time-units-per-second)))))
    (unwind-protect
        (progn
          (when (zerop *amqp-operation-depth*)
            (%signal-notifications conn))
          (setf handed-out t)
          out)
      (unless handed-out
        (%hold-back conn out)))))

(defmethod mq-protocol:ack ((backend amqp-backend) message &key multiple)
  (%with-backend-connection (conn backend)
                            (write-amqp-method conn :basic-ack
                                               :channel (amqp-backend-channel backend)
                                               :delivery-tag (%delivery-tag message)
                                               :multiple multiple))
  message)

(defmethod mq-protocol:nack ((backend amqp-backend) message &key (requeue t) multiple)
  (%with-backend-connection (conn backend)
                            (write-amqp-method conn :basic-nack
                                               :channel (amqp-backend-channel backend)
                                               :delivery-tag (%delivery-tag message)
                                               :multiple multiple
                                               :requeue requeue))
  message)
