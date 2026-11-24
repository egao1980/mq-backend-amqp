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
POLL-MESSAGES of DESTINATION. With NO-ACK they are settled on arrival, and
ACK / NACK of one signals AMQP-STALE-DELIVERY."
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
          (push (cons tag queue) (%consumer-queues conn))
          (when no-ack
            (push tag (%no-ack-consumers conn))))))
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
          (%delivery-message conn reply properties body :queue queue))))))

(defun %retired-p (conn message)
  "Was MESSAGE delivered on a channel of CONN that has closed since? The
broker requeued it then, so it must not be handed out (or acked) now."
  (let ((tag (mq-protocol:mq-message-delivery-tag message)))
    (and (integerp tag) (<= tag (%tag-offset conn)))))

(defun %given-tag (message)
  (if (integerp message) message (mq-protocol:mq-message-delivery-tag message)))

(defun %outstanding-tag (conn message multiple)
  "The wire delivery tag that acks or nacks MESSAGE on CONN's open channel.
MESSAGE is an AMQP-DELIVERY, another MQ-MESSAGE that carries the tag of
one (a copy), or an integer tag that CONN handed out; 0 with MULTIPLE is
every delivery outstanding on the channel. → (values tag NIL), or (values
NIL reason) when it is not outstanding there (see AMQP-STALE-DELIVERY).
The caller holds the connection lock."
  (let ((tag (%given-tag message))
        (settled (and (typep message 'amqp-delivery) (%delivery-settled message))))
    (cond
     ((and (typep message 'amqp-delivery)
           (not (eq conn (%delivery-connection message))))
      (values nil :other-connection))
     ((eq settled :no-ack) (values nil :no-ack))
     (settled (values nil :settled))
     ((and (eql tag 0) multiple)
      (values 0 nil))
     ((not (and (integerp tag) (plusp tag)))
      (error 'mq-protocol:mq-error
             :message (format nil "~S has no delivery tag to ack or nack" message)))
     ((<= tag (%tag-offset conn)) (values nil :channel-closed))
     ((<= tag (%settled-through conn)) (values nil :settled))
     (t (values (- tag (%tag-offset conn)) nil)))))

(defun %check-multiple (conn tag)
  "A multiple ack or nack of TAG (0: all) settles every delivery up to it
on the channel, those not handed out yet included. Refuse it while one of
those is pending: it would be acked before anyone saw it."
  (let ((unseen (count-if (lambda (m)
                            (or (zerop tag)
                                (<= (mq-protocol:mq-message-delivery-tag m) tag)))
                          (%pending-deliveries conn))))
    (when (plusp unseen)
      (error 'mq-protocol:mq-error
             :message (format nil "a multiple ack / nack ~:[up to delivery ~d~;of every delivery~*~] ~
                                   would settle ~d delivered message~:p not handed out yet; ~
                                   poll them first or ack one by one"
                              (zerop tag) tag unseen)))))

(defun %settle (backend message method &rest fields &key multiple &allow-other-keys)
  "Send METHOD (:BASIC-ACK or :BASIC-NACK) with FIELDS for MESSAGE on
BACKEND's channel. When MESSAGE is not outstanding there nothing is sent
and AMQP-STALE-DELIVERY is signalled, outside the connection lock, with
restart CONTINUE (send nothing, → NIL). → MESSAGE."
  (let ((reason nil))
    (%with-backend-connection (conn backend)
      (multiple-value-bind (wire why) (%outstanding-tag conn message multiple)
        (if why
            (setf reason why)
            (let ((through (if (zerop wire) (%max-tag conn) (%given-tag message))))
              (when multiple
                (%check-multiple conn (if (zerop wire) 0 through)))
              (apply #'write-amqp-method conn method
                     :channel (amqp-backend-channel backend)
                     :delivery-tag wire fields)
              (when (typep message 'amqp-delivery)
                (setf (%delivery-settled message)
                      (if (eq method :basic-ack) :ack :nack)))
              (when multiple
                (setf (%settled-through conn)
                      (max (%settled-through conn) through)))))))
    (if reason
        (restart-case (error 'amqp-stale-delivery :backend backend
                                                  :delivery-tag (%given-tag message)
                                                  :reason reason)
          (continue ()
            :report "Send nothing for this delivery."
            nil))
        message)))

(defun %take-pending (conn queue n)
  "Remove and return up to N delivered messages of QUEUE from CONN's
pending list, oldest first. Messages of other queues stay; messages whose
channel closed since are dropped (the broker requeued them). The caller
holds the connection lock."
  (let ((taken '()) (kept '()))
    (dolist (msg (%pending-deliveries conn))
      (cond
       ((%retired-p conn msg))
       ((and (< (length taken) n)
             (equal queue (%delivery-queue conn msg)))
        (push msg taken))
       (t (push msg kept))))
    (setf (%pending-deliveries conn) (nreverse kept))
    (nreverse taken)))

(defun %hold-back (conn messages)
  "Put MESSAGES, fetched but not handed out, back at the head of CONN's
pending list, so the next poll of their queue returns them first. Those
whose channel closed meanwhile are dropped: the broker requeued them."
  (when messages
    (bt2:with-recursive-lock-held ((%connection-lock conn))
      (setf (%pending-deliveries conn)
            (append (remove-if (lambda (m) (%retired-p conn m)) messages)
                    (%pending-deliveries conn))))))

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
  "Basic.Ack of MESSAGE (an AMQP-DELIVERY, a copy of one, or an integer tag
this connection handed out; MULTIPLE: every delivery up to it, 0 for all).
A delivery that is not outstanding on the open channel (its channel closed
since, it was acked or nacked already, or it came from another connection)
is never sent: AMQP-STALE-DELIVERY, restart CONTINUE. → MESSAGE (NIL when
skipped)."
  (%settle backend message :basic-ack :multiple multiple))

(defmethod mq-protocol:nack ((backend amqp-backend) message &key (requeue t) multiple)
  "Basic.Nack of MESSAGE (REQUEUE: put it back), with ACK's rules for which
deliveries are sent. → MESSAGE (NIL when skipped)."
  (%settle backend message :basic-nack :multiple multiple :requeue requeue))
