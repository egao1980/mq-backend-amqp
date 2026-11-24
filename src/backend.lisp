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
  "Run BODY with CONN, under its lock. On a live connection, first read what
the broker already sent (%DRAIN-INCOMING: a broker Close of the backend's
channel is signalled now, before more requests go to it), tick the
heartbeat and make the backend's channel usable (%ENSURE-CHANNEL). A
closed connection signals MQ-CONNECTION-ERROR. This is one backend
operation: broker notifications read during it are signalled when it (or
the outermost one) is done, with the lock released (%WITH-NOTIFICATIONS)."
  `(let ((,conn (%conn ,backend)))
     (%with-notifications (,conn)
       (bt2:with-recursive-lock-held ((%connection-lock ,conn))
         (%check-open ,conn)
         (when (%live-p ,conn)
           (%drain-incoming ,conn (amqp-backend-channel ,backend))
           (amqp-heartbeat-tick ,conn)
           (%ensure-channel ,conn (amqp-backend-channel ,backend)))
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
  "Basic.Publish of MESSAGE (or a payload) to DESTINATION: a queue name on
the default exchange, or (exchange . routing-key). Every frame is encoded
before any is written, so a message the encoder refuses (a key over 255
octets, a header with no field-table type) signals AMQP-FRAME-ERROR with
nothing sent, as pika does. A write cut short drops the connection: half a
publish on the wire would make the broker read the next request as its
content. A broker Close that came in before is signalled first (see
%WITH-BACKEND-CONNECTION), so a publish to a channel the broker closed
fails instead of being discarded. → the MQ-MESSAGE."
  (let* ((msg (if (mq-protocol:mq-message-p message)
                  message
                  (mq-protocol:make-mq-message :payload message)))
         (payload (mq-protocol:mq-message-payload msg))
         (channel (amqp-backend-channel backend))
         (frames (multiple-value-bind (exchange routing-key)
                     (%destination-pair destination msg)
                   (list* (encode-method-frame :basic-publish
                                               :channel channel
                                               :exchange (or exchange "")
                                               :routing-key (or routing-key "")
                                               :mandatory nil
                                               :immediate nil)
                          (encode-content-header
                           :class-id 60
                           :body-size (length payload)
                           :channel channel
                           :properties (append
                                        (and (mq-protocol:mq-message-headers msg)
                                             (list :headers (mq-protocol:mq-message-headers msg)))
                                        (and (mq-protocol:mq-message-key msg)
                                             (list :correlation-id
                                                   (princ-to-string (mq-protocol:mq-message-key msg))))))
                          (%body-frames (%conn backend) payload channel)))))
    (%with-backend-connection (conn backend)
      (%with-reply-guard (conn)
        (dolist (frame frames)
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
  "One Basic.Get on BACKEND's channel. → an AMQP-DELIVERY, or NIL when the
queue is empty. The Get-Ok and its content frames are one reply
(%WITH-REPLY-GUARD)."
  (let ((channel (amqp-backend-channel backend)))
    (%with-reply-guard (conn)
      (let ((reply (amqp-rpc conn :basic-get '(:basic-get-ok :basic-get-empty)
                                  :channel channel
                                  :reserved-1 0 :queue queue :no-ack nil)))
        (when (eq :basic-get-ok (amqp-method-name reply))
          (multiple-value-bind (properties body) (%read-content conn)
            (%delivery-message conn channel reply properties body :queue queue)))))))

(defun %given-tag (message)
  (if (integerp message) message (mq-protocol:mq-message-delivery-tag message)))

(defun %highest-unsettled (unsettled limit)
  "The highest tag in the UNSETTLED set at or below LIMIT (NIL: any). NIL
when there is none."
  (let ((top nil))
    (maphash (lambda (tag v)
               (declare (ignore v))
               (when (and (or (null limit) (<= tag limit))
                          (or (null top) (> tag top)))
                 (setf top tag)))
             unsettled)
    top))

(defun %outstanding-tag (conn channel message multiple)
  "What acks or nacks MESSAGE on CONN's CHANNEL. MESSAGE is an
AMQP-DELIVERY, another MQ-MESSAGE that carries the tag of one (a copy), or
an integer tag that CONN handed out (tag 0 with MULTIPLE is %SETTLE's, see
%EVERY-HANDED-OUT). → (values wire-tag tag NIL), TAG being the highest
delivery it settles (offset included), or (values NIL NIL reason) when
MESSAGE is not outstanding on CHANNEL (see AMQP-STALE-DELIVERY).
A multiple ack settles every unsettled delivery up to its tag; it is sent
with the highest of them, never with a tag already settled (the broker
would close the channel). A tag above every tag handed out is sent as
given (deliveries read by other means). The caller holds the connection
lock."
  (let* ((state (%channel-state conn channel))
         (offset (%cs-offset state))
         (unsettled (%cs-unsettled state))
         (tag (%given-tag message))
         (delivery (and (typep message 'amqp-delivery) message))
         (settled (and delivery (%delivery-settled delivery))))
    (cond
      ((and delivery (not (eq conn (%delivery-connection delivery))))
       (values nil nil :other-connection))
      ((and delivery (not (eql channel (%delivery-channel delivery))))
       (values nil nil :other-channel))
      ((eq settled :no-ack) (values nil nil :no-ack))
      (settled (values nil nil :settled))
      ((not (and (integerp tag) (plusp tag)))
       (error 'mq-protocol:mq-error
              :message (format nil "~S has no delivery tag to ack or nack" message)))
      ((<= tag offset) (values nil nil :channel-closed))
      ((> tag (%cs-max-tag state)) (values (- tag offset) tag nil))
      (multiple
       (let ((top (%highest-unsettled unsettled tag)))
         (if top
             (values (- top offset) top nil)
             (values nil nil :settled))))
      ((gethash tag unsettled) (values (- tag offset) tag nil))
      (t (values nil nil :settled)))))

(defun %check-multiple (conn channel tag)
  "A multiple ack or nack up to TAG settles every delivery up to it on
CHANNEL, those read but not handed out yet included: kept for a poll, or
fetched by a poll that has not returned them yet. Refuse it while there is
one: it would be acked before anyone saw it."
  (let ((unseen (count-if (lambda (m)
                            (and (eql channel (%delivery-channel m))
                                 (<= (mq-protocol:mq-message-delivery-tag m) tag)))
                          (append (%pending-deliveries conn) (%handing-out conn)))))
    (when (plusp unseen)
      (error 'mq-protocol:mq-error
             :message (format nil "a multiple ack / nack up to delivery ~D would settle ~D ~
                                   delivered message~:P not handed out yet; poll them first ~
                                   or ack one by one"
                              tag unseen)))))

(defun %every-handed-out (conn channel)
  "What an ack or nack of every delivery (tag 0, multiple) settles on CONN's
CHANNEL: the deliveries handed out that the broker still waits for, and no
other. → (values through singles): THROUGH, the highest of them below every
delivery read but not handed out yet (kept for a poll, or fetched by a poll
that has not returned), is settled with one multiple ack; SINGLES, those
above such a delivery, one by one, so it is not covered. Both NIL when there
is nothing to settle. Deliveries still in the socket have higher tags than
any read, so they are never covered either."
  (let* ((unseen (loop for m in (append (%pending-deliveries conn) (%handing-out conn))
                       when (eql channel (%delivery-channel m))
                         collect (mq-protocol:mq-message-delivery-tag m)))
         (lowest-unseen (and unseen (reduce #'min unseen)))
         (through nil)
         (singles '()))
    (maphash (lambda (tag v)
               (declare (ignore v))
               (cond
                 ((member tag unseen))
                 ((or (null lowest-unseen) (< tag lowest-unseen))
                  (when (or (null through) (> tag through))
                    (setf through tag)))
                 (t (push tag singles))))
             (%cs-unsettled (%channel-state conn channel)))
    (values through (sort singles #'<))))

(defun %settle (backend message method &rest fields &key multiple &allow-other-keys)
  "Send METHOD (:BASIC-ACK or :BASIC-NACK) with FIELDS for MESSAGE on
BACKEND's channel (%OUTSTANDING-TAG), and mark what it settles. Tag 0 with
MULTIPLE is every delivery handed out (%EVERY-HANDED-OUT), never one not
handed out yet. When MESSAGE is not outstanding there nothing is sent and
AMQP-STALE-DELIVERY is signalled, outside the connection lock, with restart
CONTINUE (send nothing, → NIL). → MESSAGE."
  (let ((reason nil)
        (channel (amqp-backend-channel backend)))
    (%with-backend-connection (conn backend)
      (let* ((state (%channel-state conn channel))
             (unsettled (%cs-unsettled state))
             (offset (%cs-offset state)))
        (flet ((send (tag &key (multiple multiple))
                 ;; The first :MULTIPLE in the plist is the one encoded.
                 (apply #'write-amqp-method conn method
                        :channel channel :delivery-tag (- tag offset) :multiple multiple
                        fields)
                 (if multiple
                     (dolist (done (loop for k being the hash-keys of unsettled
                                         when (<= k tag) collect k))
                       (remhash done unsettled))
                     (remhash tag unsettled))))
          (if (and multiple (eql 0 (%given-tag message)))
              (multiple-value-bind (through singles) (%every-handed-out conn channel)
                (if (or through singles)
                    (progn
                      (when through (send through))
                      (dolist (tag singles) (send tag :multiple nil)))
                    (setf reason :settled)))
              (multiple-value-bind (wire tag why) (%outstanding-tag conn channel message multiple)
                (declare (ignore wire))
                (cond
                  (why (setf reason why))
                  (t
                   (when multiple
                     (%check-multiple conn channel tag))
                   (send tag)
                   (when (typep message 'amqp-delivery)
                     (setf (%delivery-settled message)
                           (if (eq method :basic-ack) :ack :nack))))))))))
    (if reason
        (restart-case (error 'amqp-stale-delivery :backend backend
                                                  :delivery-tag (%given-tag message)
                                                  :reason reason)
          (continue ()
            :report "Send nothing for this delivery."
            nil))
        message)))

(defun %take-pending (conn queue n &optional (channel (amqp-connection-channel conn)))
  "Remove and return up to N delivered messages of QUEUE on CHANNEL from
CONN's pending list, oldest first. Messages of other queues or channels
stay; messages whose channel closed since are dropped (the broker requeued
them). The caller holds the connection lock."
  (let ((taken '()) (kept '()))
    (dolist (msg (%pending-deliveries conn))
      (cond
       ((%retired-p conn msg))
       ((and (< (length taken) n)
             (eql channel (%delivery-channel msg))
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
NACK). Messages already delivered to this connection for DESTINATION on
the backend's channel (to a consumer on it, see SUBSCRIBE) come first,
then Basic.Get. Messages of other queues stay for polls of those queues.
With TIMEOUT (seconds) an empty queue is retried until a message arrives or
the time is up; without it one attempt is made. Broker notifications read
meanwhile are signalled before the messages are returned (a multiple ack
from a handler does not cover them: they are not handed out yet); if a
handler unwinds, the messages are kept for the next poll of DESTINATION,
not lost."
  (check-type n (integer 1))
  (let* ((queue (%dest-string destination))
         (channel (amqp-backend-channel backend))
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
          (setf out (append out (%take-pending conn queue (- n (length out)) channel)))
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
    (bt2:with-recursive-lock-held ((%connection-lock conn))
      (setf (%handing-out conn) (append out (%handing-out conn))))
    (unwind-protect
        (progn
          (when (zerop *amqp-operation-depth*)
            (%signal-notifications conn))
          (setf handed-out t)
          out)
      (bt2:with-recursive-lock-held ((%connection-lock conn))
        (setf (%handing-out conn)
              (remove-if (lambda (m) (member m out)) (%handing-out conn))))
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
