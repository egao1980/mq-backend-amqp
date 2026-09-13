(in-package #:mq-backend-amqp)

(defclass amqp-backend (mq-protocol:mq-backend)
  ((connection :initarg :connection :accessor amqp-backend-connection
               :initform nil)
   (channel :initarg :channel :accessor amqp-backend-channel :initform 1))
  (:documentation
   "mq-protocol backend that writes AMQP 0-9-1 frames onto a connection."))

(defun make-amqp-backend (&key connection stream (channel 1))
  (make-instance 'amqp-backend
                 :connection (or connection
                                 (and stream
                                      (make-amqp-connection :stream stream
                                                            :channel channel)))
                 :channel channel))

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

(defmethod mq-protocol:publish ((backend amqp-backend) destination message &key)
  (let* ((conn (%conn backend))
         (msg (if (mq-protocol:mq-message-p message)
                  message
                  (mq-protocol:make-mq-message :payload message)))
         (payload (mq-protocol:mq-message-payload msg))
         (channel (amqp-backend-channel backend)))
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
      (write-amqp-frame conn (encode-body-frame payload :channel channel))
      msg)))

(defmethod mq-protocol:subscribe ((backend amqp-backend) destination handler &key
                                  (consumer-tag "") no-ack)
  (write-amqp-method (%conn backend) :basic-consume
                     :channel (amqp-backend-channel backend)
                     :queue (%dest-string destination)
                     :consumer-tag (or consumer-tag "")
                     :no-local nil
                     :no-ack no-ack
                     :exclusive nil
                     :no-wait nil)
  (make-instance 'mq-protocol:mq-subscription
                 :destination destination
                 :handler handler
                 :tag consumer-tag))

(defmethod mq-protocol:poll-messages ((backend amqp-backend) destination
                                      &key (n 1) timeout)
  (declare (ignore destination n timeout))
  (error 'mq-protocol:mq-error
         :backend backend
         :message "poll-messages needs a live broker — use read-amqp-frame"))

(defmethod mq-protocol:ack ((backend amqp-backend) message &key multiple)
  (write-amqp-method (%conn backend) :basic-ack
                     :channel (amqp-backend-channel backend)
                     :delivery-tag (if (integerp message)
                                       message
                                       (or (mq-protocol:mq-message-delivery-tag message)
                                           0))
                     :multiple multiple)
  message)

(defmethod mq-protocol:nack ((backend amqp-backend) message &key (requeue t) multiple)
  (write-amqp-method (%conn backend) :basic-nack
                     :channel (amqp-backend-channel backend)
                     :delivery-tag (if (integerp message)
                                       message
                                       (or (mq-protocol:mq-message-delivery-tag message)
                                           0))
                     :multiple multiple
                     :requeue requeue)
  message)
