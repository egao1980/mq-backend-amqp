(in-package #:mq-backend-amqp)

;;; AMQP 0-9-1 class-id / method-id table (RabbitMQ target).

(defparameter *amqp-method-specs*
  '((:connection-start 10 10
     ((:version-major :octet)
      (:version-minor :octet)
      (:server-properties :table)
      (:mechanisms :longstr)
      (:locales :longstr)))
    (:connection-start-ok 10 11
     ((:client-properties :table)
      (:mechanism :shortstr)
      (:response :longstr)
      (:locale :shortstr)))
    (:connection-tune 10 30
     ((:channel-max :short)
      (:frame-max :long)
      (:heartbeat :short)))
    (:connection-tune-ok 10 31
     ((:channel-max :short)
      (:frame-max :long)
      (:heartbeat :short)))
    (:connection-open 10 40
     ((:virtual-host :shortstr)
      (:reserved-1 :shortstr)
      (:reserved-2 :bit)))
    (:connection-open-ok 10 41
     ((:reserved-1 :shortstr)))
    (:channel-open 20 10
     ((:reserved-1 :shortstr)))
    (:channel-open-ok 20 11
     ((:reserved-1 :longstr)))
    (:exchange-declare 40 10
     ((:reserved-1 :short)
      (:exchange :shortstr)
      (:type :shortstr)
      (:passive :bit)
      (:durable :bit)
      (:auto-delete :bit)
      (:internal :bit)
      (:no-wait :bit)
      (:arguments :table)))
    (:queue-declare 50 10
     ((:reserved-1 :short)
      (:queue :shortstr)
      (:passive :bit)
      (:durable :bit)
      (:exclusive :bit)
      (:auto-delete :bit)
      (:no-wait :bit)
      (:arguments :table)))
    (:queue-bind 50 20
     ((:reserved-1 :short)
      (:queue :shortstr)
      (:exchange :shortstr)
      (:routing-key :shortstr)
      (:no-wait :bit)
      (:arguments :table)))
    (:basic-qos 60 10
     ((:prefetch-size :long)
      (:prefetch-count :short)
      (:global :bit)))
    (:basic-consume 60 20
     ((:reserved-1 :short)
      (:queue :shortstr)
      (:consumer-tag :shortstr)
      (:no-local :bit)
      (:no-ack :bit)
      (:exclusive :bit)
      (:no-wait :bit)
      (:arguments :table)))
    (:basic-cancel 60 30
     ((:consumer-tag :shortstr)
      (:no-wait :bit)))
    (:basic-publish 60 40
     ((:reserved-1 :short)
      (:exchange :shortstr)
      (:routing-key :shortstr)
      (:mandatory :bit)
      (:immediate :bit)))
    (:basic-deliver 60 60
     ((:consumer-tag :shortstr)
      (:delivery-tag :longlong)
      (:redelivered :bit)
      (:exchange :shortstr)
      (:routing-key :shortstr)))
    (:basic-ack 60 80
     ((:delivery-tag :longlong)
      (:multiple :bit)))
    (:basic-nack 60 120
     ((:delivery-tag :longlong)
      (:multiple :bit)
      (:requeue :bit)))
    (:heartbeat 0 0 ())))

(defun %method-index ()
  (let ((by-name (make-hash-table :test 'eq))
        (by-id (make-hash-table :test 'equal)))
    (dolist (spec *amqp-method-specs*)
      (destructuring-bind (name class-id method-id fields) spec
        (let ((row (list :name name :class-id class-id :method-id method-id
                         :fields fields)))
          (setf (gethash name by-name) row)
          (setf (gethash (cons class-id method-id) by-id) row))))
    (values by-name by-id)))

(defvar *amqp-methods-by-name*)
(defvar *amqp-methods-by-id*)

(eval-when (:load-toplevel :execute)
  (multiple-value-setq (*amqp-methods-by-name* *amqp-methods-by-id*)
    (%method-index)))

(defstruct amqp-method
  name
  class-id
  method-id
  arguments)

(defun amqp-method-spec (name-or-ids)
  (or (if (consp name-or-ids)
          (gethash name-or-ids *amqp-methods-by-id*)
          (gethash name-or-ids *amqp-methods-by-name*))
      (error 'amqp-frame-error
             :message (format nil "unknown AMQP method ~s" name-or-ids))))

(defun encode-method (name &rest arguments)
  "Encode method NAME + ARGUMENTS plist to a method-frame payload."
  (let* ((spec (amqp-method-spec name))
         (class-id (getf spec :class-id))
         (method-id (getf spec :method-id))
         (fields (getf spec :fields))
         (head (binary-protocol:pack "!HH" class-id method-id))
         (body (encode-fields fields arguments)))
    (concatenate '(simple-array (unsigned-byte 8) (*)) head body)))

(defun decode-method (payload)
  "Decode a method-frame payload into an AMQP-METHOD."
  (unless (>= (length payload) 4)
    (error 'amqp-frame-error :message "method payload too short"))
  (destructuring-bind (class-id method-id)
      (binary-protocol:unpack "!HH" payload)
    (let* ((spec (gethash (cons class-id method-id) *amqp-methods-by-id*))
           (cur (make-cursor :octets payload :pos 4)))
      (unless spec
        (error 'amqp-frame-error
               :message (format nil "unknown class/method ~d/~d"
                                class-id method-id)))
      (make-amqp-method
       :name (getf spec :name)
       :class-id class-id
       :method-id method-id
       :arguments (decode-fields (getf spec :fields) cur)))))
