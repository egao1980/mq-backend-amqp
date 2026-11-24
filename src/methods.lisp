(in-package #:mq-backend-amqp)

;;; AMQP 0-9-1 class-id / method-id table (RabbitMQ target).

(defparameter *amqp-method-specs*
  '(;; connection (10)
    (:connection-start 10 10
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
    (:connection-secure 10 20
                        ((:challenge :longstr)))
    (:connection-secure-ok 10 21
                           ((:response :longstr)))
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
    (:connection-close 10 50
                       ((:reply-code :short)
                        (:reply-text :shortstr)
                        (:class-id :short)
                        (:method-id :short)))
    (:connection-close-ok 10 51 ())
    (:connection-blocked 10 60
                         ((:reason :shortstr)))
    (:connection-unblocked 10 61 ())
    ;; channel (20)
    (:channel-open 20 10
                   ((:reserved-1 :shortstr)))
    (:channel-open-ok 20 11
                      ((:reserved-1 :longstr)))
    (:channel-flow 20 20
                   ((:active :bit)))
    (:channel-flow-ok 20 21
                      ((:active :bit)))
    (:channel-close 20 40
                    ((:reply-code :short)
                     (:reply-text :shortstr)
                     (:class-id :short)
                     (:method-id :short)))
    (:channel-close-ok 20 41 ())
    ;; exchange (40)
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
    (:exchange-declare-ok 40 11 ())
    (:exchange-delete 40 20
                      ((:reserved-1 :short)
                       (:exchange :shortstr)
                       (:if-unused :bit)
                       (:no-wait :bit)))
    (:exchange-delete-ok 40 21 ())
    (:exchange-bind 40 30
                    ((:reserved-1 :short)
                     (:destination :shortstr)
                     (:source :shortstr)
                     (:routing-key :shortstr)
                     (:no-wait :bit)
                     (:arguments :table)))
    (:exchange-bind-ok 40 31 ())
    (:exchange-unbind 40 40
                      ((:reserved-1 :short)
                       (:destination :shortstr)
                       (:source :shortstr)
                       (:routing-key :shortstr)
                       (:no-wait :bit)
                       (:arguments :table)))
    (:exchange-unbind-ok 40 51 ())
    ;; queue (50)
    (:queue-declare 50 10
                    ((:reserved-1 :short)
                     (:queue :shortstr)
                     (:passive :bit)
                     (:durable :bit)
                     (:exclusive :bit)
                     (:auto-delete :bit)
                     (:no-wait :bit)
                     (:arguments :table)))
    (:queue-declare-ok 50 11
                       ((:queue :shortstr)
                        (:message-count :long)
                        (:consumer-count :long)))
    (:queue-bind 50 20
                 ((:reserved-1 :short)
                  (:queue :shortstr)
                  (:exchange :shortstr)
                  (:routing-key :shortstr)
                  (:no-wait :bit)
                  (:arguments :table)))
    (:queue-bind-ok 50 21 ())
    (:queue-purge 50 30
                  ((:reserved-1 :short)
                   (:queue :shortstr)
                   (:no-wait :bit)))
    (:queue-purge-ok 50 31
                     ((:message-count :long)))
    (:queue-delete 50 40
                   ((:reserved-1 :short)
                    (:queue :shortstr)
                    (:if-unused :bit)
                    (:if-empty :bit)
                    (:no-wait :bit)))
    (:queue-delete-ok 50 41
                      ((:message-count :long)))
    (:queue-unbind 50 50
                   ((:reserved-1 :short)
                    (:queue :shortstr)
                    (:exchange :shortstr)
                    (:routing-key :shortstr)
                    (:arguments :table)))
    (:queue-unbind-ok 50 51 ())
    ;; basic (60)
    (:basic-qos 60 10
                ((:prefetch-size :long)
                 (:prefetch-count :short)
                 (:global :bit)))
    (:basic-qos-ok 60 11 ())
    (:basic-consume 60 20
                    ((:reserved-1 :short)
                     (:queue :shortstr)
                     (:consumer-tag :shortstr)
                     (:no-local :bit)
                     (:no-ack :bit)
                     (:exclusive :bit)
                     (:no-wait :bit)
                     (:arguments :table)))
    (:basic-consume-ok 60 21
                       ((:consumer-tag :shortstr)))
    (:basic-cancel 60 30
                   ((:consumer-tag :shortstr)
                    (:no-wait :bit)))
    (:basic-cancel-ok 60 31
                      ((:consumer-tag :shortstr)))
    (:basic-publish 60 40
                    ((:reserved-1 :short)
                     (:exchange :shortstr)
                     (:routing-key :shortstr)
                     (:mandatory :bit)
                     (:immediate :bit)))
    (:basic-return 60 50
                   ((:reply-code :short)
                    (:reply-text :shortstr)
                    (:exchange :shortstr)
                    (:routing-key :shortstr)))
    (:basic-deliver 60 60
                    ((:consumer-tag :shortstr)
                     (:delivery-tag :longlong)
                     (:redelivered :bit)
                     (:exchange :shortstr)
                     (:routing-key :shortstr)))
    (:basic-get 60 70
                ((:reserved-1 :short)
                 (:queue :shortstr)
                 (:no-ack :bit)))
    (:basic-get-ok 60 71
                   ((:delivery-tag :longlong)
                    (:redelivered :bit)
                    (:exchange :shortstr)
                    (:routing-key :shortstr)
                    (:message-count :long)))
    (:basic-get-empty 60 72
                      ((:reserved-1 :shortstr)))
    (:basic-ack 60 80
                ((:delivery-tag :longlong)
                 (:multiple :bit)))
    (:basic-reject 60 90
                   ((:delivery-tag :longlong)
                    (:requeue :bit)))
    (:basic-recover-async 60 100
                          ((:requeue :bit)))
    (:basic-recover 60 110
                    ((:requeue :bit)))
    (:basic-recover-ok 60 111 ())
    (:basic-nack 60 120
                 ((:delivery-tag :longlong)
                  (:multiple :bit)
                  (:requeue :bit)))
    ;; confirm (85, RabbitMQ)
    (:confirm-select 85 10
                     ((:no-wait :bit)))
    (:confirm-select-ok 85 11 ())
    ;; tx (90)
    (:tx-select 90 10 ())
    (:tx-select-ok 90 11 ())
    (:tx-commit 90 20 ())
    (:tx-commit-ok 90 21 ())
    (:tx-rollback 90 30 ())
    (:tx-rollback-ok 90 31 ())
    (:heartbeat 0 0 ()))
  "AMQP 0-9-1 methods (name class-id method-id fields) for the classes the
backend speaks, with RabbitMQ's extensions (exchange.bind, connection.blocked,
confirm). Bits pack LSB first.")

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
