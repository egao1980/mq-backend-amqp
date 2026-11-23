(in-package #:mq-backend-amqp/tests)

;;; Evidence for egao1980/mq-backend-amqp#2: connect + handshake.
;;;
;;; Spec: the issue's target code works against RabbitMQ:
;;;
;;;   (mq-backend-amqp:with-amqp-backend (b :host "127.0.0.1" :port 5672
;;;                                         :user "guest" :password "guest")
;;;     (stack-mq:publish b "orders" (stack-mq:make-mq-message :payload "order-1"))
;;;     (stack-mq:poll-messages b "orders"))
;;;
;;; i.e. protocol header, Start-Ok (SASL PLAIN), Tune-Ok, Open, Channel.Open,
;;; Basic.Get for poll-messages, heartbeat frames tolerated, and the socket
;;; released on exit. Live tests need RabbitMQ and skip without it:
;;;   docker run -d --name rabbitmq-test -p 127.0.0.1:15673:5672 rabbitmq:3.13.7-alpine
;;; Override with MQ_AMQP_TEST_HOST / _PORT / _USER / _PASSWORD.
;;;
;;; WITH-AMQP-BACKEND is written mq-backend-amqp:: so this file compiles on
;;; a tree without it; with-amqp-backend-is-exported checks the export.

(defun %env (name default)
  (let ((v (uiop:getenv name)))
    (if (and v (plusp (length v))) v default)))

(defun %host () (%env "MQ_AMQP_TEST_HOST" "127.0.0.1"))
(defun %port () (parse-integer (%env "MQ_AMQP_TEST_PORT" "15673")))
(defun %user () (%env "MQ_AMQP_TEST_USER" "guest"))
(defun %password () (%env "MQ_AMQP_TEST_PASSWORD" "guest"))

(defun %broker-up-p ()
  (let ((s (handler-case
               (usocket:socket-connect (%host) (%port) :timeout 2)
             (error () nil))))
    (when s (usocket:socket-close s) t)))

(defmacro with-broker (() &body body)
  `(if (%broker-up-p)
       (progn ,@body)
       (skip (format nil "no RabbitMQ at ~A:~D (see tests/connect-test.lisp)"
                     (%host) (%port)))))

(defun %queue-name (tag)
  (format nil "evidence.~A.~D.~D" tag (get-universal-time) (random 1000000)))

(defun %declare-queue (backend queue)
  "Queue.Declare (no-wait) through the exported framing API."
  (mq-backend-amqp:write-amqp-method
   (mq-backend-amqp:amqp-backend-connection backend) :queue-declare
   :reserved-1 0 :queue queue :passive nil :durable nil
   :exclusive t :auto-delete t :no-wait t :arguments nil))

(defun %octets (string) (map '(vector (unsigned-byte 8)) #'char-code string))

(deftest with-amqp-backend-is-exported
  (ok (eq :external
          (nth-value 1 (find-symbol "WITH-AMQP-BACKEND" "MQ-BACKEND-AMQP")))
      "mq-backend-amqp exports WITH-AMQP-BACKEND"))

(deftest connect-refused-signals-mq-connection-error
  ;; No broker needed: nothing listens on 127.0.0.1:1.
  (ok (signals (mq-backend-amqp::with-amqp-backend (b :host "127.0.0.1" :port 1
                                                      :user "guest" :password "guest")
                 b)
               'mq-protocol:mq-connection-error)))

(deftest handshake-publish-poll
  (with-broker ()
    (let ((q (%queue-name "orders")))
      (mq-backend-amqp::with-amqp-backend (b :host (%host) :port (%port)
                                             :user (%user) :password (%password))
        (%declare-queue b q)
        (stack-mq:publish b q (stack-mq:make-mq-message :payload "order-1"))
        (let ((msgs (stack-mq:poll-messages b q :timeout 2)))
          (ok (= 1 (length msgs)) "poll-messages returns the published message")
          (ok (and msgs (equalp (%octets "order-1")
                                (stack-mq:mq-message-payload (first msgs))))
              "payload round-trips")
          (ok (and msgs (stack-mq:mq-message-delivery-tag (first msgs)))
              "the message carries a delivery tag")
          (when msgs (stack-mq:ack b (first msgs))))
        (ok (null (stack-mq:poll-messages b q :timeout 1))
            "the acked message is gone")))))

(deftest bad-password-signals-mq-connection-error
  (with-broker ()
    (ok (signals (mq-backend-amqp::with-amqp-backend
                     (b :host (%host) :port (%port)
                        :user (%user) :password "definitely-not-the-password")
                   b)
                 'mq-protocol:mq-connection-error)
        "SASL PLAIN rejection surfaces as mq-connection-error")))

(deftest heartbeat-frames-are-tolerated
  ;; Negotiate a 2 s heartbeat, stay idle 2.5 s: the broker sends heartbeat
  ;; frames meanwhile, and the next poll must read past them.
  (with-broker ()
    (let ((q (%queue-name "hb")))
      (mq-backend-amqp::with-amqp-backend (b :host (%host) :port (%port)
                                             :user (%user) :password (%password)
                                             :heartbeat 2)
        (%declare-queue b q)
        (sleep 2.5)
        (stack-mq:publish b q (stack-mq:make-mq-message :payload "after-idle"))
        (let ((msgs (stack-mq:poll-messages b q :timeout 2)))
          (ok (and msgs (equalp (%octets "after-idle")
                                (stack-mq:mq-message-payload (first msgs))))
              "poll after an idle heartbeat interval still works"))))))

(deftest exit-releases-the-socket
  (with-broker ()
    (let ((conn nil))
      (mq-backend-amqp::with-amqp-backend (b :host (%host) :port (%port)
                                             :user (%user) :password (%password))
        (setf conn (mq-backend-amqp:amqp-backend-connection b))
        (ok (open-stream-p (mq-backend-amqp:amqp-connection-stream conn))
            "connected inside the body"))
      (ok (and conn
               (not (open-stream-p (mq-backend-amqp:amqp-connection-stream conn))))
          "the stream is closed after WITH-AMQP-BACKEND returns"))))
