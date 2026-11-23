(defsystem "mq-backend-amqp"
  :version "0.2.0"
  :description "Pure CL AMQP 0-9-1 backend for mq-protocol: framing, handshake (SASL PLAIN), heartbeats, Basic.Get, TLS through cl-stack-ssl (RabbitMQ target)"
  :author "egao1980"
  :license "MIT"
  :depends-on ("mq-protocol" "binary-protocol" "encoding-protocol" "usocket" "quri"
               "cl-stack-ssl" "bordeaux-threads")
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "codec")
               (:file "methods")
               (:file "frames")
               (:file "connection")
               (:file "session")
               (:file "backend"))
  :in-order-to ((test-op (test-op "mq-backend-amqp/tests"))))

(defsystem "mq-backend-amqp/tests"
  :depends-on ("mq-backend-amqp" "rove" "usocket" "flexi-streams")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "frame-test")
               (:file "connection-test")
               (:file "connect-test")
               (:file "session-test")
               (:file "cancel-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
