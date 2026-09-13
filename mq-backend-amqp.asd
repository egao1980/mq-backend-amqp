(defsystem "mq-backend-amqp"
  :version "0.1.0"
  :description "Pure CL AMQP 0-9-1 framing backend for mq-protocol (RabbitMQ target)"
  :author "egao1980"
  :license "MIT"
  :depends-on ("mq-protocol" "binary-protocol")
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "codec")
               (:file "methods")
               (:file "frames")
               (:file "connection")
               (:file "backend"))
  :in-order-to ((test-op (test-op "mq-backend-amqp/tests"))))

(defsystem "mq-backend-amqp/tests"
  :depends-on ("mq-backend-amqp" "rove")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "frame-test")
               (:file "connection-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
