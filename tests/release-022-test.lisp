(in-package #:mq-backend-amqp/tests)

;;; 0.2.2 adversarial check of the bounded writes (#12, break 3): a broker
;;; that stops reading but keeps sending is not gone. RabbitMQ under a
;;; memory or disk alarm sends Connection.Blocked, stops reading
;;; publishing connections and keeps sending heartbeats; pika's heartbeat
;;; checker counts octets received and keeps the connection, and 0.2.1
;;; waited too. The publish must wait for the broker to read again.

(deftest-parametrize a-publish-to-a-broker-that-stops-reading-but-keeps-sending-waits
    ((transport) (:plain) (:tls))
  ;; The fake broker serves the handshake (heartbeat 1 s), then for 5 s
  ;; sends a heartbeat every 0.3 s and reads nothing, then reads all.
  (with-transport (transport)
    (multiple-value-bind (port thread)
        (%serve transport
                (lambda (srv socket)
                  (declare (ignore socket))
                  (%fake-handshake srv :heartbeat 1)
                  (loop repeat 17
                        do (mq-backend-amqp:write-amqp-frame srv (mq-backend-amqp::encode-heartbeat))
                           (sleep 0.3))
                  (loop (mq-backend-amqp:read-amqp-frame srv))))
      (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 1
                                                      :tls (eq transport :tls) :tls-verify nil))
             (payload (make-array (* 32 1024 1024) :element-type '(unsigned-byte 8)
                                                    :initial-element 120))
             (outcome nil)
             (start (get-internal-real-time))
             (publisher
               (bt2:make-thread
                (lambda ()
                  (setf outcome
                        (handler-case
                            (progn (mq-protocol:publish b "q" (mq-protocol:make-mq-message
                                                               :payload payload))
                                   :returned)
                          (error (e) e))))
                :name "publisher")))
        (loop until (or outcome (> (%seconds-since start) 20)) do (sleep 0.1))
        (let ((took (%seconds-since start)))
          (close b :abort t)
          (bt2:join-thread publisher)
          (bt2:join-thread thread)
          (format t "~&    ~S: ~A after ~,1F s~%" transport outcome took)
          (ok (eq :returned outcome)
              (format nil "~S: the publish returns once the broker reads again (got ~A after ~,1F s)"
                      transport outcome took)))))))
