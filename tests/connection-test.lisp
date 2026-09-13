(in-package #:mq-backend-amqp/tests)

(deftest mock-stream-method-roundtrip
  (let* ((pipe (mq-backend-amqp:make-octet-pipe))
         (conn (mq-backend-amqp:make-amqp-connection :stream pipe :channel 1)))
    (mq-backend-amqp:write-amqp-method conn :channel-open :reserved-1 "")
    (multiple-value-bind (method frame)
        (mq-backend-amqp:read-amqp-method conn)
      (ok (eq :channel-open (mq-backend-amqp:amqp-method-name method)))
      (ok (= 1 (mq-backend-amqp:amqp-frame-channel frame))))))

(deftest publish-writes-three-frames
  (let* ((pipe (mq-backend-amqp:make-octet-pipe))
         (conn (mq-backend-amqp:make-amqp-connection :stream pipe :channel 1))
         (backend (mq-backend-amqp:make-amqp-backend :connection conn))
         (msg (mq-protocol:make-mq-message :key "k" :payload #(72 105)
                                           :headers '(("h" . "v"))
                                           :topic "rk" :exchange "ex")))
    (mq-protocol:publish backend "rk" msg)
    (let ((mframe (mq-backend-amqp:read-amqp-frame conn))
          (hframe (mq-backend-amqp:read-amqp-frame conn))
          (bframe (mq-backend-amqp:read-amqp-frame conn)))
      (ok (= mq-backend-amqp:+amqp-frame-method+
             (mq-backend-amqp:amqp-frame-type mframe)))
      (ok (eq :basic-publish
              (mq-backend-amqp:amqp-method-name
               (mq-backend-amqp:decode-method
                (mq-backend-amqp:amqp-frame-payload mframe)))))
      (ok (= mq-backend-amqp:+amqp-frame-header+
             (mq-backend-amqp:amqp-frame-type hframe)))
      (ok (= mq-backend-amqp:+amqp-frame-body+
             (mq-backend-amqp:amqp-frame-type bframe)))
      (ok (equalp #(72 105) (mq-backend-amqp:amqp-frame-payload bframe)))
      (let ((header (mq-backend-amqp:decode-content-header hframe)))
        (ok (= 2 (getf header :body-size)))
        (ok (equal "k" (getf (getf header :properties) :correlation-id)))))))

(deftest ack-nack-on-pipe
  (let* ((pipe (mq-backend-amqp:make-octet-pipe))
         (backend (mq-backend-amqp:make-amqp-backend
                   :stream pipe :channel 1)))
    (mq-protocol:ack backend 7)
    (mq-protocol:nack backend 8 :requeue t)
    (let ((conn (mq-backend-amqp:amqp-backend-connection backend)))
      (ok (eq :basic-ack
              (mq-backend-amqp:amqp-method-name
               (mq-backend-amqp:read-amqp-method conn))))
      (ok (eq :basic-nack
              (mq-backend-amqp:amqp-method-name
               (mq-backend-amqp:read-amqp-method conn)))))))

(deftest missing-connection
  (ok (signals (mq-protocol:publish (mq-backend-amqp:make-amqp-backend)
                                    "t"
                                    (mq-protocol:make-mq-message :payload #(1)))
               'mq-protocol:mq-connection-error)))
