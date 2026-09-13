(in-package #:mq-backend-amqp/tests)

(defun octets= (a b)
  (and (= (length a) (length b))
       (every #'= a b)))

(deftest basic-publish-roundtrip
  (let* ((encoded (mq-backend-amqp:encode-method-frame
                   :basic-publish
                   :channel 1
                   :exchange "ex"
                   :routing-key "rk"
                   :mandatory t
                   :immediate nil))
         (method (mq-backend-amqp:decode-method-frame encoded)))
    (ok (eq :basic-publish (mq-backend-amqp:amqp-method-name method)))
    (ok (= 60 (mq-backend-amqp:amqp-method-class-id method)))
    (ok (= 40 (mq-backend-amqp:amqp-method-method-id method)))
    (let ((args (mq-backend-amqp:amqp-method-arguments method)))
      (ok (equal "ex" (getf args :exchange)))
      (ok (equal "rk" (getf args :routing-key)))
      (ok (eq t (getf args :mandatory)))
      (ok (null (getf args :immediate))))))

(deftest method-frame-connection-tune
  (let* ((encoded (mq-backend-amqp:encode-method-frame
                   :connection-tune
                   :channel 0
                   :channel-max 2047
                   :frame-max 131072
                   :heartbeat 60))
         (frame (mq-backend-amqp:decode-frame encoded))
         (method (mq-backend-amqp:decode-method
                  (mq-backend-amqp:amqp-frame-payload frame))))
    (ok (= mq-backend-amqp:+amqp-frame-method+
           (mq-backend-amqp:amqp-frame-type frame)))
    (ok (zerop (mq-backend-amqp:amqp-frame-channel frame)))
    (ok (eq :connection-tune (mq-backend-amqp:amqp-method-name method)))
    (let ((args (mq-backend-amqp:amqp-method-arguments method)))
      (ok (= 2047 (getf args :channel-max)))
      (ok (= 131072 (getf args :frame-max)))
      (ok (= 60 (getf args :heartbeat))))))

(deftest heartbeat-roundtrip
  (let* ((encoded (mq-backend-amqp:encode-heartbeat :channel 0))
         (frame (mq-backend-amqp:decode-frame encoded)))
    (ok (mq-backend-amqp:heartbeat-frame-p frame))
    (ok (zerop (length (mq-backend-amqp:amqp-frame-payload frame))))
    (ok (= mq-backend-amqp:+amqp-frame-end+
           (aref encoded (1- (length encoded)))))))

(deftest queue-declare-bits-and-table
  (let* ((encoded (mq-backend-amqp:encode-method
                   :queue-declare
                   :queue "q"
                   :durable t
                   :auto-delete t
                   :arguments '(("x-max-length" . 10))))
         (method (mq-backend-amqp:decode-method encoded))
         (args (mq-backend-amqp:amqp-method-arguments method)))
    (ok (eq :queue-declare (mq-backend-amqp:amqp-method-name method)))
    (ok (equal "q" (getf args :queue)))
    (ok (eq t (getf args :durable)))
    (ok (eq t (getf args :auto-delete)))
    (ok (null (getf args :exclusive)))
    (ok (equal '(("x-max-length" . 10)) (getf args :arguments)))))

(deftest connection-start-ok-roundtrip
  (let* ((encoded (mq-backend-amqp:encode-method
                   :connection-start-ok
                   :client-properties '(("product" . "mq-backend-amqp"))
                   :mechanism "PLAIN"
                   :response #(0)
                   :locale "en_US"))
         (method (mq-backend-amqp:decode-method encoded))
         (args (mq-backend-amqp:amqp-method-arguments method)))
    (ok (eq :connection-start-ok (mq-backend-amqp:amqp-method-name method)))
    (ok (equal "PLAIN" (getf args :mechanism)))
    (ok (equal "en_US" (getf args :locale)))
    (ok (equal "mq-backend-amqp"
               (cdr (assoc "product" (getf args :client-properties)
                           :test #'equal))))))

(deftest basic-deliver-ack-nack
  (let* ((deliver (mq-backend-amqp:decode-method
                   (mq-backend-amqp:encode-method
                    :basic-deliver
                    :consumer-tag "ct"
                    :delivery-tag 42
                    :redelivered t
                    :exchange "ex"
                    :routing-key "rk")))
         (ack (mq-backend-amqp:decode-method
               (mq-backend-amqp:encode-method :basic-ack
                                             :delivery-tag 42
                                             :multiple t)))
         (nack (mq-backend-amqp:decode-method
                (mq-backend-amqp:encode-method :basic-nack
                                              :delivery-tag 42
                                              :multiple nil
                                              :requeue t))))
    (ok (equal "ct" (getf (mq-backend-amqp:amqp-method-arguments deliver)
                          :consumer-tag)))
    (ok (= 42 (getf (mq-backend-amqp:amqp-method-arguments deliver)
                    :delivery-tag)))
    (ok (eq t (getf (mq-backend-amqp:amqp-method-arguments deliver)
                    :redelivered)))
    (ok (eq t (getf (mq-backend-amqp:amqp-method-arguments ack) :multiple)))
    (ok (eq t (getf (mq-backend-amqp:amqp-method-arguments nack) :requeue)))))

(deftest content-header-roundtrip
  (let* ((octets (mq-backend-amqp:encode-content-header
                  :class-id 60
                  :body-size 2
                  :channel 1
                  :properties '(:content-type "application/octet-stream"
                                :delivery-mode 2
                                :headers (("k" . "v")))))
         (decoded (mq-backend-amqp:decode-content-header octets))
         (props (getf decoded :properties)))
    (ok (= 60 (getf decoded :class-id)))
    (ok (= 2 (getf decoded :body-size)))
    (ok (equal "application/octet-stream" (getf props :content-type)))
    (ok (= 2 (getf props :delivery-mode)))
    (ok (equal '(("k" . "v")) (getf props :headers)))))

(deftest bad-frame-end
  (let ((octets (mq-backend-amqp:encode-heartbeat)))
    (setf (aref octets (1- (length octets))) 0)
    (ok (signals (mq-backend-amqp:decode-frame octets)
                 'mq-backend-amqp:amqp-frame-error))))
