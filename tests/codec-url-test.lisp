(in-package #:mq-backend-amqp/tests)

;;; #6: field tables carry f (float), d (double) and D (decimal), which
;;; RabbitMQ and Python clients send; an unknown type octet is an
;;; AMQP-FRAME-ERROR, not a CASE-FAILURE. #9: IPv6 hosts in amqp:// URLs,
;;; cross-checked against pika 1.3 URLParameters.

(defun %table-octets (alist)
  (let ((buf (mq-backend-amqp::make-buffer)))
    (mq-backend-amqp::encode-table buf alist)
    (mq-backend-amqp::buffer-octets buf)))

(defun %read-table (octets)
  (mq-backend-amqp::decode-table (mq-backend-amqp::make-cursor :octets (coerce octets '(vector (unsigned-byte 8))))))

(deftest-parametrize pika-field-tables-decode
    ((what octets expected)
     ;; pika data.encode_table({"ratio": 0.5})
     ("a Python float is d" #(0 0 0 15 5 114 97 116 105 111 100 63 224 0 0 0 0 0 0) '(("ratio" . 0.5d0)))
     ;; {"price": Decimal("1.23")}
     ("a Decimal is D" #(0 0 0 12 5 112 114 105 99 101 68 2 0 0 0 123) '(("price" . 123/100)))
     ;; {"neg": Decimal("-2.5")}
     ("a negative Decimal" #(0 0 0 10 3 110 101 103 68 1 255 255 255 231) '(("neg" . -5/2))))
  (let ((got (handler-case (%read-table octets)
               (error (e) (list :error (type-of e) (princ-to-string e))))))
    (ok (equal expected got) (format nil "~A: ~S (got ~S)" what expected got))))

(deftest float-and-decimal-headers-round-trip
  (let* ((table '(("f" . 0.25f0) ("d" . -1.5d0) ("D" . 314/100) ("i" . 7)))
         (octets (%table-octets table)))
    (ok (equal table (%read-table octets)) "f, d and D come back as they went")
    ;; length (4 octets), then "f" as shortstr (2), then its type octet.
    (ok (= (char-code #\f) (aref octets 6)) "a single-float is written as f")
    (ok (equalp octets (%table-octets (%read-table octets))) "and re-encode to the same octets")))

(deftest pika-reads-our-decimal-encoding
  ;; The same octets pika writes for Decimal("1.23").
  (ok (equalp #(0 0 0 12 5 112 114 105 99 101 68 2 0 0 0 123)
              (%table-octets '(("price" . 123/100))))))

(deftest a-ratio-that-is-no-decimal-is-refused
  (ok (signals (%table-octets '(("third" . 1/3))) 'mq-backend-amqp:amqp-frame-error))
  (ok (signals (%table-octets (list (cons "big" (ash 1 70)))) 'mq-backend-amqp:amqp-frame-error)
      "nor is an integer over 64 bits silently truncated"))

(deftest an-unknown-field-type-is-a-frame-error
  ;; {"x": <type ?>} with a made-up type octet 63 (?).
  (let ((e (handler-case (progn (%read-table #(0 0 0 4 1 120 63 0)) nil)
             (error (e) e))))
    (ok (typep e 'mq-backend-amqp:amqp-frame-error) (format nil "amqp-frame-error (got ~S)" (type-of e)))
    (ok (and (typep e 'mq-backend-amqp:amqp-frame-error)
             (eql 6 (mq-backend-amqp:amqp-frame-error-offset e)))
        "at the offset of the type octet")))

(deftest a-float-header-survives-the-content-header
  (let* ((frame (mq-backend-amqp:encode-content-header
                 :class-id 60 :body-size 0 :channel 1
                 :properties '(:headers (("ratio" . 0.5d0) ("price" . 123/100)))))
         (decoded (mq-backend-amqp:decode-content-header
                   (mq-backend-amqp:decode-frame frame))))
    (ok (equal '(("ratio" . 0.5d0) ("price" . 123/100))
               (getf (getf decoded :properties) :headers)))))

;; pika 1.3: URLParameters(url).host / .port / .virtual_host
(deftest-parametrize ipv6-urls-parse-as-in-pika
    ((url host port vhost)
     ("amqp://[::1]:5672/%2F" "::1" 5672 "/")
     ("amqps://[2001:db8::1]/%2F" "2001:db8::1" 5671 "/")
     ("amqp://[::1]:15683/%2F" "::1" 15683 "/")
     ("amqp://host:5672/v" "host" 5672 "v"))
  (let ((got (mq-backend-amqp:parse-amqp-url url)))
    (ok (and (equal host (getf got :host)) (eql port (getf got :port))
             (equal vhost (getf got :vhost)))
        (format nil "~A → ~S ~D ~S (got ~S)" url host port vhost got))))

(deftest ipv6-url-refusals-blame-no-escaping
  ;; With a quri that refuses userinfo before an IPv6 literal
  ;; (cl-stack-systems#85) the URL is refused, but not for its escaping,
  ;; and the password is masked; with a fixed quri it parses.
  (let ((got (handler-case (mq-backend-amqp:parse-amqp-url "amqp://app:s3cret@[::1]:5672/%2F")
               (mq-protocol:mq-error (e) (princ-to-string e)))))
    (if (stringp got)
        (progn
          (ok (not (search "s3cret" got)) (format nil "the password is masked (~A)" got))
          (ok (not (search "escape" got)) (format nil "no escaping hint (~A)" got)))
        (ok (and (equal "::1" (getf got :host)) (equal "s3cret" (getf got :password)))
            (format nil "parses (~S)" got)))))

(deftest-parametrize bracketed-hosts-are-unbracketed
    ((given expected)
     ("[::1]" "::1") ("::1" "::1") ("127.0.0.1" "127.0.0.1") ("[]" "") ("[" "["))
  (ok (equal expected (mq-backend-amqp::%unbracket-host given))))

(deftest ipv6-endpoints-are-bracketed-in-reports
  (ok (equal "[::1]:5672" (mq-backend-amqp::%host-port "::1" 5672)))
  (ok (equal "rabbit:5672" (mq-backend-amqp::%host-port "rabbit" 5672))))

(deftest live-connect-by-bracketed-ipv6-host
  (with-broker ()
    (let ((control (handler-case
                       (mq-backend-amqp:with-amqp-backend (b :host "::1" :port (%port)
                                                             :user (%user) :password (%password))
                         (mq-backend-amqp:amqp-declare-queue b "")
                         :ok)
                     (error () nil))))
      (if (null control)
          (skip "the broker does not answer on ::1")
          (ok (eq :ok (handler-case
                          (mq-backend-amqp:with-amqp-backend (b :host "[::1]" :port (%port)
                                                                :user (%user) :password (%password))
                            (mq-backend-amqp:amqp-declare-queue b "")
                            :ok)
                        (error (e) (princ-to-string e))))
              ":host \"[::1]\" connects like \"::1\"")))))
