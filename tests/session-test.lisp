(in-package #:mq-backend-amqp/tests)

;;; The session layer without a broker: a scripted broker is a two-way
;;; stream whose input is the frames a broker would send. Then live checks
;;; against RabbitMQ (skip without it): broker Close → conditions, channel
;;; recovery, frame splitting, URL, synchronous declare.

(defun %frames (&rest specs)
  "Octets of SPECS: (method-name channel . fields), :heartbeat,
(:header channel body-size) or (:body channel octets)."
  (apply #'concatenate '(vector (unsigned-byte 8))
         (mapcar (lambda (spec)
                   (cond
                     ((eq spec :heartbeat) (mq-backend-amqp:encode-heartbeat))
                     ((eq (first spec) :header)
                      (mq-backend-amqp:encode-content-header
                       :class-id 60 :channel (second spec) :body-size (third spec)))
                     ((eq (first spec) :body)
                      (mq-backend-amqp:encode-body-frame (third spec) :channel (second spec)))
                     (t (apply #'mq-backend-amqp:encode-method-frame (first spec)
                               :channel (second spec) (cddr spec)))))
                 specs)))

(defun %scripted (input &key (frame-max 131072))
  "A connection reading INPUT octets. → (values connection output-stream)."
  (let* ((in (flexi-streams:make-in-memory-input-stream input))
         (out (flexi-streams:make-in-memory-output-stream))
         (conn (mq-backend-amqp:make-amqp-connection :stream (make-two-way-stream in out)
                                                     :channel 1 :frame-max frame-max)))
    (values conn out)))

(defun %written (out)
  "Frames the client wrote to OUT (an in-memory output stream, which this
empties, or its octets), as (type channel method-or-nil), after an optional
protocol header."
  (let* ((octets (if (streamp out) (flexi-streams:get-output-stream-sequence out) out))
         (start (if (and (>= (length octets) 8)
                         (equalp (subseq octets 0 8) mq-backend-amqp:+amqp-protocol-header+))
                    8 0)))
    (loop while (< start (length octets))
          collect (let* ((size (mq-backend-amqp:frame-size octets :start start))
                         (frame (mq-backend-amqp:decode-frame octets :start start)))
                    (incf start size)
                    (list (mq-backend-amqp:amqp-frame-type frame)
                          (mq-backend-amqp:amqp-frame-channel frame)
                          (and (= 1 (mq-backend-amqp:amqp-frame-type frame))
                               (mq-backend-amqp:decode-method
                                (mq-backend-amqp:amqp-frame-payload frame))))))))

(defun %names (written)
  (loop for (nil nil m) in written when m collect (mq-backend-amqp:amqp-method-name m)))

(defun %args (written name)
  (mq-backend-amqp:amqp-method-arguments
   (third (find name written :key (lambda (w) (and (third w)
                                                   (mq-backend-amqp:amqp-method-name (third w))))))))

(defun %broker-hello (&key (mechanisms "AMQPLAIN PLAIN") (heartbeat 60))
  (list (list :connection-start 0 :version-major 0 :version-minor 9
              :server-properties '(("product" . "RabbitMQ"))
              :mechanisms mechanisms :locales "en_US")
        (list :connection-tune 0 :channel-max 2047 :frame-max 131072 :heartbeat heartbeat)))

(defun %handshake (conn &key (heartbeat 2) (vhost "/"))
  (mq-backend-amqp::%handshake conn :user "guest" :password "pw" :vhost vhost
                                    :heartbeat heartbeat :frame-max 131072 :channel-max 0
                                    :locale "en_US" :client-properties nil))

(deftest scripted-handshake
  (multiple-value-bind (conn out)
      (%scripted (apply #'%frames (append (%broker-hello)
                                          '((:connection-open-ok 0 :reserved-1 "")
                                            (:channel-open-ok 1 :reserved-1 "")))))
    (%handshake conn :vhost "/v")
    (let* ((octets (flexi-streams:get-output-stream-sequence out))
           (w (%written octets)))
      (ok (equalp mq-backend-amqp:+amqp-protocol-header+ (subseq octets 0 8))
          "the protocol header goes first")
      (ok (equal '(:connection-start-ok :connection-tune-ok :connection-open :channel-open)
                 (%names w)))
      (let ((start-ok (%args w :connection-start-ok)))
        (ok (equal "PLAIN" (getf start-ok :mechanism)))
        (ok (equal (map 'string #'code-char #(0 103 117 101 115 116 0 112 119))
                   (getf start-ok :response))
            "SASL PLAIN response is NUL guest NUL pw")
        (ok (assoc "capabilities" (getf start-ok :client-properties) :test #'equal)
            "client capabilities are advertised"))
      (ok (= 2 (getf (%args w :connection-tune-ok) :heartbeat)) "heartbeat min(2, 60)")
      (ok (= 2047 (getf (%args w :connection-tune-ok) :channel-max)))
      (ok (equal "/v" (getf (%args w :connection-open) :virtual-host))))
    (ok (= 2 (mq-backend-amqp:amqp-connection-heartbeat conn)))
    (ok (equal "RabbitMQ" (cdr (assoc "product" (mq-backend-amqp:amqp-connection-server-properties conn)
                                      :test #'equal))))))

(deftest-parametrize heartbeat-negotiation
    ((client server expected)
     (nil 60 60) (0 60 0) (2 60 2) (120 60 60) (5 0 5) (nil 0 0))
  (let ((conn (%scripted (apply #'%frames
                                (append (%broker-hello :heartbeat server)
                                        '((:connection-open-ok 0 :reserved-1 "")
                                          (:channel-open-ok 1 :reserved-1 "")))))))
    (%handshake conn :heartbeat client)
    (ok (eql expected (mq-backend-amqp:amqp-connection-heartbeat conn))
        (format nil "client ~S, broker ~S → ~S" client server expected))))

(deftest handshake-refused-login-is-connection-closed
  (multiple-value-bind (conn out)
      (%scripted (%frames (first (%broker-hello))
                          '(:connection-close 0 :reply-code 403
                            :reply-text "ACCESS_REFUSED - Login was refused"
                            :class-id 0 :method-id 0)))
    (let ((e (handler-case (progn (%handshake conn) nil)
               (mq-backend-amqp:amqp-connection-closed (e) e))))
      (ok (typep e 'mq-protocol:mq-connection-error) "an mq-connection-error")
      (ok (and e (eql 403 (mq-backend-amqp:amqp-reply-code e))))
      (ok (and e (search "ACCESS_REFUSED" (mq-backend-amqp:amqp-reply-text e))))
      (ok (equal '(:connection-start-ok :connection-close-ok) (%names (%written out)))
          "the broker's Close is answered with Close-Ok")
      (ok (mq-backend-amqp:amqp-connection-closed-p conn)))))

(deftest handshake-needs-plain
  (let ((conn (%scripted (%frames (first (%broker-hello :mechanisms "AMQPLAIN EXTERNAL"))))))
    (ok (signals (%handshake conn) 'mq-protocol:mq-connection-error))))

(deftest rpc-skips-heartbeats-and-keeps-deliveries
  (multiple-value-bind (conn out)
      (%scripted (%frames :heartbeat
                          '(:basic-deliver 1 :consumer-tag "c" :delivery-tag 5 :redelivered nil
                            :exchange "" :routing-key "q")
                          '(:header 1 3)
                          (list :body 1 (coerce #(97 98 99) '(vector (unsigned-byte 8))))
                          :heartbeat
                          '(:basic-get-empty 1 :reserved-1 "")))
    (let ((reply (mq-backend-amqp:amqp-rpc conn :basic-get '(:basic-get-ok :basic-get-empty)
                                           :channel 1 :reserved-1 0 :queue "q" :no-ack nil)))
      (ok (eq :basic-get-empty (mq-backend-amqp:amqp-method-name reply)))
      (ok (equal '(:basic-get) (%names (%written out))))
      (let ((pending (mq-backend-amqp::%pending-deliveries conn)))
        (ok (= 1 (length pending)) "the Deliver that came first is kept")
        (ok (and pending (equalp #(97 98 99) (mq-protocol:mq-message-payload (first pending)))))
        (ok (and pending (eql 5 (mq-protocol:mq-message-delivery-tag (first pending)))))))))

(deftest channel-close-signals-and-recovers
  (multiple-value-bind (conn out)
      (%scripted (%frames '(:channel-close 1 :reply-code 404
                            :reply-text "NOT_FOUND - no queue 'x'" :class-id 60 :method-id 70)
                          '(:channel-open-ok 1 :reserved-1 "")))
    (let ((e (handler-case
                 (progn (mq-backend-amqp:amqp-rpc conn :basic-get '(:basic-get-ok :basic-get-empty)
                                                  :channel 1 :reserved-1 0 :queue "x" :no-ack nil)
                        nil)
               (mq-backend-amqp:amqp-channel-closed (e) e))))
      (ok (typep e 'mq-protocol:mq-error))
      (ok (and e (eql 404 (mq-backend-amqp:amqp-reply-code e))))
      (ok (and e (eql 60 (mq-backend-amqp:amqp-failing-class-id e))
               (eql 70 (mq-backend-amqp:amqp-failing-method-id e)))
          "the failing method is Basic.Get")
      (ok (not (mq-backend-amqp::%channel-open-p conn))))
    (mq-backend-amqp::%ensure-channel conn)
    (ok (mq-backend-amqp::%channel-open-p conn) "Channel.Open again")
    (ok (equal '(:basic-get :channel-close-ok :channel-open) (%names (%written out))))))

(deftest publish-splits-the-body-at-frame-max
  (multiple-value-bind (conn out) (%scripted #() :frame-max 128)
    (let ((backend (mq-backend-amqp:make-amqp-backend :connection conn)))
      (mq-protocol:publish backend "q" (mq-protocol:make-mq-message
                                        :payload (make-array 300 :element-type '(unsigned-byte 8)
                                                                 :initial-element 7)))
      (ok (equal '(1 2 3 3 3) (mapcar #'first (%written out)))
          "method, header, three body frames of at most 120 octets")))
  (multiple-value-bind (conn out) (%scripted #())
    (mq-protocol:publish (mq-backend-amqp:make-amqp-backend :connection conn) "q"
                         (mq-protocol:make-mq-message :payload ""))
    (ok (equal '(1 2) (mapcar #'first (%written out))) "an empty body has no body frame")))

(deftest heartbeat-tick-sends-when-idle
  (multiple-value-bind (conn out) (%scripted #())
    (setf (mq-backend-amqp:amqp-connection-heartbeat conn) 1)
    (setf (mq-backend-amqp::%last-sent conn)
          (- (get-internal-real-time) (* 2 internal-time-units-per-second)))
    (ok (mq-backend-amqp:amqp-heartbeat-tick conn) "idle past half the interval: send")
    (ok (null (mq-backend-amqp:amqp-heartbeat-tick conn)) "just sent: nothing")
    (ok (equal '((8 0 nil)) (%written out)) "one heartbeat frame on channel 0"))
  (multiple-value-bind (conn out) (%scripted #())
    (setf (mq-backend-amqp::%last-sent conn) 0)
    (ok (null (mq-backend-amqp:amqp-heartbeat-tick conn)) "heartbeat 0: never")
    (ok (null (%written out)))))

(deftest-parametrize issue-3-methods-decode
    ((name class-id method-id)
     (:queue-declare-ok 50 11) (:queue-bind-ok 50 21) (:exchange-declare-ok 40 11)
     (:basic-consume-ok 60 21) (:basic-qos-ok 60 11) (:basic-get 60 70)
     (:basic-get-ok 60 71) (:basic-get-empty 60 72) (:channel-close 20 40)
     (:channel-close-ok 20 41) (:connection-close 10 50) (:connection-close-ok 10 51))
  (let ((m (mq-backend-amqp:decode-method (mq-backend-amqp:encode-method name))))
    (ok (and (eq name (mq-backend-amqp:amqp-method-name m))
             (= class-id (mq-backend-amqp:amqp-method-class-id m))
             (= method-id (mq-backend-amqp:amqp-method-method-id m)))
        (format nil "~S is ~D/~D" name class-id method-id))))

(deftest issue-3-wire-example
  (let ((m (mq-backend-amqp:decode-method #(0 50 0 11 0 0 0 0 0 0 0 0 0))))
    (ok (eq :queue-declare-ok (mq-backend-amqp:amqp-method-name m)))
    (ok (equal '("" 0 0) (let ((a (mq-backend-amqp:amqp-method-arguments m)))
                           (list (getf a :queue) (getf a :message-count)
                                 (getf a :consumer-count)))))))

(deftest-parametrize parse-amqp-urls
    ((url expected)
     ("amqp://localhost" '(:host "localhost" :port 5672 :tls nil :vhost "/"))
     ("amqps://u:p%40ss@mq.example.com/%2f"
      '(:host "mq.example.com" :port 5671 :tls t :vhost "/" :user "u" :password "p@ss"))
     ("amqp://guest:guest@127.0.0.1:15673/orders"
      '(:host "127.0.0.1" :port 15673 :tls nil :vhost "orders" :user "guest" :password "guest"))
     ("amqp://h/" '(:host "h" :port 5672 :tls nil :vhost "")))
  (ok (equal expected (mq-backend-amqp:parse-amqp-url url))
      (format nil "~A → ~S" url (mq-backend-amqp:parse-amqp-url url))))

(deftest bad-amqp-url
  (ok (signals (mq-backend-amqp:parse-amqp-url "http://h/") 'mq-protocol:mq-error)))

(deftest-parametrize bad-amqp-url-errors-mask-the-password
    ((url shown)
     ("http://app:s3cret@rabbit.internal/" "http://app:***@rabbit.internal/")
     ("amqp://app:s3cret@/vh" "amqp://app:***@/vh")
     ("amqps://:p@s3cret@" "amqps://:***@"))
  ;; #7: AMQP_URL comes from configuration; reports end up in logs.
  (let ((text (handler-case (progn (mq-backend-amqp:parse-amqp-url url) "no error")
                (mq-protocol:mq-error (e) (princ-to-string e)))))
    (ok (not (search "s3cret" text)) (format nil "no password in: ~A" text))
    (ok (search shown text) (format nil "~A shows ~A" text shown))))

;;; --- live (RabbitMQ; see connect-test.lisp) ---------------------------------

(defun %live (&rest args)
  (apply #'mq-backend-amqp:connect-amqp-backend :host (%host) :port (%port)
         :user (%user) :password (%password) args))

(deftest live-missing-queue-is-channel-closed-then-recovers
  (with-broker ()
    (let ((b (%live)))
      (unwind-protect
          (let ((e (handler-case (progn (mq-protocol:poll-messages b (%queue-name "missing")) nil)
                     (mq-backend-amqp:amqp-channel-closed (e) e))))
            (ok (and e (eql 404 (mq-backend-amqp:amqp-reply-code e)))
                (format nil "Basic.Get on a missing queue: ~A" e))
            (let ((q (mq-backend-amqp:amqp-declare-queue b (%queue-name "after")
                                                         :exclusive t :auto-delete t)))
              (mq-protocol:publish b q (mq-protocol:make-mq-message :payload "ok"))
              (ok (equal '("ok") (mapcar (lambda (m) (map 'string #'code-char
                                                          (mq-protocol:mq-message-payload m)))
                                         (mq-protocol:poll-messages b q :timeout 2)))
                  "the channel is reopened and works")))
        (close b)))))

(deftest live-exchange-bind-route
  (with-broker ()
    (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                          :user (%user) :password (%password))
      (let ((ex (%queue-name "ex"))
            (q (%queue-name "bound")))
        (mq-backend-amqp:amqp-declare-exchange b ex :type "direct" :auto-delete t)
        (mq-backend-amqp:amqp-declare-queue b q :exclusive t :auto-delete t)
        (mq-backend-amqp:amqp-bind-queue b q ex :routing-key "orders.new")
        (mq-protocol:publish b (cons ex "orders.new") (mq-protocol:make-mq-message :payload "o-7"))
        (let ((m (first (mq-protocol:poll-messages b q :timeout 2))))
          (ok (and m (equalp (%octets "o-7") (mq-protocol:mq-message-payload m)))
              "published to the exchange, polled from the bound queue")
          (ok (and m (equal ex (mq-protocol:mq-message-exchange m))))
          (when m (mq-protocol:nack b m :requeue t)))
        (let ((again (first (mq-protocol:poll-messages b q :timeout 2))))
          (ok (and again (mq-protocol:mq-message-redelivered-p again))
              "a requeued nack comes back redelivered")
          (when again (mq-protocol:ack b again)))
        (multiple-value-bind (name count) (mq-backend-amqp:amqp-declare-queue b q :passive t)
          (ok (and (equal q name) (eql 0 count)) "passive declare: the queue is empty"))))))

(deftest live-unknown-vhost-is-connection-closed
  (with-broker ()
    (let ((e (handler-case (progn (close (%live :vhost "no-such-vhost")) nil)
               (mq-protocol:mq-connection-error (e) e))))
      (ok (typep e 'mq-backend-amqp:amqp-connection-closed) (format nil "~A" e))
      (ok (and e (member (mq-backend-amqp:amqp-reply-code e) '(403 530)))))))

(deftest live-large-payload-round-trips
  (with-broker ()
    (let ((q (%queue-name "big"))
          (payload (let ((v (make-array 400000 :element-type '(unsigned-byte 8))))
                     (dotimes (i (length v) v) (setf (aref v i) (mod (* i 31) 251))))))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (%declare-queue b q)
        (mq-protocol:publish b q (mq-protocol:make-mq-message :payload payload))
        (let ((msgs (mq-protocol:poll-messages b q :timeout 2)))
          (ok (and msgs (equalp payload (mq-protocol:mq-message-payload (first msgs))))
              (format nil "400000 octets over frame-max ~D"
                      (mq-backend-amqp:amqp-connection-frame-max
                       (mq-backend-amqp:amqp-backend-connection b)))))))))

(deftest live-url-and-synchronous-declare
  (with-broker ()
    (mq-backend-amqp:with-amqp-backend
        (b :url (format nil "amqp://~A:~A@~A:~D/%2f" (%user) (%password) (%host) (%port)))
      (let* ((conn (mq-backend-amqp:amqp-backend-connection b))
             (ok (mq-backend-amqp:amqp-rpc conn :queue-declare '(:queue-declare-ok)
                                           :channel 1 :reserved-1 0 :queue "" :passive nil
                                           :durable nil :exclusive t :auto-delete t
                                           :no-wait nil :arguments nil))
             (name (getf (mq-backend-amqp:amqp-method-arguments ok) :queue)))
        (ok (eql 0 (search "amq.gen-" name)) (format nil "server-named queue ~A" name))
        (mq-protocol:publish b name (mq-protocol:make-mq-message :payload "x" :key "k-1"))
        (let ((m (first (mq-protocol:poll-messages b name :timeout 2))))
          (ok (and m (equal "k-1" (mq-protocol:mq-message-key m))) "key round-trips")
          (ok (and m (equal name (mq-protocol:mq-message-topic m))) "routing key"))))))

(defvar *test-ca-loaded* nil)

(defun %tls-env ()
  (let ((port (uiop:getenv "MQ_AMQP_TEST_TLS_PORT"))
        (ca (uiop:getenv "MQ_AMQP_TEST_CA")))
    (when (and port (plusp (length port)) ca (probe-file ca))
      (unless *test-ca-loaded*
        (cl+ssl:ssl-load-global-verify-locations ca)
        (setf *test-ca-loaded* t))
      (parse-integer port))))

(deftest live-amqps
  (let ((port (%tls-env)))
    (if (null port)
        (skip "MQ_AMQP_TEST_TLS_PORT / MQ_AMQP_TEST_CA not set (see tests/session-test.lisp)")
        (progn
          (mq-backend-amqp:with-amqp-backend
              (b :url (format nil "amqps://~A:~A@127.0.0.1:~D" (%user) (%password) port))
            (let ((q (mq-backend-amqp:amqp-declare-queue b "" :exclusive t :auto-delete t)))
              (mq-protocol:publish b q (mq-protocol:make-mq-message :payload "over-tls"))
              (ok (equalp (%octets "over-tls")
                          (mq-protocol:mq-message-payload
                           (first (mq-protocol:poll-messages b q :timeout 2))))
                  "amqps:// round trip")))
          (ok (signals (close (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port
                                                                    :connect-timeout 2))
                'mq-protocol:mq-connection-error)
              "plain AMQP to the TLS port is a connection error")))))
