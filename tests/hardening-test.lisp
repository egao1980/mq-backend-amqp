(in-package #:mq-backend-amqp/tests)

;;; 0.2.1, after the round-4 adversarial evidence (adversarial-r4-test.lisp,
;;; mostly live or against a fake broker): the same rules on a scripted
;;; broker, so CI checks them without RabbitMQ.

(deftest-parametrize a-refused-publish-writes-nothing
    ((what message)
     ("a key over 255 octets" (mq-protocol:make-mq-message
                               :payload "m0" :key (make-string 300 :initial-element #\k)))
     ("a header with no field-table type" (mq-protocol:make-mq-message
                                           :payload "m0" :headers '(("third" . 1/3)))))
  (multiple-value-bind (conn out) (%scripted (%frames))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn)))
      (ok (signals (mq-protocol:publish b "q" message) 'mq-backend-amqp:amqp-frame-error)
          (format nil "~A is refused" what))
      (ok (null (%written out)) "and no frame of it was written")
      (mq-protocol:publish b "q" (mq-protocol:make-mq-message :payload "m1"))
      (ok (equal '(:basic-publish) (%names (%written out)))
          "the next publish goes out whole"))))

(deftest an-ack-of-every-delivery-names-the-highest-one-handed-out
  ;; Wire tag 0 with multiple would settle deliveries still in the socket.
  (multiple-value-bind (conn out)
      (%scripted (apply #'%frames (append (%get-ok 1 "m1") (%get-ok 2 "m2") (%get-ok 3 "m3"))))
    (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
           (m1 (mq-backend-amqp::%basic-get b conn "q"))
           (m2 (mq-backend-amqp::%basic-get b conn "q"))
           (m3 (mq-backend-amqp::%basic-get b conn "q")))
      (declare (ignore m1 m2))
      (%written out)
      (mq-protocol:ack b m3)
      (mq-protocol:ack b 0 :multiple t)
      (ok (equal '((:basic-ack 3 nil) (:basic-ack 2 t)) (%acks out))
          "ack of every delivery is sent as a multiple ack of 2, the highest unsettled tag")
      (ok (eq :settled (%refusal (lambda () (mq-protocol:ack b 0 :multiple t))))
          "with nothing unsettled left there is nothing to send")
      (ok (null (%acks out))))))

(deftest a-multiple-ack-skips-a-tag-already-settled
  ;; The broker closes the channel on a multiple ack whose own tag is not
  ;; outstanding; the highest unsettled tag below it settles the same set.
  (multiple-value-bind (conn out)
      (%scripted (apply #'%frames (append (%get-ok 1 "m1") (%get-ok 2 "m2"))))
    (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
           (m1 (mq-backend-amqp::%basic-get b conn "q"))
           (m2 (mq-backend-amqp::%basic-get b conn "q")))
      (declare (ignore m1))
      (%written out)
      (mq-protocol:nack b m2 :requeue t)
      (mq-protocol:ack b 2 :multiple t)
      (ok (equal '((:basic-nack 2 nil) (:basic-ack 1 t)) (%acks out))))))

(deftest an-ack-of-every-delivery-acks-around-one-not-handed-out
  ;; m1 and m3 handed out (Basic.Get); a2, for a consumer, came in between
  ;; and is kept for a poll. One multiple ack up to m1, then m3 alone.
  (multiple-value-bind (conn out)
      (%scripted (apply #'%frames (append (%get-ok 1 "m1")
                                          (%deliver "ctag-a" 2 "a2")
                                          (%get-ok 3 "m3"))))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn)))
      (mq-backend-amqp::%basic-get b conn "q")
      (mq-backend-amqp::%basic-get b conn "q")
      (%written out)
      (mq-protocol:ack b 0 :multiple t)
      (ok (equal '((:basic-ack 1 t) (:basic-ack 3 nil)) (%acks out))
          "a2, not handed out, is not covered"))))

(deftest a-plain-amqp-peer-on-the-wrong-port-is-a-connection-error
  ;; The fake peer answers the protocol header with what a TLS port sends
  ;; a plain client: octets that read as a huge frame.
  (multiple-value-bind (port thread)
      (%serve-once (lambda (srv socket)
                     (declare (ignore socket))
                     (mq-backend-amqp:read-amqp-octets (mq-backend-amqp:amqp-connection-stream srv) 8)
                     (mq-backend-amqp:write-amqp-octets
                      (mq-backend-amqp:amqp-connection-stream srv)
                      (map '(vector (unsigned-byte 8)) #'char-code "HTTP/1.1 400 Bad Request"))
                     (sleep 1)))
    (let ((e (handler-case (progn (close (mq-backend-amqp:connect-amqp-backend
                                          :host "127.0.0.1" :port port :connect-timeout 3))
                             nil)
               (error (e) e))))
      (ok (typep e 'mq-protocol:mq-connection-error)
          (format nil "an mq-connection-error (got ~S: ~A)" (and e (type-of e)) e))
      (bt2:join-thread thread))))

;;; SBCL's deadlines do not bound a plain socket read on Windows; there the
;;; frame reads wait for each octet instead (*BYTE-WISE-DEADLINE-READS*).
;;; These run that path on every OS.

(defmacro with-reads ((mode) &body body)
  "Run BODY with frame reads in MODE, :BYTE-WISE or :DEADLINE. :DEADLINE is
skipped on Windows: SBCL's with-deadline does not bound a plain socket read
there, which is why the backend reads byte-wise on Windows."
  `(if (and (eq ,mode :deadline) (uiop:os-windows-p))
       (skip "with-deadline does not bound plain socket reads on Windows (byte-wise reads are used there)")
       (let ((mq-backend-amqp::*byte-wise-deadline-reads* (eq ,mode :byte-wise)))
         ,@body)))

(deftest-parametrize a-frame-cut-short-fails-in-either-read-mode
    ((mode) (:byte-wise) (:deadline))
  (with-reads (mode)
    (multiple-value-bind (port thread)
        (%serve-once (lambda (srv socket)
                       (declare (ignore socket))
                       (%fake-handshake srv :heartbeat 1)
                       (%client-method srv)
                       (mq-backend-amqp:write-amqp-octets (mq-backend-amqp:amqp-connection-stream srv)
                                                          (subseq (%frames (%declare-ok "q")) 0 4))
                       (sleep 6)))
      (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 1))
             (start (get-internal-real-time))
             (e (handler-case (progn (mq-backend-amqp:amqp-declare-queue b "q") nil)
                  (mq-protocol:mq-error (e) e)))
             (waited (%seconds-since start)))
        (ok (typep e 'mq-backend-amqp:amqp-transport-error)
            (format nil "~S: amqp-transport-error (got ~S: ~A)" mode (and e (type-of e)) e))
        (ok (< waited 4) (format nil "~S: after two 1 s intervals (~,1F s)" mode waited))
        (close b)
        (bt2:join-thread thread)))))

(deftest-parametrize a-close-ok-cut-short-gives-up-in-either-read-mode
    ((mode) (:byte-wise) (:deadline))
  (with-reads (mode)
    (multiple-value-bind (port thread)
        (%serve-once (lambda (srv socket)
                       (declare (ignore socket))
                       (%fake-handshake srv :heartbeat 0)
                       (loop until (eq :connection-close (%client-method srv)))
                       (mq-backend-amqp:write-amqp-octets (mq-backend-amqp:amqp-connection-stream srv)
                                                          (subseq (%frames '(:connection-close-ok 0)) 0 4))
                       (sleep 8)))
      (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 0))
             (start (get-internal-real-time)))
        (close b)
        (ok (< (%seconds-since start) 6.5)
            (format nil "~S: close returns within about 5 s (~,1F s)" mode (%seconds-since start)))
        (bt2:join-thread thread)))))

(deftest-parametrize a-silent-plain-server-fails-the-connect-in-either-read-mode
    ((mode) (:byte-wise) (:deadline))
  (with-reads (mode)
    (multiple-value-bind (port thread)
        (%serve-once (lambda (srv socket)
                       (declare (ignore srv socket))
                       (sleep 6)))
      (let* ((start (get-internal-real-time))
             (e (handler-case
                    (progn (close (mq-backend-amqp:connect-amqp-backend
                                   :host "127.0.0.1" :port port :connect-timeout 2))
                      nil)
                  (mq-protocol:mq-error (e) e)))
             (waited (%seconds-since start)))
        (ok (typep e 'mq-protocol:mq-connection-error)
            (format nil "~S: an mq-connection-error (got ~S: ~A)" mode (and e (type-of e)) e))
        (ok (< waited 4) (format nil "~S: within the 2 s connect-timeout (~,1F s)" mode waited))
        (bt2:join-thread thread)))))

(deftest live-byte-wise-reads-carry-a-large-message
  (with-broker ()
    (with-reads (:byte-wise)
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password) :heartbeat 5)
        (let ((q (mq-backend-amqp:amqp-declare-queue b "" :exclusive t :auto-delete t))
              (big (make-array 400000 :element-type '(unsigned-byte 8) :initial-element 9)))
          (mq-protocol:publish b q (mq-protocol:make-mq-message :payload big))
          (let ((got (first (mq-protocol:poll-messages b q :timeout 3))))
            (ok (and got (equalp big (mq-protocol:mq-message-payload got)))
                "a 400 000-byte message read octet by octet under heartbeat deadlines")
            (when got (mq-protocol:ack b got))))))))

(deftest another-channels-close-is-not-this-requests-answer
  (multiple-value-bind (conn out)
      (%scripted (%frames '(:channel-close 2 :reply-code 404 :reply-text "NOT_FOUND"
                            :class-id 60 :method-id 40)
                          (%declare-ok "q1")
                          '(:channel-open-ok 2 :reserved-1 "")))
    (let ((b1 (mq-backend-amqp:make-amqp-backend :connection conn)))
      (ok (equal "q1" (mq-backend-amqp:amqp-declare-queue b1 "q1"))
          "channel 1's Queue.Declare gets its own Declare-Ok, past channel 2's close")
      (ok (member :channel-close-ok (%names (%written out)))
          "channel 2's close was answered")
      (ok (not (mq-backend-amqp::%channel-open-p conn 2)) "channel 2 is closed")
      (let ((e (handler-case (progn (mq-backend-amqp::%ensure-channel conn 2) nil)
                 (mq-backend-amqp:amqp-channel-closed (e) e))))
        (ok (and e (eql 404 (mq-backend-amqp:amqp-reply-code e)))
            "the next operation on channel 2 reports its close, once"))
      (mq-backend-amqp::%ensure-channel conn 2)
      (ok (mq-backend-amqp::%channel-open-p conn 2) "and the one after reopens it")
      (ok (equal '(:channel-open) (%names (%written out)))))))

(deftest a-delivery-on-another-channel-is-not-acked-here
  (multiple-value-bind (conn out)
      (%scripted (apply #'%frames (append (%get-ok 1 "m1"))))
    (let* ((b1 (mq-backend-amqp:make-amqp-backend :connection conn))
           (b2 (mq-backend-amqp:make-amqp-backend :connection conn :channel 2))
           (m1 (mq-backend-amqp::%basic-get b1 conn "q")))
      (%written out)
      (ok (eq :other-channel (%refusal (lambda () (mq-protocol:ack b2 m1)))))
      (ok (null (%acks out))))))

(deftest subscribe-with-prefetch-sends-basic-qos-first
  (multiple-value-bind (conn out) (%scripted (%frames))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn)))
      (mq-protocol:subscribe b "q" (lambda (m) (declare (ignore m))) :no-wait t :prefetch 10)
      (let ((w (%written out)))
        (ok (equal '(:basic-qos :basic-consume) (%names w)))
        (ok (eql 10 (getf (%args w :basic-qos) :prefetch-count)))))))

(deftest a-frame-over-frame-max-is-refused-before-its-payload
  ;; A peer that is not an AMQP broker: "HTTP/1.1 400" read as a frame
  ;; header says 1 345 270 062 octets (#x502F312E, "P/1.") follow.
  (let ((conn (%scripted (map '(vector (unsigned-byte 8)) #'char-code "HTTP/1.1 400 Bad"))))
    (let ((e (handler-case (progn (mq-backend-amqp:read-amqp-frame conn) nil)
               (error (e) e))))
      (ok (typep e 'mq-backend-amqp:amqp-frame-error)
          (format nil "amqp-frame-error, not an allocation (got ~S)" (and e (type-of e))))
      (ok (and e (search "frame-max" (princ-to-string e)))))))

(deftest a-quri-uri-parses-with-its-credentials
  (let ((got (mq-backend-amqp:parse-amqp-url (quri:uri "amqp://app:s3cret@rabbit.internal:5673/prod"))))
    (ok (equal '("rabbit.internal" 5673 "app" "s3cret" "prod")
               (list (getf got :host) (getf got :port) (getf got :user)
                     (getf got :password) (getf got :vhost))))))

(deftest a-url-that-is-no-string-is-refused-without-printing-it
  (let ((e (handler-case (progn (mq-backend-amqp:parse-amqp-url '(:password "s3cret")) nil)
             (mq-protocol:mq-error (e) (princ-to-string e)))))
    (ok (and e (not (search "s3cret" e))) (format nil "an mq-error that hides it (~A)" e))))

#+sbcl
(deftest a-password-given-as-a-function-stays-out-of-the-backtrace
  ;; A refused login (403 inside the handshake) with the password as a
  ;; function: no frame of the backtrace prints it.
  (multiple-value-bind (port thread)
      (%serve-once (lambda (srv socket)
                     (declare (ignore socket))
                     (mq-backend-amqp:read-amqp-octets (mq-backend-amqp:amqp-connection-stream srv) 8)
                     (%send srv (first (%broker-hello)))
                     (%client-method srv)
                     (%send srv '(:connection-close 0 :reply-code 403
                                  :reply-text "ACCESS_REFUSED" :class-id 0 :method-id 0))
                     (%client-method srv)))
    (let ((trace (block trace
                   (handler-bind ((mq-protocol:mq-connection-error
                                    (lambda (c)
                                      (declare (ignore c))
                                      (return-from trace
                                        (with-output-to-string (s)
                                          (sb-debug:print-backtrace :stream s :count 60))))))
                     (mq-backend-amqp:connect-amqp-backend
                      :url (format nil "amqp://app@127.0.0.1:~D/prod" port)
                      :password (lambda () "pw-s3cret") :connect-timeout 3)
                     nil))))
      (ok trace "the login was refused")
      (ok (and trace (not (search "pw-s3cret" trace)))
          "no frame of the backtrace prints the password")
      (bt2:join-thread thread))))
