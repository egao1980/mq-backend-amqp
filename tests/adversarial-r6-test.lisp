(in-package #:mq-backend-amqp/tests)

;;; Round-6 adversarial evidence against PR #11 (agent/amqp-hardening-0f71,
;;; 9172bfc). Each test says what a correct client does (pika 1.3.2 against
;;; the same broker or the same fake broker, or mq-protocol's own in-memory
;;; backend). Live tests need RabbitMQ (see connect-test.lisp); the
;;; fake-broker tests use %SERVE (adversarial-r4-test.lisp).

;;; Breaks (each test below fails on 9172bfc, except the :TLS case of 4,
;;; which passes and is there as the control):
;;;
;;; 1. PUBLISH of a delivered message ignores its DESTINATION: a queue name
;;;    is used only when the message has no exchange / topic of its own
;;;    (%DESTINATION-PAIR), and every delivery has both (Get-Ok / Deliver
;;;    fill them in). Forwarding a polled message to another queue sends it
;;;    back where it came from: the queue it is published to never gets it
;;;    (lost there), and one from a fanout exchange goes to every queue
;;;    bound to that exchange again (duplicated). PUBLISH's docstring says
;;;    the destination is "a queue name on the default exchange";
;;;    mq-protocol's in-memory backend and mq-backend-kafka publish to the
;;;    destination; pika has no such field (basic_publish("", "qb", body)
;;;    goes to qb).
;;; 2. Closing a channel with AMQP-RPC while the broker is closing it (an
;;;    earlier publish to a missing exchange) kills the connection. The
;;;    broker's Channel.Close 404 is taken as the whole answer, but
;;;    RabbitMQ also answers the client's Close with Close-Ok (its channel
;;;    is closing: AMQP 0-9-1 crossing closes). That Close-Ok reaches the
;;;    next operation as an unexpected frame: Connection.Close 505, every
;;;    unacked delivery of every channel requeued. pika: close() raises
;;;    ChannelClosedByBroker 404, reads the Close-Ok, and the connection
;;;    stays open.
;;; 3. A publish to a broker that stops reading (a paused container, a host
;;;    gone without an RST) blocks in the socket write until the peer lets
;;;    go: writes have no deadline, and nothing checks the heartbeat
;;;    meanwhile. pika raises AMQPHeartbeatTimeout after 12 s against the
;;;    same fake broker (heartbeat 1 s, 64 MB body).
;;; 4. :CONNECT-TIMEOUT does not bound the AMQP handshake on a plain socket
;;;    once Tune has set a heartbeat. The wait for Open-Ok goes through
;;;    %WAIT-FOR-FRAME, whose usocket WAIT-FOR-INPUT is a select that
;;;    SB-SYS:WITH-DEADLINE does not interrupt, and *READ-DEADLINE* is not
;;;    consulted there: a broker that stops after Tune with heartbeat 60
;;;    (RabbitMQ's default) holds a :connect-timeout 2 connect for 120 s.
;;;    Over TLS the same wait polls LISTEN and fails at 2 s. pika:
;;;    AMQPConnectorStackTimeout after 15 s.

;;; --- 1. a delivered message published to another queue ---------------------

(deftest a-polled-message-published-to-another-queue-goes-to-that-queue
  ;; m1 is got from "q" (its Get-Ok says exchange "", routing key "q"), then
  ;; published to "qb". mq-protocol's in-memory backend does the same and
  ;; files it under "qb" (shown first, as the reference).
  (let* ((mem (mq-protocol:make-in-memory-mq-backend))
         (reference
           (progn
             (mq-protocol:publish mem "q" (mq-protocol:make-mq-message :payload "m1"))
             (mq-protocol:publish mem "qb" (first (mq-protocol:poll-messages mem "q")))
             (list (length (mq-protocol:poll-messages mem "q" :n 5))
                   (length (mq-protocol:poll-messages mem "qb" :n 5))))))
    (ok (equal '(0 1) reference)
        (format nil "in-memory reference: q holds ~D, qb holds ~D" (first reference) (second reference)))
    (multiple-value-bind (conn out) (%scripted (apply #'%frames (%get-ok 1 "m1")))
      (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
             (m (mq-backend-amqp::%basic-get b conn "q")))
        (flexi-streams:get-output-stream-sequence out) ; the Basic.Get
        (mq-protocol:publish b "qb" m)
        (let ((args (%args (%written out) :basic-publish)))
          (ok (equal '("" "qb") (list (getf args :exchange) (getf args :routing-key)))
              (format nil "the Basic.Publish goes to exchange \"\", routing key \"qb\" ~
                           (sent exchange ~S, routing key ~S)"
                      (getf args :exchange) (getf args :routing-key))))))))

(deftest live-a-polled-message-published-to-another-queue-reaches-that-queue
  (with-broker ()
    (let ((qa (%queue-name "fwd-a")) (qb (%queue-name "fwd-b"))
          (polled nil) (in-a nil) (in-b nil))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (let ((conn (mq-backend-amqp:amqp-backend-connection b)))
          (mq-backend-amqp:amqp-declare-queue b qa)
          (mq-backend-amqp:amqp-declare-queue b qb)
          (mq-protocol:publish b qa (mq-protocol:make-mq-message :payload "m1"))
          (let ((m (first (mq-protocol:poll-messages b qa :timeout 2))))
            (setf polled (and m (%text m)))
            (when m
              (mq-protocol:publish b qb m)
              (mq-protocol:ack b m)))
          (sleep 0.3)
          (setf in-a (%queue-rpc conn qa :passive t)
                in-b (%queue-rpc conn qb :passive t))))
      (%drain-queue qa :timeout 0.5)
      (%drain-queue qb :timeout 0.5)
      (format t "~&    m1 polled from QA and published to QB: QA holds ~S, QB holds ~S~%" in-a in-b)
      (ok (equal "m1" polled) "m1 is polled from QA")
      (ok (eql 1 in-b) (format nil "the publish to QB puts m1 in QB (QB holds ~S)" in-b))
      (ok (eql 0 in-a) (format nil "and QA, where m1 was acked, is empty (QA holds ~S)" in-a)))))

(deftest live-a-fanout-delivery-forwarded-to-an-audit-queue-is-not-duplicated
  ;; Fanout exchange X feeds QA and QC. A worker on QA forwards each message
  ;; to QAUDIT and acks it.
  (with-broker ()
    (let ((x (%queue-name "fwd-x")) (qa (%queue-name "fwd-a")) (qc (%queue-name "fwd-c"))
          (audit (%queue-name "fwd-audit")) (counts nil))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (let ((conn (mq-backend-amqp:amqp-backend-connection b)))
          (mq-backend-amqp:amqp-declare-exchange b x :type "fanout")
          (dolist (q (list qa qc audit)) (mq-backend-amqp:amqp-declare-queue b q))
          (mq-backend-amqp:amqp-bind-queue b qa x)
          (mq-backend-amqp:amqp-bind-queue b qc x)
          (mq-protocol:publish b (cons x "") (mq-protocol:make-mq-message :payload "m1"))
          (let ((m (first (mq-protocol:poll-messages b qa :timeout 2))))
            (when m
              (mq-protocol:publish b audit m)
              (mq-protocol:ack b m)))
          (sleep 0.3)
          (setf counts (mapcar (lambda (q) (%queue-rpc conn q :passive t)) (list qa qc audit)))
          (mq-backend-amqp:amqp-rpc conn :exchange-delete '(:exchange-delete-ok)
                                    :channel 1 :reserved-1 0 :exchange x
                                    :if-unused nil :no-wait nil)))
      (dolist (q (list qa qc audit)) (%drain-queue q :timeout 0.5))
      (destructuring-bind (in-a in-c in-audit) counts
        (format t "~&    after the forward: QA ~S, QC ~S, QAUDIT ~S~%" in-a in-c in-audit)
        (ok (eql 1 in-audit) (format nil "QAUDIT holds the forwarded m1 (it holds ~S)" in-audit))
        (ok (eql 1 in-c) (format nil "QC still holds its one copy of m1 (it holds ~S)" in-c))
        (ok (eql 0 in-a) (format nil "QA, whose m1 was acked, is empty (it holds ~S)" in-a))))))

;;; --- 2. a channel close that crosses the broker's ----------------------------

(defun %crossing-close-script ()
  "The broker's side of a crossing close on channel 1: Channel.Close 404
for an earlier publish, then Close-Ok for the client's own Close (the
channel was closing), then Open-Ok for the reopen."
  (%frames '(:channel-close 1 :reply-code 404
             :reply-text "NOT_FOUND - no exchange 'missing' in vhost '/'"
             :class-id 60 :method-id 40)
           '(:channel-close-ok 1)
           '(:channel-open-ok 1 :reserved-1 "")))

(defun %close-channel-1 (conn)
  "Channel.Close of channel 1 through AMQP-RPC. → :CLOSE-OK, or the reply
code of the broker's own AMQP-CHANNEL-CLOSED."
  (handler-case
      (progn (mq-backend-amqp:amqp-rpc conn :channel-close '(:channel-close-ok)
                                       :channel 1 :reply-code 200 :reply-text "bye"
                                       :class-id 0 :method-id 0)
             :close-ok)
    (mq-backend-amqp:amqp-channel-closed (e) (mq-backend-amqp:amqp-reply-code e))))

(deftest a-channel-close-crossing-the-brokers-keeps-the-connection-in-step
  (let* ((conn (%scripted (%crossing-close-script)))
         (code (%close-channel-1 conn))
         (reopen (handler-case
                     (mq-backend-amqp:amqp-method-name
                      (mq-backend-amqp:amqp-rpc conn :channel-open '(:channel-open-ok)
                                                :channel 1 :reserved-1 ""))
                   (mq-protocol:mq-error (e) (format nil "~S: ~A" (type-of e) e)))))
    (ok (eql 404 code) (format nil "the close is answered by the broker's 404 (got ~S)" code))
    (ok (eq :channel-open-ok reopen)
        (format nil "the next request gets its own reply, Open-Ok (got ~A)" reopen))
    (ok (not (mq-backend-amqp:amqp-connection-closed-p conn)) "and the connection stays open")))

(deftest live-closing-a-channel-the-broker-is-closing-keeps-the-connection
  ;; pika 1.3.2, same broker: basic_publish("missing", "k", b"x"), then
  ;; channel.close() raises ChannelClosedByBroker (404, "NOT_FOUND - no
  ;; exchange ..."); its log shows the Close-Ok that follows; a
  ;; queue_declare on a new channel then works and the connection is open.
  (with-broker ()
    (let ((code nil) (after nil) (closed nil))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (let ((conn (mq-backend-amqp:amqp-backend-connection b)))
          (mq-protocol:publish b (cons (%queue-name "no-such-exchange") "k")
                               (mq-protocol:make-mq-message :payload "m1"))
          (setf code (%close-channel-1 conn))
          (sleep 0.3)
          (setf after (handler-case
                          (progn (mq-backend-amqp:amqp-declare-queue b "" :exclusive t)
                                 :returned)
                        (mq-protocol:mq-error (e) (format nil "~S: ~A" (type-of e) e)))
                closed (mq-backend-amqp:amqp-connection-closed-p conn))))
      (format t "~&    close: ~S; the Queue.Declare after it: ~A~%" code after)
      (ok (eql 404 code) (format nil "the close is answered by the broker's 404 (got ~S)" code))
      (ok (eq :returned after)
          (format nil "a Queue.Declare after it returns (got ~A)" after))
      (ok (not closed) "and the connection stays open"))))

;;; --- 3. a publish to a broker that stops reading -----------------------------

(deftest-parametrize a-publish-to-a-broker-that-stops-reading-fails-in-time
    ((transport) (:plain) (:tls))
  ;; The fake broker serves the handshake (heartbeat 1 s), then neither
  ;; reads nor sends until it is released (or 60 s pass). The client
  ;; publishes 32 MB, more than the socket buffers hold.
  (with-transport (transport)
    (let ((release nil))
      (multiple-value-bind (port thread)
          (%serve transport
                  (lambda (srv socket)
                    (declare (ignore socket))
                    (%fake-handshake srv :heartbeat 1)
                    (loop repeat 600 until release do (sleep 0.1))))
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
          (loop until (or outcome (> (%seconds-since start) 15)) do (sleep 0.1))
          (let* ((took (and outcome (%seconds-since start)))
                 (in-time outcome))
            ;; Released, the fake broker closes the socket: a write still
            ;; blocked then fails (broken pipe).
            (setf release t)
            (bt2:join-thread publisher)
            (bt2:join-thread thread)
            (close b :abort t)
            (format t "~&    ~S: ~:[still blocked in the write after 15 s; once the broker ~
                       closed the socket: ~A~;~:*~A after ~,1F s~]~%"
                    transport in-time (if in-time took outcome))
            (ok (and took (typep in-time 'mq-protocol:mq-connection-error))
                (format nil "~S: the publish fails with mq-connection-error within 15 s ~
                             (pika: AMQPHeartbeatTimeout after 12 s); ~:[it was still ~
                             blocked in the write after 15 s~;~:*got ~A after ~,1F s~]"
                        transport in-time took))))))))

;;; --- 4. a broker silent after Tune -----------------------------------------

(deftest-parametrize a-broker-silent-after-tune-fails-the-connect-in-time
    ((transport) (:plain) (:tls))
  ;; The fake broker proposes heartbeat 60 s in Tune (RabbitMQ's default),
  ;; reads Tune-Ok and Connection.Open, then answers nothing until it is
  ;; released (or 60 s pass). The client connects with :connect-timeout 2.
  ;; Left alone, the :PLAIN connect fails after 120.0 s ("nothing from the
  ;; broker for 120 s (two heartbeat intervals)"); the test lets the broker
  ;; go after 10 s. pika (defaults) against the same fake broker:
  ;; AMQPConnectorStackTimeout ("Timeout during AMQP handshake") after 15 s,
  ;; its stack_timeout.
  (with-transport (transport)
    (let ((release nil))
      (multiple-value-bind (port thread)
          (%serve transport
                  (lambda (srv socket)
                    (declare (ignore socket))
                    (mq-backend-amqp:read-amqp-octets (mq-backend-amqp:amqp-connection-stream srv) 8)
                    (%send srv (first (%broker-hello)))
                    (%client-method srv) ; Start-Ok
                    (%send srv (list :connection-tune 0 :channel-max 2047 :frame-max 131072
                                     :heartbeat 60))
                    (%client-method srv) ; Tune-Ok
                    (%client-method srv) ; Connection.Open, never answered
                    (loop repeat 600 until release do (sleep 0.1))))
        (let* ((outcome nil)
               (start (get-internal-real-time))
               (connector
                 (bt2:make-thread
                  (lambda ()
                    (setf outcome
                          (handler-case
                              (progn (close (mq-backend-amqp:connect-amqp-backend
                                             :host "127.0.0.1" :port port :connect-timeout 2
                                             :tls (eq transport :tls) :tls-verify nil)
                                            :abort t)
                                     :connected)
                            (error (e) e))))
                  :name "connector")))
          (loop until (or outcome (> (%seconds-since start) 10)) do (sleep 0.1))
          (let* ((took (and outcome (%seconds-since start)))
                 (in-time outcome))
            ;; Released, the fake broker closes the socket: a connect still
            ;; waiting then fails (unexpected EOF).
            (setf release t)
            (bt2:join-thread connector)
            (bt2:join-thread thread)
            (format t "~&    ~S: ~:[the connect was still waiting after 10 s; once the broker ~
                       closed the socket: ~A~;~:*~A after ~,1F s~]~%"
                    transport in-time (if in-time took outcome))
            (ok (and took (< took 6) (typep in-time 'mq-backend-amqp:amqp-transport-error))
                (format nil "~S: the connect fails with amqp-transport-error about 2 s in ~
                             (:connect-timeout); ~:[it was still waiting after 10 s~;~:*got ~A ~
                             after ~,1F s~]"
                        transport in-time took))))))))
