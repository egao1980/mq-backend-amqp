(in-package #:mq-backend-amqp/tests)

;;; Round-4 adversarial evidence against PR #4 (agent/amqp-connect-eb10).
;;; Each test says what a correct client does (pika 1.3.2 against the same
;;; broker or the same fake broker). Live tests need RabbitMQ (see
;;; connect-test.lisp); the fake-broker tests run a broker thread on
;;; 127.0.0.1 (%SERVE-ONCE in heartbeat-test.lisp, %SERVE-TLS-ONCE here,
;;; which needs the openssl CLI for a throwaway certificate).

(defun %drain-queue (queue &key (timeout 2))
  "On a fresh connection: the texts of the messages on QUEUE (acked); then
QUEUE is deleted."
  (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                        :user (%user) :password (%password))
    (let ((got (mq-protocol:poll-messages b queue :n 50 :timeout timeout)))
      (dolist (m got) (mq-protocol:ack b m))
      (mq-backend-amqp:amqp-rpc (mq-backend-amqp:amqp-backend-connection b)
                                :queue-delete '(:queue-delete-ok)
                                :channel 1 :reserved-1 0 :queue queue
                                :if-unused nil :if-empty nil :no-wait nil)
      (mapcar #'%text got))))

(defun %outcome (thunk)
  "→ :RETURNED, or the type of the MQ-ERROR that THUNK signals."
  (handler-case (progn (funcall thunk) :returned)
    (mq-protocol:mq-error (e) (type-of e))))

(defvar *test-tls-pair* nil)

(defun %tls-pair ()
  "→ (certificate-path key-path): a self-signed certificate for 127.0.0.1,
made once with the openssl CLI; NIL without it."
  (or *test-tls-pair*
      (let* ((dir (merge-pathnames (format nil "mq-backend-amqp-tests-~D/"
                                           (random 100000000 (make-random-state t)))
                                   (uiop:temporary-directory)))
             (cert (merge-pathnames "cert.pem" dir))
             (key (merge-pathnames "key.pem" dir)))
        (ensure-directories-exist dir)
        (ignore-errors
         (uiop:run-program (list "openssl" "req" "-x509" "-newkey" "rsa:2048" "-nodes"
                                 "-keyout" (uiop:native-namestring key)
                                 "-out" (uiop:native-namestring cert)
                                 "-days" "1" "-subj" "/CN=127.0.0.1")
                           :output nil :error-output nil))
        (when (and (probe-file cert) (probe-file key))
          (setf *test-tls-pair* (list (uiop:native-namestring cert)
                                      (uiop:native-namestring key)))))))

(defun %serve-tls-once (script)
  "%SERVE-ONCE over TLS with %TLS-PAIR's certificate: SCRIPT gets an
AMQP-CONNECTION over the server end of the TLS stream, and the socket.
→ (values port thread)."
  (destructuring-bind (cert key) (%tls-pair)
    (let* ((listener (usocket:socket-listen "127.0.0.1" 0 :reuse-address t
                                                          :element-type '(unsigned-byte 8)))
           (port (usocket:get-local-port listener)))
      (values port
              (bt2:make-thread
               (lambda ()
                 (unwind-protect
                      (let ((socket (usocket:socket-accept listener)))
                        (unwind-protect
                             (handler-case
                                 (funcall script
                                          (mq-backend-amqp:make-amqp-connection
                                           :stream (cl+ssl:make-ssl-server-stream
                                                    (usocket:socket-stream socket)
                                                    :certificate cert :key key))
                                          socket)
                               (error () nil))
                          (ignore-errors (usocket:socket-close socket))))
                   (ignore-errors (usocket:socket-close listener))))
               :name "fake amqps broker")))))

(defun %serve (transport script)
  "SCRIPT as a fake broker over TRANSPORT, :PLAIN or :TLS. → (values port thread)."
  (if (eq transport :tls) (%serve-tls-once script) (%serve-once script)))

(defmacro with-transport ((transport) &body body)
  `(if (and (eq ,transport :tls) (null (%tls-pair)))
       (skip "no openssl CLI to make a test certificate")
       (progn ,@body)))

;;; 1. PUBLISH writes the Basic.Publish method frame before it encodes the
;;;    content header. A message the encoder refuses (a key over 255
;;;    octets, since correlation-id is a shortstr; a header value with no
;;;    field-table type) signals AMQP-FRAME-ERROR with the method frame
;;;    already on the wire. The broker then reads the next frame the client
;;;    sends, the next publish's method, where the content header belongs:
;;;    505 UNEXPECTED_FRAME, connection closed. Every publish until the
;;;    client next reads returns normally and is lost. pika marshals all
;;;    frames of a publish before it writes any ("in case the marshaling
;;;    operation throws an exception"): ShortStringTooLong, nothing sent,
;;;    and m1, m2 arrive.

(defun %unencodable (what)
  (ecase what
    (:long-key (mq-protocol:make-mq-message :payload "m0"
                                            :key (make-string 300 :initial-element #\k)))
    (:ratio-header (mq-protocol:make-mq-message :payload "m0"
                                                :headers '(("ratio" . 1/3))))))

(deftest-parametrize a-publish-the-encoder-refuses-sends-nothing
    ((what) (:long-key) (:ratio-header))
  (with-broker ()
    (let ((q (%queue-name "encode")) (refused nil) (later '()) (next nil))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (mq-backend-amqp:amqp-declare-queue b q)
        (setf refused (%outcome (lambda () (mq-protocol:publish b q (%unencodable what)))))
        (dolist (text '("m1" "m2"))
          (push (%outcome (lambda ()
                            (mq-protocol:publish b q (mq-protocol:make-mq-message :payload text))))
                later))
        (sleep 0.3)
        (setf next (handler-case (progn (mq-backend-amqp:amqp-declare-queue b q) :ok)
                     (mq-protocol:mq-error (e) (princ-to-string e)))))
      (let ((arrived (%drain-queue q)))
        (format t "~&    ~S: refused ~S; then publishes ~S; next RPC ~A; queue holds ~S~%"
                what refused (reverse later) next arrived)
        (ok (eq 'mq-backend-amqp:amqp-frame-error refused) "the message is refused")
        (ok (equal '("m1" "m2") arrived)
            (format nil "m1 and m2, published after it, returned normally, so both arrive ~
                         (queue holds ~S; the next RPC got ~A)"
                    arrived next))))))

;;; 2. A broker Channel.Close that answers no request (404 for a publish to
;;;    a missing exchange, 406 for an ack of an unknown tag, 403, ...) is
;;;    read only by the next RPC. PUBLISH reads nothing, so in a
;;;    publisher's loop every later publish goes to a channel the broker
;;;    closed, is discarded, and returns normally: lost without a trace for
;;;    as long as no RPC runs. pika's basic_publish processes incoming
;;;    frames: the next publish raises ChannelClosedByBroker (404) and the
;;;    one after it ChannelWrongStateError, so the caller knows.

(deftest publishes-after-a-broker-channel-close-are-not-silently-lost
  (with-broker ()
    (let ((q (%queue-name "after-404")) (outcomes '()) (next nil))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (mq-backend-amqp:amqp-declare-queue b q)
        (mq-protocol:publish b (cons "adv1-no-such-exchange" "k")
                             (mq-protocol:make-mq-message :payload "m0"))
        (sleep 0.3)
        (dolist (text '("m1" "m2" "m3"))
          (push (cons text (%outcome (lambda ()
                                       (mq-protocol:publish
                                        b q (mq-protocol:make-mq-message :payload text)))))
                outcomes)
          (sleep 0.1))
        (setf next (handler-case (progn (mq-backend-amqp:amqp-declare-queue b q) :ok)
                     (mq-protocol:mq-error (e) (princ-to-string e)))))
      (let ((arrived (%drain-queue q)))
        (format t "~&    publishes ~S; next RPC ~A; queue holds ~S~%" (reverse outcomes) next arrived)
        (dolist (o (reverse outcomes))
          (ok (or (not (eq :returned (cdr o))) (member (car o) arrived :test #'equal))
              (format nil "~A: its publish returned normally, so it reaches the queue ~
                           (queue holds ~S; the next RPC got ~A)"
                      (car o) arrived next)))))))

;;; 3. ACK 0 :MULTIPLE T sends Basic.Ack(0, multiple), which the broker
;;;    applies to every delivery outstanding on the channel. %CHECK-MULTIPLE
;;;    refuses it while a delivery that was READ is not handed out yet, but
;;;    a consumer's deliveries that reached the socket after the last read
;;;    are outstanding at the broker too, and are acked unseen. The README:
;;;    "a :multiple ack or nack that would also settle deliveries not
;;;    handed out yet ... would ack them unseen" is an mq-error. Here m2 is
;;;    acked although nobody saw it; the worker stops after the ack, and m2
;;;    is gone. (Sending the highest tag handed out, with multiple, instead
;;;    of 0 settles only what was seen.)

(deftest an-ack-of-every-delivery-does-not-ack-one-still-in-the-socket
  (with-broker ()
    (let ((q (%queue-name "ack-all")) (m1 nil) (ack nil))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (mq-backend-amqp:amqp-declare-queue b q)
        (mq-protocol:subscribe b q (lambda (m) (declare (ignore m))))
        (mq-protocol:publish b q (mq-protocol:make-mq-message :payload "m1"))
        (setf m1 (first (mq-protocol:poll-messages b q :timeout 2)))
        ;; The broker pushes m2 to the consumer; nothing reads it yet.
        (mq-protocol:publish b q (mq-protocol:make-mq-message :payload "m2"))
        (sleep 0.3)
        (setf ack (handler-case (progn (mq-protocol:ack b 0 :multiple t) :sent)
                    (mq-protocol:mq-error (e) (list :refused (princ-to-string e))))))
      ;; The worker stops here, before it saw m2.
      (let ((again (%drain-queue q)))
        (ok (equal "m1" (and m1 (%text m1))) "m1 was handed out")
        (ok (equal '("m2") again)
            (format nil "m2 was never handed out, so the ack of every delivery (~S) left it ~
                         unacked and it comes back (queue holds ~S)"
                    ack again))))))

;;; 4. The heartbeat deadline guards only the wait for the first octet of a
;;;    frame (%WAIT-FOR-FRAME). READ-AMQP-FRAME then reads the rest of the
;;;    header and the payload with a blocking READ-SEQUENCE that has no
;;;    deadline of its own and sends no heartbeats. So when the broker or
;;;    the network dies in the middle of a frame (a partition while a
;;;    131 KB body frame is in flight), the call over TLS waits until the
;;;    peer lets go, i.e. until TCP gives up; here it ends only because the
;;;    fake broker closes after 12 s ("unexpected eof"). Over plain TCP it
;;;    ends after :CONNECT-TIMEOUT (10 s by default), only because usocket
;;;    also makes that the stream's read timeout (see 6). The README
;;;    promises two heartbeat intervals. pika counts octets, not frames:
;;;    against the same fake broker (4 octets of Declare-Ok, then silence),
;;;    plain or TLS, queue_declare raises AMQPHeartbeatTimeout after 12 s
;;;    (its check interval is the timeout + 5 s) however long the peer
;;;    keeps the socket open.

(deftest-parametrize a-broker-silent-in-the-middle-of-a-frame-fails-after-two-intervals
    ((transport) (:plain) (:tls))
  (with-transport (transport)
    (multiple-value-bind (port thread)
        (%serve transport
                (lambda (srv socket)
                  (declare (ignore socket))
                  (%fake-handshake srv :heartbeat 1)
                  (%client-method srv)  ; Queue.Declare
                  ;; The first 4 octets of the Declare-Ok frame, then 12 s of silence.
                  (mq-backend-amqp:write-amqp-octets (mq-backend-amqp:amqp-connection-stream srv)
                                                     (subseq (%frames (%declare-ok "q")) 0 4))
                  (sleep 12)))
      (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 1
                                                      :tls (eq transport :tls) :tls-verify nil))
             (start (get-internal-real-time))
             (e (handler-case (progn (mq-backend-amqp:amqp-declare-queue b "q") nil)
                  (mq-protocol:mq-error (e) e)))
             (waited (%seconds-since start)))
        (ok (typep e 'mq-backend-amqp:amqp-transport-error)
            (format nil "~S: the Queue.Declare fails with amqp-transport-error (got ~S: ~A)"
                    transport (and e (type-of e)) e))
        (ok (< waited 4)
            (format nil "~S: after about two 1 s intervals, not when the broker lets go at 12 s ~
                         (waited ~,1F s)"
                    transport waited))
        (close b)
        (bt2:join-thread thread)))))

;;; 4b. The same read in CLOSE. %WAIT-FOR-CLOSE-OK's docstring: "On a
;;;     socket (plain or TLS) each read waits at most 5 s." The 5 s bound
;;;     is on the first octet only, so a Close-Ok cut short holds CLOSE,
;;;     and the cleanup of WITH-AMQP-BACKEND, for as long as the peer keeps
;;;     the socket open over TLS (10 s, the read timeout, over plain TCP).
;;;     This breaks the docstring only: pika's close() against the same
;;;     fake broker (heartbeat 0) also waits until the peer lets go.

(deftest-parametrize close-gives-up-on-a-close-ok-cut-short
    ((transport) (:plain) (:tls))
  (with-transport (transport)
    (multiple-value-bind (port thread)
        (%serve transport
                (lambda (srv socket)
                  (declare (ignore socket))
                  (%fake-handshake srv :heartbeat 0)
                  (loop until (eq :connection-close (%client-method srv)))
                  (mq-backend-amqp:write-amqp-octets (mq-backend-amqp:amqp-connection-stream srv)
                                                     (subseq (%frames '(:connection-close-ok 0)) 0 4))
                  (sleep 12)))
      (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 0
                                                      :tls (eq transport :tls) :tls-verify nil))
             (start (get-internal-real-time))
             (closed (handler-case (close b) (error (e) e)))
             (waited (%seconds-since start)))
        (ok (< waited 6.5)
            (format nil "~S: CLOSE returns after at most about 5 s, not when the peer lets go ~
                         at 12 s (took ~,1F s, returned ~S)"
                    transport waited closed))
        (bt2:join-thread thread)))))

;;; 5. An amqps:// URL to a server that accepts and never answers (a load
;;;    balancer with no live backend, a stalled broker, a port that waits
;;;    for the client to speak first) hangs OPEN-AMQP-CONNECTION: the TLS
;;;    handshake has no deadline, and :CONNECT-TIMEOUT bounds only the TCP
;;;    connect. Here it ends only because the server closes after 8 s.
;;;    (amqp:// to the same server ends after :CONNECT-TIMEOUT, by
;;;    usocket's read timeout, see 6.) pika bounds the whole set-up:
;;;    AMQPConnectorStackTimeout after stack_timeout (15 s by default)
;;;    against the same silent server.

(deftest an-amqps-server-that-never-answers-fails-the-connect-in-time
  (multiple-value-bind (port thread)
      (%serve-once (lambda (srv socket)
                     (declare (ignore srv socket))
                     (sleep 8)))
    (let* ((start (get-internal-real-time))
           (e (handler-case
                  (progn (close (mq-backend-amqp:connect-amqp-backend
                                 :url (format nil "amqps://guest:guest@127.0.0.1:~D/" port)
                                 :tls-verify nil :connect-timeout 2))
                         nil)
                (mq-protocol:mq-error (e) e)))
           (waited (%seconds-since start)))
      (ok (typep e 'mq-protocol:mq-connection-error)
          (format nil "an mq-connection-error (got ~S: ~A)" (and e (type-of e)) e))
      (ok (< waited 5)
          (format nil "amqps:// with :connect-timeout 2 gives up before the server lets go ~
                       at 8 s (waited ~,1F s)"
                  waited))
      (bt2:join-thread thread))))

;;; 6. OPEN-AMQP-CONNECTION passes CONNECT-TIMEOUT to usocket as :TIMEOUT,
;;;    which on SBCL is also the read timeout of the socket stream (usocket
;;;    backend/sbcl.lisp: read-timeout defaults to timeout). So over plain
;;;    TCP every read gives up after CONNECT-TIMEOUT (10 s by default):
;;;    with :HEARTBEAT 0 a reply slower than that fails with
;;;    AMQP-TRANSPORT-ERROR ("I/O timeout") and the connection is dropped,
;;;    so every message handed out is requeued and redelivered. The README:
;;;    "With :heartbeat 0 a call waits as long as the broker takes." pika
;;;    with heartbeat=0 and socket_timeout=2 gets the Declare-Ok that comes
;;;    after 3.5 s.

(deftest without-heartbeats-a-reply-slower-than-the-connect-timeout-is-waited-for
  (multiple-value-bind (port thread)
      (%serve-once
       (lambda (srv socket)
         (declare (ignore socket))
         (%fake-handshake srv :heartbeat 0)
         (%client-method srv)            ; Queue.Declare
         (sleep 3.5)
         (%send srv (%declare-ok "late"))
         (loop until (eq :connection-close (%client-method srv)))
         (%send srv '(:connection-close-ok 0))))
    (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port
                                                    :heartbeat 0 :connect-timeout 2))
           (got (handler-case (mq-backend-amqp:amqp-declare-queue b "late")
                  (mq-protocol:mq-error (e) (list :error (type-of e) (princ-to-string e))))))
      (ok (equal "late" got)
          (format nil "heartbeat 0 waits for the reply that comes after 3.5 s (got ~S)" got))
      (close b)
      (bt2:join-thread thread))))

;;; 7. A broker Channel.Close of ANOTHER channel of the connection (a second
;;;    backend on channel 2: MAKE-AMQP-BACKEND :CONNECTION :CHANNEL 2, the
;;;    channel opened with AMQP-RPC) is taken for the answer to the request
;;;    in flight on channel 1. That call fails with AMQP-CHANNEL-CLOSED,
;;;    %WITH-REPLY-GUARD counts it as in step, and channel 1's own reply
;;;    stays in the socket. The next call reads it as its own: a poll of QB
;;;    hands out QA's message, and every reply after it is one behind.
;;;    pika: a channel's close never answers another channel's RPC.

(deftest live-a-close-of-another-channel-does-not-answer-this-one
  (with-broker ()
    (let ((qa (%queue-name "ch-a")) (qb (%queue-name "ch-b")) (first-poll nil) (second-poll nil))
      (mq-backend-amqp:with-amqp-backend (b1 :host (%host) :port (%port)
                                             :user (%user) :password (%password))
        (let* ((conn (mq-backend-amqp:amqp-backend-connection b1))
               (b2 (progn (mq-backend-amqp:amqp-rpc conn :channel-open '(:channel-open-ok)
                                                    :channel 2 :reserved-1 "")
                          (mq-backend-amqp:make-amqp-backend :connection conn :channel 2))))
          (mq-backend-amqp:amqp-declare-queue b1 qa)
          (mq-backend-amqp:amqp-declare-queue b1 qb)
          (mq-protocol:publish b1 qa (mq-protocol:make-mq-message :payload "a1"))
          (mq-protocol:publish b1 qb (mq-protocol:make-mq-message :payload "b1"))
          ;; Channel 2 publishes to a missing exchange: RabbitMQ closes channel 2.
          (mq-protocol:publish b2 (cons "adv1-no-such-exchange" "k")
                               (mq-protocol:make-mq-message :payload "x"))
          (sleep 0.3)
          (flet ((poll (q)
                   (handler-case (mapcar #'%text (mq-protocol:poll-messages b1 q :timeout 2))
                     (mq-protocol:mq-error (e) (list :error (type-of e) (princ-to-string e))))))
            (setf first-poll (poll qa)
                  second-poll (poll qb)))))
      (%drain-queue qa :timeout 1)
      (%drain-queue qb :timeout 1)
      (ok (equal '("a1") first-poll)
          (format nil "channel 1's poll of QA gets a1; channel 2's close is not its answer (got ~S)"
                  first-poll))
      (ok (equal '("b1") second-poll)
          (format nil "the poll of QB gets b1, not another queue's message (got ~S)" second-poll)))))

;;; 8. PARSE-AMQP-URL (and :URL) takes the URL to be a string. A QURI:URI,
;;;    the stack's parsed URL, fails in %REDACT-URL's SEARCH with a
;;;    TYPE-ERROR (not an MQ-ERROR) whose report prints the URI with its
;;;    password.

(deftest a-url-object-is-not-reported-with-its-password
  (let ((text (handler-case
                  (progn (mq-backend-amqp:parse-amqp-url
                          (quri:uri "amqp://app:s3cret@rabbit.internal/prod"))
                         "parsed")
                (error (e) (format nil "~S: ~A" (type-of e) e)))))
    (ok (not (search "s3cret" text)) (format nil "no password in: ~A" text))))

;;; 9. Not one of the asked-for breaks (a refused login, no loss): quri's
;;;    url-decode turns + into a space, as form decoding does. RabbitMQ's
;;;    URI spec is percent-decoding only, and pika (urllib unquote) keeps
;;;    the +: URLParameters gives password 's3cret+x==' and vhost
;;;    'my+vhost'. Base64 passwords hold +; the README lists only / ? # @ %
;;;    as characters to escape.

(deftest-parametrize a-plus-in-an-amqp-url-stays-a-plus
    ((url password vhost)
     ("amqp://app:s3cret+x==@rabbit.internal/prod" "s3cret+x==" "prod")
     ("amqp://app:pw@rabbit.internal/my+vhost" "pw" "my+vhost"))
  (let ((got (mq-backend-amqp:parse-amqp-url url)))
    (ok (and (equal password (getf got :password)) (equal vhost (getf got :vhost)))
        (format nil "~A: password ~S, vhost ~S, as in pika (got ~S, ~S)"
                url password vhost (getf got :password) (getf got :vhost)))))
