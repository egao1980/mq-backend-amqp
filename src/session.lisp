(in-package #:mq-backend-amqp)

;;; A live AMQP 0-9-1 session: socket (usocket; TLS through cl-stack-ssl's
;;; cl+ssl), protocol header, Start / Start-Ok (SASL PLAIN), Tune / Tune-Ok,
;;; Open, Channel.Open; synchronous replies; heartbeats; Close both ways.

;;; --- conditions ------------------------------------------------------------

(define-condition amqp-transport-error (mq-protocol:mq-connection-error)
  ((cause :initarg :cause :reader amqp-transport-error-cause :initform nil))
  (:report (lambda (c s)
             (format s "amqp connection error~@[: ~a~]" (mq-protocol:mq-error-message c))))
  (:documentation "The socket could not be opened or failed: CAUSE is the usocket,
stream or cl+ssl condition."))

(define-condition amqp-connection-closed (mq-protocol:mq-connection-error)
  ((reply-code :initarg :reply-code :reader amqp-reply-code :initform nil)
   (reply-text :initarg :reply-text :reader amqp-reply-text :initform nil)
   (class-id :initarg :class-id :reader amqp-failing-class-id :initform 0)
   (method-id :initarg :method-id :reader amqp-failing-method-id :initform 0))
  (:report (lambda (c s)
             (format s "AMQP connection closed by the broker: ~@[~d ~]~@[~a~]"
                     (amqp-reply-code c) (amqp-reply-text c))))
  (:documentation "The broker sent Connection.Close: 403 ACCESS_REFUSED (bad
credentials or vhost permission), 530 NOT_ALLOWED, 320 CONNECTION_FORCED, ..."))

(define-condition amqp-channel-closed (mq-protocol:mq-error)
  ((reply-code :initarg :reply-code :reader amqp-reply-code :initform nil)
   (reply-text :initarg :reply-text :reader amqp-reply-text :initform nil)
   (class-id :initarg :class-id :reader amqp-failing-class-id :initform 0)
   (method-id :initarg :method-id :reader amqp-failing-method-id :initform 0))
  (:report (lambda (c s)
             (format s "AMQP channel closed by the broker: ~@[~d ~]~@[~a~]"
                     (amqp-reply-code c) (amqp-reply-text c))))
  (:documentation "The broker sent Channel.Close: 404 NOT_FOUND, 406
PRECONDITION_FAILED, 403 ACCESS_REFUSED. The connection stays up; the next
operation opens the channel again."))

(define-condition amqp-consumer-cancelled (condition)
  ((consumer-tag :initarg :consumer-tag :reader amqp-cancelled-consumer-tag)
   (channel :initarg :channel :reader amqp-cancelled-channel :initform 1))
  (:report (lambda (c s)
             (format s "AMQP consumer ~s cancelled by the broker"
                     (amqp-cancelled-consumer-tag c))))
  (:documentation "SIGNALled (not an error) after the broker sent Basic.Cancel:
the consumed queue was deleted or its node failed (RabbitMQ consumer cancel
notification, which Start-Ok asks for). The tag is recorded in
AMQP-CONNECTION-CANCELLED-CONSUMERS at once; the condition is signalled when
the operation that read the cancel has its reply (the outermost backend
operation of the thread, or AMQP-RPC), with no lock held and no reply in
flight, as pika runs on_cancel after dispatching the frame. A handler may
use the connection, or unwind: POLL-MESSAGES keeps the messages it had
fetched for the next poll of that queue."))

(defun %endpoint (conn)
  (if (amqp-connection-host conn)
      (format nil "~A:~D" (amqp-connection-host conn) (amqp-connection-port conn))
      "amqp stream"))

(defmacro %with-transport-errors ((endpoint) &body body)
  "Run BODY; a socket, stream or TLS failure becomes AMQP-TRANSPORT-ERROR."
  `(handler-case (progn ,@body)
     ;; cl+ssl exports no base condition; CL+SSL-ERROR is its root.
     ((or usocket:socket-error usocket:ns-error stream-error cl+ssl::cl+ssl-error) (e)
       (error 'amqp-transport-error :message (format nil "~A: ~A" ,endpoint e)
                                    :cause e))))

;;; --- frames on a live connection ---------------------------------------------

(defun amqp-heartbeat-tick (connection)
  "Send a heartbeat frame when nothing went to the broker for half the
negotiated interval. Every backend operation calls this; call it yourself
from a loop that does no AMQP work for longer than the interval (the broker
drops a client silent for two intervals). → T when a heartbeat was sent."
  (let ((interval (amqp-connection-heartbeat connection)))
    (when (and (plusp interval)
               (not (amqp-connection-closed-p connection))
               (> (- (get-internal-real-time) (%last-sent connection))
                  (* interval internal-time-units-per-second 1/2)))
      (%with-transport-errors ((%endpoint connection))
        (write-amqp-frame connection (encode-heartbeat)))
      t)))

(defun %drop-socket (conn)
  "Release the socket, if the connection owns one."
  (let ((socket (%connection-socket conn))
        (stream (amqp-connection-stream conn)))
    (setf (amqp-connection-closed-p conn) t)
    (when socket
      (setf (%connection-socket conn) nil)
      ;; The connection is going away: its own transport errors on the
      ;; way down are of no further use.
      (handler-case
          (progn
            (when (streamp stream) (close stream :abort t))
            (usocket:socket-close socket))
        ((or stream-error usocket:socket-error cl+ssl::cl+ssl-error) () nil)))
    (and socket t)))

(defun %close-args (method)
  (let ((args (amqp-method-arguments method)))
    (list :reply-code (getf args :reply-code)
          :reply-text (getf args :reply-text)
          :class-id (getf args :class-id)
          :method-id (getf args :method-id))))

(defun %broker-closed-connection (conn method)
  (handler-case (write-amqp-method conn :connection-close-ok :channel 0)
    ((or stream-error usocket:socket-error cl+ssl::cl+ssl-error mq-protocol:mq-error) () nil))
  (%drop-socket conn)
  (apply #'error 'amqp-connection-closed :message "Connection.Close" (%close-args method)))

(defun %broker-closed-channel (conn frame method)
  "Channel.Close from the broker: answer Close-Ok and signal it. The
broker requeues the channel's unacked messages, so the delivered messages
not handed out yet are dropped: their delivery tags died with the channel."
  (write-amqp-method conn :channel-close-ok :channel (amqp-frame-channel frame))
  (setf (%channel-open-p conn) nil
        (%pending-deliveries conn) '())
  (apply #'error 'amqp-channel-closed :message "Channel.Close" (%close-args method)))

(defvar *amqp-operation-depth* 0
  "How many backend operations (or AMQP-RPCs) this thread is inside.
Broker notifications are signalled when the outermost one is done.")

(defun %signal-notifications (conn)
  "SIGNAL the broker notifications queued on CONN, oldest first, with no
lock held and no reply in flight. One a handler unwinds from is consumed;
the rest stay queued for the end of the next operation."
  (loop for note = (bt2:with-recursive-lock-held ((%connection-lock conn))
                     (pop (%pending-notifications conn)))
        while note
        do (apply #'signal note)))

(defmacro %with-notifications ((conn) &body body)
  "Run BODY one operation deeper; when the outermost operation returns
normally, signal CONN's queued broker notifications (%SIGNAL-NOTIFICATIONS).
CONN is evaluated after BODY."
  `(multiple-value-prog1
       (let ((*amqp-operation-depth* (1+ *amqp-operation-depth*)))
         ,@body)
     (when (zerop *amqp-operation-depth*)
       (%signal-notifications ,conn))))

(defmacro %with-reply-guard ((conn) &body body)
  "Run BODY, which reads a reply off CONN. Leaving BODY by a non-local exit
before it returns (an interrupt, a handler that unwinds) may leave part of
the reply unread, and the next request would read it as its own. The
connection is then dropped, so later operations fail with
MQ-CONNECTION-ERROR. The broker's Channel.Close (AMQP-CHANNEL-CLOSED) is
the exception: it was answered with Close-Ok and the broker sends nothing
more for that request, so the connection stays in step."
  (let ((done (gensym "DONE")) (in-step (gensym "IN-STEP")))
    `(let ((,done nil) (,in-step nil))
       (unwind-protect
           (handler-bind ((amqp-channel-closed
                           (lambda (c) (declare (ignore c)) (setf ,in-step t))))
             (multiple-value-prog1 (progn ,@body) (setf ,done t)))
         (unless (or ,done ,in-step)
           (%drop-socket ,conn))))))

(defun %broker-cancelled-consumer (conn frame method)
  "Basic.Cancel from the broker: answer Cancel-Ok unless it said no-wait
(RabbitMQ does), record the tag, and queue AMQP-CONSUMER-CANCELLED for
%SIGNAL-NOTIFICATIONS. It is not signalled here: a reply may be in flight,
and a handler that unwound from inside the read would leave it unread."
  (let ((tag (getf (amqp-method-arguments method) :consumer-tag))
        (channel (amqp-frame-channel frame)))
    (unless (getf (amqp-method-arguments method) :no-wait)
      (%with-transport-errors ((%endpoint conn))
                              (write-amqp-method conn :basic-cancel-ok :channel channel :consumer-tag tag)))
    (push tag (amqp-connection-cancelled-consumers conn))
    (setf (%pending-notifications conn)
          (append (%pending-notifications conn)
                  (list (list 'amqp-consumer-cancelled :consumer-tag tag :channel channel))))))

(defun %broker-flow (conn frame method)
  "Channel.Flow from the broker: record it and answer Flow-Ok."
  (let ((active (getf (amqp-method-arguments method) :active)))
    (setf (amqp-connection-flow-active conn) (and active t))
    (%with-transport-errors ((%endpoint conn))
                            (write-amqp-method conn :channel-flow-ok :channel (amqp-frame-channel frame)
                                               :active active))))

(defun %next-frame (conn)
  "The next frame that is not a heartbeat or a broker notification.
→ (values frame method-or-nil). A Connection.Close or Channel.Close from
the broker is answered with its Close-Ok and signalled. Connection.Blocked
/ Unblocked are recorded; Basic.Cancel (consumer cancel notification) and
Channel.Flow are answered and recorded. None of these is anyone's reply,
so the RPC in flight keeps waiting for its own."
  (loop
    (let ((frame (%with-transport-errors ((%endpoint conn))
                                         (read-amqp-frame conn))))
      (cond
       ((heartbeat-frame-p frame))
       ((/= (amqp-frame-type frame) +amqp-frame-method+)
        (return (values frame nil)))
       (t
        (let ((method (decode-method (amqp-frame-payload frame))))
          (case (amqp-method-name method)
            (:connection-close (%broker-closed-connection conn method))
            (:channel-close (%broker-closed-channel conn frame method))
            (:connection-blocked
             (setf (amqp-connection-blocked conn)
                   (or (getf (amqp-method-arguments method) :reason) t)))
            (:connection-unblocked
             (setf (amqp-connection-blocked conn) nil))
            (:basic-cancel (%broker-cancelled-consumer conn frame method))
            (:channel-flow (%broker-flow conn frame method))
            (t (return (values frame method))))))))))

(defun %read-content (conn)
  "The content header and body frames after Basic.Deliver / Get-Ok / Return.
Any other frame in between is an unexpected frame (%UNEXPECTED-FRAME).
→ (values properties body-octets)."
  (multiple-value-bind (frame method) (%next-frame conn)
    (when (or method (/= (amqp-frame-type frame) +amqp-frame-header+))
      (%unexpected-frame conn "expected a content header frame"))
    (let* ((header (decode-content-header frame))
           (size (getf header :body-size))
           (body (make-array size :element-type '(unsigned-byte 8)))
           (filled 0))
      (loop while (< filled size)
            do (multiple-value-bind (part method) (%next-frame conn)
                 (when (or method (/= (amqp-frame-type part) +amqp-frame-body+))
                   (%unexpected-frame conn "expected a content body frame"))
                 (let ((payload (amqp-frame-payload part)))
                   (replace body payload :start1 filled)
                   (incf filled (length payload)))))
      (values (getf header :properties) body))))

(defclass amqp-delivery (mq-protocol:mq-message)
  ((consumer-tag :initarg :consumer-tag :initform nil
                 :reader amqp-delivery-consumer-tag
                 :documentation "The consumer a Basic.Deliver was for; NIL for Basic.Get.")
   (queue :initarg :queue :initform nil :reader %delivery-get-queue
          :documentation "The queue of a Basic.Get-Ok."))
  (:documentation "A message the broker delivered: an MQ-MESSAGE that also
knows its consumer, so POLL-MESSAGES hands it out for its own queue only."))

(defun %delivery-queue (conn message)
  "The queue MESSAGE came from: its Basic.Get queue, or the queue its
consumer reads (recorded by SUBSCRIBE). NIL when unknown."
  (or (and (typep message 'amqp-delivery) (%delivery-get-queue message))
      (and (typep message 'amqp-delivery)
           (cdr (assoc (amqp-delivery-consumer-tag message) (%consumer-queues conn)
                       :test #'equal)))))

(defun %delivery-message (method properties body &key queue)
  "An AMQP-DELIVERY for a Basic.Deliver (it names its consumer) or a
Basic.Get-Ok (QUEUE is the queue it was got from) and its content."
  (let ((args (amqp-method-arguments method)))
    (make-instance 'amqp-delivery
                   :key (getf properties :correlation-id)
                   :payload body
                   :headers (getf properties :headers)
                   :delivery-tag (getf args :delivery-tag)
                   :redelivered-p (getf args :redelivered)
                   :exchange (getf args :exchange)
                   :topic (getf args :routing-key)
                   :consumer-tag (getf args :consumer-tag)
                   :queue queue)))

(defun %unexpected-frame (conn text)
  "A frame that no request is waiting for: AMQP 0-9-1 connection exception
505 UNEXPECTED_FRAME. Replies after it can no longer be matched to their
requests, so the client sends Connection.Close 505 (best effort, without
waiting for Close-Ok), drops the socket and signals AMQP-FRAME-ERROR.
Later operations on the connection fail with MQ-CONNECTION-ERROR instead
of reading the wrong reply."
  (when (%live-p conn)
    (handler-case
        (write-amqp-method conn :connection-close :channel 0
                           :reply-code 505 :reply-text "UNEXPECTED_FRAME"
                           :class-id 0 :method-id 0)
      ((or stream-error usocket:socket-error cl+ssl::cl+ssl-error mq-protocol:mq-error) () nil)))
  (%drop-socket conn)
  (error 'amqp-frame-error :message text))

(defun %await (conn replies)
  "Read until a method named in REPLIES arrives. Deliveries and returned
messages that come first are kept or skipped. Anything else is an
unexpected frame (see %UNEXPECTED-FRAME). → (values method frame)."
  (loop
    (multiple-value-bind (frame method) (%next-frame conn)
      (unless method
        (%unexpected-frame conn (format nil "unexpected frame type ~d while waiting for ~{~s~^ / ~}"
                                        (amqp-frame-type frame) replies)))
      (let ((name (amqp-method-name method)))
        (cond
         ((member name replies) (return (values method frame)))
         ((eq name :basic-deliver)
          (multiple-value-bind (properties body) (%read-content conn)
            (setf (%pending-deliveries conn)
                  (append (%pending-deliveries conn)
                          (list (%delivery-message method properties body))))))
         ((eq name :basic-return)
          ;; An unroutable mandatory publish coming back: not a reply.
          (%read-content conn))
         (t
          (%unexpected-frame conn (format nil "unexpected ~s while waiting for ~{~s~^ / ~}"
                                          name replies))))))))

(defun %check-open (conn)
  "Signal MQ-CONNECTION-ERROR if CONN was closed (by CLOSE, by the broker,
or after an unexpected frame)."
  (when (amqp-connection-closed-p conn)
    (error 'mq-protocol:mq-connection-error
           :message (format nil "~A: the AMQP connection is closed" (%endpoint conn)))))

(defun amqp-rpc (connection name replies &rest arguments)
  "Send method NAME (ARGUMENTS: its fields, plus :CHANNEL) and wait for one
of the method names REPLIES. A broker Close in between is signalled
(AMQP-CHANNEL-CLOSED / AMQP-CONNECTION-CLOSED). Broker notifications read
on the way (AMQP-CONSUMER-CANCELLED) are signalled after the reply, unless
AMQP-RPC runs inside a backend operation, which signals them when it is
done. A non-local exit before the reply is read drops the connection
(%WITH-REPLY-GUARD). → the reply AMQP-METHOD."
  (%with-notifications (connection)
                       (bt2:with-recursive-lock-held ((%connection-lock connection))
                         (%check-open connection)
                         (%with-reply-guard (connection)
                                            (%with-transport-errors ((%endpoint connection))
                                                                    (apply #'write-amqp-method connection name arguments))
                                            (values (%await connection replies))))))

(defun %ensure-channel (conn)
  "Reopen the default channel after the broker closed it."
  (unless (%channel-open-p conn)
    (amqp-rpc conn :channel-open '(:channel-open-ok)
              :channel (amqp-connection-channel conn) :reserved-1 "")
    (setf (%channel-open-p conn) t))
  conn)

;;; --- handshake -------------------------------------------------------------

(defparameter *amqp-client-properties*
  '(("product" . "cl-stack mq-backend-amqp")
    ("platform" . "Common Lisp")
    ("capabilities" . (("authentication_failure_close" . t)
                       ("basic.nack" . t)
                       ("connection.blocked" . t)
                       ("consumer_cancel_notify" . t)
                       ("publisher_confirms" . t))))
  "Start-Ok client properties. authentication_failure_close makes RabbitMQ
answer a bad login with Connection.Close 403 instead of dropping the socket.")

(defun %utf8 (string)
  (encoding-protocol:encode (or string "") :encoding :utf-8))

(defun %plain-response (user password)
  "SASL PLAIN (RFC 4616): NUL user NUL password, UTF-8."
  (concatenate '(simple-array (unsigned-byte 8) (*))
               #(0) (%utf8 user) #(0) (%utf8 password)))

(defun %negotiate (client server)
  "Tune value: 0 means no limit on that side; otherwise the lower wins."
  (cond ((zerop server) client)
        ((zerop client) server)
        (t (min client server))))

(defun %handshake (conn &key user password vhost heartbeat frame-max channel-max
                          locale client-properties)
  (%with-transport-errors ((%endpoint conn))
    (write-amqp-octets (amqp-connection-stream conn) +amqp-protocol-header+))
  (let* ((start (%await conn '(:connection-start)))
         (args (amqp-method-arguments start))
         (mechanisms (uiop:split-string (or (getf args :mechanisms) "") :separator " ")))
    (setf (amqp-connection-server-properties conn) (getf args :server-properties))
    (unless (member "PLAIN" mechanisms :test #'string=)
      (error 'mq-protocol:mq-connection-error
             :message (format nil "broker offers SASL ~{~A~^ ~}, not PLAIN" mechanisms)))
    (write-amqp-method conn :connection-start-ok
                       :channel 0
                       :client-properties (append client-properties *amqp-client-properties*)
                       :mechanism "PLAIN"
                       :response (%plain-response user password)
                       :locale locale))
  (let* ((tune (%await conn '(:connection-tune)))
         (args (amqp-method-arguments tune))
         (server-heartbeat (or (getf args :heartbeat) 0))
         (negotiated-heartbeat (cond ((null heartbeat) server-heartbeat)
                                     ((zerop heartbeat) 0)
                                     ((zerop server-heartbeat) heartbeat)
                                     (t (min heartbeat server-heartbeat)))))
    (setf (amqp-connection-channel-max conn)
          (%negotiate channel-max (or (getf args :channel-max) 0))
          (amqp-connection-frame-max conn)
          (%negotiate frame-max (or (getf args :frame-max) 0))
          (amqp-connection-heartbeat conn) negotiated-heartbeat)
    (write-amqp-method conn :connection-tune-ok
                       :channel 0
                       :channel-max (amqp-connection-channel-max conn)
                       :frame-max (amqp-connection-frame-max conn)
                       :heartbeat negotiated-heartbeat))
  (amqp-rpc conn :connection-open '(:connection-open-ok)
            :channel 0 :virtual-host vhost :reserved-1 "" :reserved-2 nil)
  (amqp-rpc conn :channel-open '(:channel-open-ok)
            :channel (amqp-connection-channel conn) :reserved-1 "")
  (setf (%channel-open-p conn) t)
  conn)

;;; --- amqp:// URLs ----------------------------------------------------------

(defun %redact-url (url)
  "URL with any password in its userinfo replaced by ***, for error
reports: URLs come from configuration and reports end up in logs. The
userinfo is taken to end at the LAST @, not at the first / ? or #, so a
password that holds one of those unescaped (base64 passwords hold /) is
masked too. This can over-mask a malformed URL, never under-mask it."
  (let* ((start (let ((p (search "://" url))) (if p (+ p 3) 0)))
         (at (position #\@ url :start start :from-end t))
         (colon (and at (position #\: url :start start :end at))))
    (if colon
        (concatenate 'string (subseq url 0 (1+ colon)) "***" (subseq url at))
        url)))

(defun parse-amqp-url (url)
  "Parse amqp://[user[:password]@]host[:port][/vhost] (amqps:// for TLS), the
RabbitMQ URI spec: the vhost is the percent-decoded path after its first
\"/\" (\"%2f\" is \"/\"); no path means the default vhost \"/\". User and
password are percent-decoded, so / ? # @ and % in them must be escaped.
→ plist :host :port :user :password :vhost :tls. Errors are MQ-ERRORs that
print the URL with the password masked (amqp://user:***@host)."
  (let ((shown (%redact-url url)))
    (flet ((refuse (what)
             (error 'mq-protocol:mq-error
                    :message (format nil "~A: ~A~:[~; (escape % / ? # @ in the user or ~
                                          password as %25 %2F %3F %23 %40)~]"
                                     what shown (find #\@ url)))))
      (let* ((uri (handler-case (quri:uri url)
                    (error () (refuse "malformed URL"))))
             (scheme (and (quri:uri-scheme uri) (string-downcase (quri:uri-scheme uri)))))
        (unless (member scheme '("amqp" "amqps") :test #'equal)
          (refuse "not an amqp:// or amqps:// URL"))
        (unless (plusp (length (or (quri:uri-host uri) "")))
          (refuse "no host in URL"))
        ;; An unescaped / ? or # in the password ends quri's authority
        ;; early and puts the @host in the path, query or fragment: refuse
        ;; it rather than connect to the wrong host.
        (when (some (lambda (part) (find #\@ (or part "")))
                    (list (quri:uri-path uri) (quri:uri-query uri) (quri:uri-fragment uri)))
          (refuse "malformed URL"))
        (flet ((decode (text)
                 (handler-case (quri:url-decode text)
                   (error () (refuse "bad percent-escape in URL")))))
          (let* ((tls (string= scheme "amqps"))
                 (userinfo (quri:uri-userinfo uri))
                 (colon (and userinfo (position #\: userinfo)))
                 (path (quri:uri-path uri)))
            (append
             (list :host (quri:uri-host uri)
                   :port (or (quri:uri-port uri) (if tls 5671 5672))
                   :tls tls
                   :vhost (if (or (null path) (string= path ""))
                              "/"
                              (decode (subseq path 1))))
             (when userinfo
               (list :user (decode (subseq userinfo 0 (or colon (length userinfo))))))
             (when colon
               (list :password (decode (subseq userinfo (1+ colon))))))))))))

;;; --- open / close ----------------------------------------------------------

(defun open-amqp-connection (&rest args
                                   &key url host port user password vhost tls
                                   (tls-verify :required) heartbeat (frame-max 131072)
                                   (channel-max 0) (channel 1) (locale "en_US")
                                   client-properties (connect-timeout 10))
  "Connect to a broker and run the handshake. → an AMQP-CONNECTION that owns
its socket, with CHANNEL open.
HOST (\"127.0.0.1\"), PORT (5672, 5671 with TLS), USER / PASSWORD
(\"guest\"), VHOST (\"/\"), or URL amqp://user:pass@host:port/vhost; explicit
keywords win over URL parts. TLS through cl-stack-ssl with TLS-VERIFY
(:required). HEARTBEAT: seconds, NIL for the broker's proposal, 0 for none.
Signals AMQP-TRANSPORT-ERROR (refused, timed out, TLS) or
AMQP-CONNECTION-CLOSED (bad login 403, vhost 530), both MQ-CONNECTION-ERRORs."
  (declare (ignore host port user password vhost tls))
  (let ((from-url (and url (parse-amqp-url url))))
    (flet ((opt (key default)
             (let ((given (getf args key '%absent)))
               (if (eq given '%absent) (getf from-url key default) given))))
      (let* ((host (opt :host "127.0.0.1"))
             (tls (opt :tls nil))
             (port (opt :port (if tls 5671 5672)))
             (endpoint (format nil "~A:~D" host port))
             (socket (%with-transport-errors (endpoint)
                                             (usocket:socket-connect host port
                                                                     :element-type '(unsigned-byte 8)
                                                                     :timeout connect-timeout
                                                                     :nodelay t)))
             (conn nil)
             (done nil))
        (unwind-protect
            (%with-transport-errors (endpoint)
                                    (let ((stream (if tls
                                                      (cl+ssl:make-ssl-client-stream (usocket:socket-stream socket)
                                                                                     :hostname host
                                                                                     :verify tls-verify)
                                                      (usocket:socket-stream socket))))
                                      (setf conn (make-instance 'amqp-connection
                                                                :stream stream :socket socket
                                                                :host host :port port :channel channel))
                                      (%handshake conn :user (opt :user "guest") :password (opt :password "guest")
                                                       :vhost (opt :vhost "/") :heartbeat heartbeat
                                                       :frame-max frame-max :channel-max channel-max
                                                       :locale locale :client-properties client-properties)
                                      (setf done t)
                                      conn))
          (unless done
            (if conn
                (%drop-socket conn)
                (handler-case (usocket:socket-close socket)
                  ((or stream-error usocket:socket-error) () nil)))))))))

(defun %wait-for-close-ok (conn)
  "Read until Close-Ok (or the broker's own Close) after our Connection.Close.
On a plain socket each read waits at most 5 s."
  (loop
    (let ((socket (%connection-socket conn))
          (stream (amqp-connection-stream conn)))
      (when (and socket
                 (not (typep stream 'cl+ssl::ssl-stream))
                 (not (listen stream))
                 (not (usocket:wait-for-input socket :timeout 5 :ready-only t)))
        (return nil)))
    (let ((frame (read-amqp-frame conn)))
      (when (= (amqp-frame-type frame) +amqp-frame-method+)
        (case (amqp-method-name (decode-method (amqp-frame-payload frame)))
          (:connection-close-ok (return t))
          (:connection-close
           (write-amqp-method conn :connection-close-ok :channel 0)
           (return t)))))))

(defun close-amqp-connection (connection &key (reply-code 200) (reply-text "Goodbye") abort)
  "Close CONNECTION: Connection.Close, wait for Close-Ok, release the socket
(ABORT: just release it). → T if it was open."
  (bt2:with-recursive-lock-held ((%connection-lock connection))
    (unless (amqp-connection-closed-p connection)
      (unless abort
        ;; Best effort: the broker may already be gone, and the socket is
        ;; released either way.
        (handler-case
            (progn
              (write-amqp-method connection :connection-close
                                 :channel 0 :reply-code reply-code :reply-text reply-text
                                 :class-id 0 :method-id 0)
              (when (%connection-socket connection)
                (%wait-for-close-ok connection)))
          ((or mq-protocol:mq-error stream-error usocket:socket-error
               cl+ssl::cl+ssl-error)
            () nil)))
      (%drop-socket connection)
      t)))

(defmethod close ((connection amqp-connection) &key abort)
  (close-amqp-connection connection :abort abort))
