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

(define-condition amqp-blocked-timeout (amqp-transport-error)
  ()
  (:documentation "A publish waited longer than the connection's
BLOCKED-CONNECTION-TIMEOUT for the broker to lift its Connection.Blocked
(a resource alarm): the connection is dropped, as pika's
ConnectionBlockedTimeout does."))

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

(define-condition amqp-stale-delivery (mq-protocol:mq-error)
  ((delivery-tag :initarg :delivery-tag :reader amqp-stale-delivery-tag :initform nil)
   (reason :initarg :reason :reader amqp-stale-delivery-reason :initform :channel-closed))
  (:report (lambda (c s)
             (format s "AMQP ack / nack of delivery ~@[~d ~]not sent: ~a"
                     (amqp-stale-delivery-tag c)
                     (case (amqp-stale-delivery-reason c)
                       (:channel-closed "its channel closed after it was delivered, and the tag now names another message (the broker requeued it when the channel closed)")
                       (:other-connection "it was delivered on another connection")
                       (:other-channel "it was delivered on another channel, whose tags these are not")
                       (:settled "it was already acked or nacked")
                       (:no-ack "its consumer is no-ack, so the broker waits for no ack")
                       (t (amqp-stale-delivery-reason c))))))
  (:documentation "ACK or NACK of a delivery that is not outstanding on the
backend's open channel. Nothing is sent: delivery tags are per channel,
and after a channel closes and reopens the same number names another message
(pika raises ChannelWrongStateError, aio-pika ChannelInvalidStateError).
REASON is :CHANNEL-CLOSED (the broker requeued the message when the channel
closed; it is redelivered), :OTHER-CONNECTION, :OTHER-CHANNEL, :SETTLED
(already acked or nacked, or covered by a multiple one) or :NO-ACK. Restart
CONTINUE skips the call (ACK / NACK return NIL), like the Java client's
recovery, which drops acks with stale tags."))

(defun %host-port (host port)
  "HOST:PORT for reports, an IPv6 literal in brackets ([::1]:5672)."
  (format nil (if (find #\: host) "[~A]:~D" "~A:~D") host port))

(defun %endpoint (conn)
  (if (amqp-connection-host conn)
      (%host-port (amqp-connection-host conn) (amqp-connection-port conn))
      "amqp stream"))

(defun %unbracket-host (host)
  "HOST without the brackets of an IPv6 literal (\"[::1]\" → \"::1\"): quri
keeps them, usocket cannot resolve them, and pika's URLParameters strips
them too."
  (if (and (stringp host)
           (>= (length host) 2)
           (char= #\[ (char host 0))
           (char= #\] (char host (1- (length host)))))
      (subseq host 1 (1- (length host)))
      host))

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
drops a client silent for two intervals). It takes the connection lock, so
it may run on another thread: it never lands between the frames of a
publish. → T when a heartbeat was sent."
  (bt2:with-recursive-lock-held ((%connection-lock connection))
    (let ((interval (amqp-connection-heartbeat connection)))
      (when (and (plusp interval)
                 (not (amqp-connection-closed-p connection))
                 (> (- (get-internal-real-time) (%last-sent connection))
                    (* interval internal-time-units-per-second 1/2)))
        (%with-transport-errors ((%endpoint connection))
          (write-amqp-frame connection (encode-heartbeat)))
        t))))

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

(defvar *awaited-channel* nil
  "The channel whose reply the read in progress waits for (or whose
operation is reading what the broker already sent), so that a broker
Channel.Close of another channel is not taken for its answer. NIL: a close
of any channel is signalled.")

(defun %retired-p (conn message)
  "Was delivery MESSAGE handed out on a channel of CONN that has closed
since? The broker requeued it then: its tag now names nothing, or another
message. A no-ack consumer's delivery is not retired: the broker settled
it on sending, so the close requeued nothing, and dropping it would lose it."
  (and (typep message 'amqp-delivery)
       (not (eq :no-ack (%delivery-settled message)))
       (integerp (mq-protocol:mq-message-delivery-tag message))
       (<= (mq-protocol:mq-message-delivery-tag message)
           (%cs-offset (%channel-state conn (%delivery-channel message))))))

(defun %retire-channel (conn channel)
  "CHANNEL of CONN is closed, and the broker requeued its unacked messages:
their tags die with it. The tags handed out are retired (the channel's
offset becomes its highest tag, so ACK of one signals AMQP-STALE-DELIVERY
instead of acking whatever the reopened channel delivers under the same
number), its deliveries not handed out yet are dropped (no-ack ones
excepted, see %RETIRED-P), and its consumers are gone."
  (let ((state (%channel-state conn channel)))
    (setf (%cs-open-p state) nil
          (%cs-offset state) (%cs-max-tag state)
          (%cs-consumers state) '()
          (%cs-no-ack state) '())
    (clrhash (%cs-unsettled state))
    (setf (%pending-deliveries conn)
          (remove-if (lambda (m) (%retired-p conn m)) (%pending-deliveries conn)))))

(defun %broker-closed-channel (conn frame method)
  "Channel.Close from the broker: answer Close-Ok and retire the channel's
deliveries (%RETIRE-CHANNEL). When it closes the channel whose reply is
awaited (*AWAITED-CHANNEL*, or any when none is), signal
AMQP-CHANNEL-CLOSED: it is that request's answer. A close of another
channel answers nothing here; it is recorded, the read goes on for the
awaited reply, and the next operation on that channel reports it
(%ENSURE-CHANNEL). A socket that fails under the Close-Ok is an
AMQP-TRANSPORT-ERROR, wherever the Close was read."
  (let ((channel (amqp-frame-channel frame)))
    (%retire-channel conn channel)
    (%with-transport-errors ((%endpoint conn))
      (write-amqp-method conn :channel-close-ok :channel channel))
    (if (or (null *awaited-channel*) (eql channel *awaited-channel*))
        (apply #'error 'amqp-channel-closed :message "Channel.Close" (%close-args method))
        (setf (%cs-close (%channel-state conn channel)) (%close-args method)))))

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
(RabbitMQ does), record the tag, forget the consumer on its channel (its
tag may be used again; its deliveries already read keep their queue), and
queue AMQP-CONSUMER-CANCELLED for %SIGNAL-NOTIFICATIONS. It is not
signalled here: a reply may be in flight, and a handler that unwound from
inside the read would leave it unread."
  (let ((tag (getf (amqp-method-arguments method) :consumer-tag))
        (channel (amqp-frame-channel frame)))
    (unless (getf (amqp-method-arguments method) :no-wait)
      (%with-transport-errors ((%endpoint conn))
        (write-amqp-method conn :basic-cancel-ok :channel channel :consumer-tag tag)))
    (push tag (amqp-connection-cancelled-consumers conn))
    (%forget-consumer conn channel tag)
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

(defun %call-with-deadline (seconds endpoint what thunk &key conn)
  "Call THUNK, which waits on a socket, for at most SECONDS (NIL: no limit).
Past that, CONN's socket (if given) is dropped and AMQP-TRANSPORT-ERROR
signalled: \"ENDPOINT: WHAT within SECONDS s\". On SBCL every wait for the
socket inside THUNK, plain or TLS, is bounded (SB-SYS:WITH-DEADLINE; cl+ssl
waits through it too); elsewhere THUNK runs unbounded."
  (if (null seconds)
      (funcall thunk)
      (progn
        #+sbcl
        (handler-case (sb-sys:with-deadline (:seconds seconds)
                        (funcall thunk))
          (sb-sys:deadline-timeout ()
            (when conn (%drop-socket conn))
            (error 'amqp-transport-error
                   :message (format nil "~A: ~A within ~,1F s" endpoint what seconds))))
        #-sbcl
        (funcall thunk))))

(defvar *read-deadline* nil
  "NIL, or the internal real time by which reads must be done whatever the
heartbeat: OPEN-AMQP-CONNECTION binds it to its CONNECT-TIMEOUT for the
handshake (%READ-FRAME-WITHIN).")

(defvar *byte-wise-deadline-reads* #+(and sbcl win32) t #-(and sbcl win32) nil
  "Bound a plain socket's frame reads by waiting for each octet (usocket
WAIT-FOR-INPUT, %DEADLINE-READER) rather than by SB-SYS:WITH-DEADLINE.
SBCL's deadlines bound TLS reads everywhere (cl+ssl waits through
SB-SYS:WAIT-UNTIL-FD-USABLE) but not a plain socket read on Windows, so
this is on there.")

(defclass %deadline-reader ()
  ((conn :initarg :conn :reader %dr-conn)
   (seconds :initarg :seconds :reader %dr-seconds
            :documentation "How long the socket may give nothing (NIL: only LIMIT).")
   (limit :initarg :limit :initform nil :reader %dr-limit
          :documentation "NIL, or the internal real time by which all reads are done.")
   (byte-wise :initarg :byte-wise :initform nil :reader %dr-byte-wise-p)
   (what :initarg :what :reader %dr-what))
  (:documentation "A READ-AMQP-OCTETS source over CONN's socket stream that
gives up when the socket gives nothing for SECONDS, or at LIMIT: the socket
is dropped and AMQP-TRANSPORT-ERROR signalled. Octets that keep coming are
waited for however long the frame takes (pika's heartbeat checker counts
octets too), and heartbeats are sent meanwhile. BYTE-WISE: each octet is
read once usocket's WAIT-FOR-INPUT says the socket has one (plain sockets
on Windows, see *BYTE-WISE-DEADLINE-READS*); otherwise chunks of 4096
octets are read, each under SB-SYS:WITH-DEADLINE (%CALL-WITH-DEADLINE)."))

(defmethod read-amqp-octets ((source %deadline-reader) n)
  (let ((conn (%dr-conn source))
        (buf (make-array n :element-type '(unsigned-byte 8))))
    (flet ((window ()
             ;; Seconds from now until the reader gives up, if nothing comes.
             (let ((limit (and (%dr-limit source)
                               (/ (- (%dr-limit source) (get-internal-real-time))
                                  internal-time-units-per-second)))
                   (seconds (%dr-seconds source)))
               (max 0 (if (and limit seconds) (min limit seconds) (or limit seconds)))))
           (eof (got)
             (error 'amqp-transport-error
                    :message (format nil "~A: unexpected EOF (got ~D of ~D)" (%endpoint conn) got n))))
      (if (%dr-byte-wise-p source)
          (let ((stream (amqp-connection-stream conn))
                (socket (%connection-socket conn))
                (deadline nil))
            (dotimes (i n buf)
              (setf deadline nil)
              (loop until (listen stream)
                    do (let ((left (if deadline
                                       (/ (- deadline (get-internal-real-time))
                                          internal-time-units-per-second)
                                       (let ((w (window)))
                                         (setf deadline (+ (get-internal-real-time)
                                                           (round (* w internal-time-units-per-second))))
                                         w))))
                         (unless (plusp left)
                           (%drop-socket conn)
                           (error 'amqp-transport-error
                                  :message (format nil "~A: ~A within ~,1F s"
                                                   (%endpoint conn) (%dr-what source) (window))))
                         (amqp-heartbeat-tick conn)
                         ;; Ready is an octet or the end of the stream:
                         ;; READ-BYTE does not block then.
                         (when (usocket:wait-for-input socket :timeout (float (min left 0.5))
                                                              :ready-only t)
                           (return))))
              (let ((octet (read-byte stream nil nil)))
                (unless octet (eof i))
                (setf (aref buf i) octet))))
          (let ((stream (amqp-connection-stream conn)))
            (loop with start = 0
                  while (< start n)
                  do (let ((end (min n (+ start 4096))))
                       (amqp-heartbeat-tick conn)
                       (%call-with-deadline (window) (%endpoint conn) (%dr-what source)
                                            (lambda ()
                                              (let ((got (read-sequence buf stream :start start :end end)))
                                                (unless (= got end) (eof got))))
                                            :conn conn)
                       (setf start end)))
            buf)))))

(defun %read-frame-within (conn seconds what)
  "READ-AMQP-FRAME on CONN, given up when the socket gives nothing for
SECONDS (NIL: no limit) or at *READ-DEADLINE*: the socket is dropped and
AMQP-TRANSPORT-ERROR signalled (\"WHAT within N s\"). A frame whose octets
keep coming is waited for (%DEADLINE-READER). Without a socket, or with no
bound at all, it is a plain READ-AMQP-FRAME."
  (if (and (%connection-socket conn) (or seconds *read-deadline*))
      (let ((handshake (and *read-deadline*
                            (or (null seconds)
                                (< (- *read-deadline* (get-internal-real-time))
                                   (* seconds internal-time-units-per-second))))))
        (read-amqp-frame conn (make-instance '%deadline-reader
                                             :conn conn
                                             :seconds seconds
                                             :limit *read-deadline*
                                             :what (if handshake "no AMQP handshake" what)
                                             :byte-wise (and *byte-wise-deadline-reads*
                                                             (not (typep (amqp-connection-stream conn)
                                                                         'cl+ssl::ssl-stream))))))
      (read-amqp-frame conn)))

;;; --- bounded writes ----------------------------------------------------------

(defparameter *write-chunk* 4096
  "Octets written to a live socket at a time when writes are bounded
(%WRITE-OCTETS-WITHIN): each chunk is progress, and the bound is on how
long one chunk may take.")

(defun %write-window (conn)
  "Seconds a write to CONN's socket may make no progress before the broker
is taken to be gone: two heartbeat intervals, and no later than
*READ-DEADLINE* (the handshake's CONNECT-TIMEOUT). NIL: no bound
(:heartbeat 0 after the handshake)."
  (let* ((interval (amqp-connection-heartbeat conn))
         (seconds (and (plusp interval) (* 2 interval)))
         (limit (and *read-deadline*
                     (max 0 (/ (- *read-deadline* (get-internal-real-time))
                               internal-time-units-per-second)))))
    (if (and seconds limit) (min seconds limit) (or seconds limit))))

#+(and sbcl win32)
(progn
  (sb-alien:define-alien-type nil
      (sb-alien:struct %ws-fd-set
                       ;; u_int fd_count, padded to the SOCKET size on Win64
                       (count sb-alien:signed)
                       (array (array sb-alien:signed 64))))
  (sb-alien:define-alien-type nil
      (sb-alien:struct %ws-timeval
                       (sec sb-alien:long)
                       (usec sb-alien:long)))
  (sb-alien:define-alien-routine ("select" %ws-select) sb-alien:int
    (nfds sb-alien:int)
    (readfds (* t))
    (writefds (* (sb-alien:struct %ws-fd-set)))
    (exceptfds (* t))
    (timeout (* (sb-alien:struct %ws-timeval))))

  (defun %socket-writable-within-p (conn seconds)
    "Does CONN's socket take octets within SECONDS? Winsock select(): a
blocking send() to a socket it calls writable does not block. SBCL's
deadlines do not bound a plain socket's write on Windows, so writes wait
for this first, chunk by chunk, as reads wait for each octet there."
    (let ((handle (sb-bsd-sockets:socket-file-descriptor
                   (usocket:socket (%connection-socket conn)))))
      (sb-alien:with-alien ((set (sb-alien:struct %ws-fd-set))
                            (tv (sb-alien:struct %ws-timeval)))
        (multiple-value-bind (sec frac) (floor (max 0 seconds))
          (setf (sb-alien:slot set 'count) 1
                (sb-alien:deref (sb-alien:slot set 'array) 0) handle
                (sb-alien:slot tv 'sec) sec
                (sb-alien:slot tv 'usec) (min 999999 (floor (* frac 1000000)))))
        (let ((rv (%ws-select 0 nil (sb-alien:addr set) nil (sb-alien:addr tv))))
          (when (minusp rv)
            (error 'amqp-transport-error
                   :message (format nil "~A: select() failed: WSA error ~D" (%endpoint conn)
                                    (sockint::wsa-get-last-error))))
          (plusp rv))))))

(defun %pending-input (conn)
  "How many octets the broker sent that CONN's socket holds unread, as the
kernel counts them (FIONREAD; on a unix without it, a peek of at most 8192
octets), or NIL when unknown. It grows while the broker sends, even when
nothing reads them, however much already waits (a prefetching consumer)."
  (let ((socket (%connection-socket conn)))
    (and socket
         (handler-case
             (progn
               #+(and sbcl win32)
               (usocket::bytes-available-for-read socket)
               #+(and sbcl (not win32))
               (let* ((fd (sb-bsd-sockets:socket-file-descriptor (usocket:socket socket)))
                      (fionread #+linux #x541B #+(or darwin bsd) #x4004667F
                                #-(or linux darwin bsd) nil)
                      (counted (and fionread
                                    (sb-alien:with-alien ((n sb-alien:int 0))
                                      (and (sb-unix:unix-ioctl
                                            fd fionread
                                            (sb-alien:cast (sb-alien:addr n) (* char)))
                                           n)))))
                 (or counted
                     (let ((buf (make-array 8192 :element-type '(unsigned-byte 8))))
                       (declare (dynamic-extent buf))
                       (multiple-value-bind (got length)
                           (sb-bsd-sockets:socket-receive (usocket:socket socket) buf nil
                                                          :peek t :dontwait t)
                         (if got length 0)))))
               #-sbcl nil)
           ((or usocket:socket-error #+sbcl sb-bsd-sockets:socket-error) () nil)))))

(defun %write-octets-within (conn octets)
  "Write OCTETS to CONN's stream. On a live socket whose writes are bounded
(%WRITE-WINDOW), they go in chunks of *WRITE-CHUNK*, and a broker that
takes none of a chunk for that long and sent nothing meanwhile (it stopped
reading and is gone: a paused container, a host gone without an RST) is
gone: the socket is dropped and AMQP-TRANSPORT-ERROR signalled, rather than
the write blocking until the peer lets go (pika: AMQPHeartbeatTimeout). A
broker that stops reading but keeps sending (RabbitMQ under a resource
alarm: Connection.Blocked, then heartbeats) is waited for, as pika's
heartbeat checker counts octets received; what it sent is read later
(%PENDING-INPUT). On SBCL the socket is non-blocking (OPEN-AMQP-CONNECTION),
so SB-SYS:WITH-DEADLINE bounds each chunk, plain or TLS; on Windows each
chunk first waits for the socket to be writable (%SOCKET-WRITABLE-WITHIN-P).
During the handshake, the connect timeout is the bound, whatever the
broker sends. → the number of octets."
  (let ((stream (amqp-connection-stream conn))
        (n (length octets)))
    (if (not (and (%connection-socket conn) (%write-window conn)))
        (write-amqp-octets stream octets)
        (let ((seen (and (null *read-deadline*) (%pending-input conn))))
          (flet ((what ()
                   (if *read-deadline*
                       "no AMQP handshake"
                       "the broker took no octets and sent none (two heartbeat intervals)"))
                 (broker-sent-p ()
                   ;; Did the broker send octets since we last looked?
                   (let ((now (and seen (%pending-input conn))))
                     (when (and now (> now seen))
                       (setf seen now)
                       t))))
            (loop for start from 0 below n by *write-chunk*
                  for end = (min n (+ start *write-chunk*))
                  do (let ((window (or (%write-window conn) 0)))
                       #+(and sbcl win32)
                       (loop until (%socket-writable-within-p conn window)
                             unless (broker-sent-p)
                               do (%drop-socket conn)
                                  (error 'amqp-transport-error
                                         :message (format nil "~A: ~A within ~,1F s"
                                                          (%endpoint conn) (what) window)))
                       (%call-with-deadline
                        window (%endpoint conn) (what)
                        (lambda ()
                          (handler-bind (#+sbcl
                                         (sb-sys:deadline-timeout
                                           (lambda (c)
                                             (when (broker-sent-p)
                                               (let ((defer (find-restart 'sb-sys:defer-deadline c)))
                                                 (when defer (invoke-restart defer window)))))))
                            (write-sequence octets stream :start start :end end)
                            (force-output stream)))
                        :conn conn)))
            n)))))

(defun %readable-within-p (conn seconds)
  "Does the broker on CONN's socket send something within SECONDS? Plain
TCP waits on the socket. TLS polls the TLS stream's own non-blocking
LISTEN: usocket's WAIT-FOR-INPUT calls LISTEN on the socket's stream, and
on SBCL that reads the descriptor into the socket stream's buffer, but
under TLS those octets are OpenSSL's."
  (let ((stream (amqp-connection-stream conn)))
    (cond
     ((listen stream) t)
     ((typep stream 'cl+ssl::ssl-stream)
      (loop with end = (+ (get-internal-real-time)
                          (round (* seconds internal-time-units-per-second)))
            for nap = 0.001 then (min 0.05 (* 2 nap))
            for left = (/ (- end (get-internal-real-time)) internal-time-units-per-second)
            unless (plusp left)
              return nil
            do (sleep (min nap left))
            when (listen stream)
              return t))
     (t
      (and (usocket:wait-for-input (%connection-socket conn) :timeout (float seconds)
                                                             :ready-only t)
           t)))))

(defun %wait-for-frame (conn)
  "On a live connection with heartbeats, wait until the broker sends its
next frame, sending our own heartbeats meanwhile (a reply can take longer
than the broker's patience with a silent client). Nothing at all from the
broker for two heartbeat intervals means it or the network is gone (AMQP
0-9-1 4.2.7; pika's heartbeat timeout): the socket is dropped and
AMQP-TRANSPORT-ERROR signalled instead of waiting forever. The wait ends
at *READ-DEADLINE* too (the handshake's CONNECT-TIMEOUT), with \"no AMQP
handshake\": usocket's WAIT-FOR-INPUT is a select that SBCL's deadlines do
not interrupt. Without heartbeats (:heartbeat 0) a read waits as long as it
takes (bounded by %READ-FRAME-WITHIN)."
  (let ((interval (amqp-connection-heartbeat conn)))
    (when (and (%connection-socket conn) (plusp interval))
      (let* ((silent (+ (get-internal-real-time)
                        (* 2 interval internal-time-units-per-second)))
             (limit *read-deadline*)
             (deadline (if limit (min silent limit) silent)))
        (loop
          (amqp-heartbeat-tick conn)
          (let ((left (/ (- deadline (get-internal-real-time))
                         internal-time-units-per-second)))
            (unless (plusp left)
              (%drop-socket conn)
              (error 'amqp-transport-error
                     :message (if (and limit (<= limit silent))
                                  (format nil "~A: no AMQP handshake within the connect timeout"
                                          (%endpoint conn))
                                  (format nil "~A: nothing from the broker for ~D s ~
                                               (two heartbeat intervals), so it or the network is gone"
                                          (%endpoint conn) (* 2 interval)))))
            (when (%readable-within-p conn (min left (/ interval 2)))
              (return))))))))

(defun %next-frame (conn &key poll)
  "The next frame that is not a heartbeat or a broker notification.
→ (values frame method-or-nil). A Connection.Close or Channel.Close from
the broker is answered with its Close-Ok and signalled (a Channel.Close
of a channel other than *AWAITED-CHANNEL* is recorded instead).
Connection.Blocked / Unblocked are recorded; Basic.Cancel (consumer cancel
notification) and Channel.Flow are answered and recorded. None of these is
anyone's reply, so the RPC in flight keeps waiting for its own. Nor is the
broker's Close-Ok for a Channel.Close of ours that crossed its own (see
%CS-STRAY-CLOSE-OK): it is skipped. A broker silent for two heartbeat
intervals is gone (%WAIT-FOR-FRAME), and so is one that stops for that long
in the middle of a frame (%READ-FRAME-WITHIN). With POLL, nothing is waited
for: → (values NIL NIL) when the broker has sent nothing more."
  (loop
    (when (and poll
               (not (%with-transport-errors ((%endpoint conn))
                      (%readable-within-p conn 0))))
      (return (values nil nil)))
    (let* ((interval (amqp-connection-heartbeat conn))
           (frame (%with-transport-errors ((%endpoint conn))
                    (%wait-for-frame conn)
                    (%read-frame-within conn
                                        (and (%connection-socket conn) (plusp interval)
                                             (* 2 interval))
                                        "the rest of a frame did not come (two heartbeat intervals)"))))
      (cond
        ((heartbeat-frame-p frame))
        ((/= (amqp-frame-type frame) +amqp-frame-method+)
         (return (values frame nil)))
        (t
         (let* ((method (decode-method (amqp-frame-payload frame)))
                (name (amqp-method-name method))
                (state (gethash (amqp-frame-channel frame) (%channels conn)))
                (stray (and state (%cs-stray-close-ok state)
                            (progn (setf (%cs-stray-close-ok state) nil)
                              (eq name :channel-close-ok)))))
           (unless stray
             (case name
               (:connection-close (%broker-closed-connection conn method))
               (:channel-close (%broker-closed-channel conn frame method))
               (:connection-blocked
                (setf (amqp-connection-blocked conn)
                      (or (getf (amqp-method-arguments method) :reason) t)))
               (:connection-unblocked
                (setf (amqp-connection-blocked conn) nil))
               (:basic-cancel (%broker-cancelled-consumer conn frame method))
               (:channel-flow (%broker-flow conn frame method))
               (t (return (values frame method)))))))))))

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
          :documentation "The queue of a Basic.Get-Ok.")
   (connection :initarg :connection :initform nil :reader %delivery-connection
               :documentation "The AMQP-CONNECTION it was delivered on.")
   (channel :initarg :channel :initform 1 :reader %delivery-channel
            :documentation "The channel it was delivered on: its tag is that channel's.")
   (settled :initarg :settled :initform nil :accessor %delivery-settled
            :documentation "NIL while the broker waits for its ack; :ACK or :NACK
once sent, :NO-ACK for a no-ack consumer's delivery."))
  (:documentation "A message the broker delivered: an MQ-MESSAGE that also
knows its consumer, so POLL-MESSAGES hands it out for its own queue only,
and its connection and channel, so ACK / NACK send its tag only where it is
valid. Its MQ-MESSAGE-DELIVERY-TAG is the wire tag plus its channel's offset
(%CHANNEL-STATE): the same as the wire tag until that channel closes once,
unique on the channel after that."))

(defun %delivery-queue (conn message)
  "The queue MESSAGE came from: its Basic.Get queue, or the queue its
consumer read when it was delivered (recorded on it, from SUBSCRIBE), or
failing that the queue of that consumer on its own channel. NIL when
unknown."
  (and (typep message 'amqp-delivery)
       (or (%delivery-get-queue message)
           (cdr (assoc (amqp-delivery-consumer-tag message)
                       (%consumer-queues conn (%delivery-channel message))
                       :test #'equal)))))

(defun %delivery-message (conn channel method properties body &key queue)
  "An AMQP-DELIVERY on CONN's CHANNEL for a Basic.Deliver (it names its
consumer, and the queue that consumer reads on CHANNEL is recorded on it)
or a Basic.Get-Ok (QUEUE is the queue it was got from) and its content. Its
delivery tag is the wire tag plus the channel's offset (see AMQP-DELIVERY);
unless its consumer is no-ack, the broker now waits for its ack, so the tag
is unsettled."
  (let* ((args (amqp-method-arguments method))
         (consumer (getf args :consumer-tag))
         (state (%channel-state conn channel))
         (tag (+ (%cs-offset state) (getf args :delivery-tag)))
         (no-ack (and consumer
                      (member consumer (%cs-no-ack state) :test #'equal)
                      t)))
    (setf (%cs-max-tag state) (max tag (%cs-max-tag state)))
    (unless no-ack
      (setf (gethash tag (%cs-unsettled state)) t))
    (make-instance 'amqp-delivery
                   :key (getf properties :correlation-id)
                   :payload body
                   :headers (getf properties :headers)
                   :delivery-tag tag
                   :redelivered-p (getf args :redelivered)
                   :exchange (getf args :exchange)
                   :topic (getf args :routing-key)
                   :consumer-tag consumer
                   :queue (or queue
                              (and consumer
                                   (cdr (assoc consumer (%cs-consumers state) :test #'equal))))
                   :connection conn
                   :channel channel
                   :settled (and no-ack :no-ack))))

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

(defun %incidental-frame (conn frame method)
  "Handle METHOD (read in FRAME) if it is no one's reply but a delivery: a
Basic.Deliver is kept with its content for POLL-MESSAGES, a Basic.Return
(an unroutable mandatory publish) is read and skipped. → T if it was one."
  (case (amqp-method-name method)
    (:basic-deliver
     (multiple-value-bind (properties body) (%read-content conn)
       (%enqueue-delivery conn (%delivery-message conn (amqp-frame-channel frame)
                                                  method properties body)))
     t)
    (:basic-return
     (%read-content conn)
     t)))

(defun %await (conn replies)
  "Read until a method named in REPLIES arrives (on *AWAITED-CHANNEL*, when
one is set). Deliveries and returned messages that come first are kept or
skipped. Anything else is an unexpected frame (see %UNEXPECTED-FRAME).
→ (values method frame)."
  (loop
    (multiple-value-bind (frame method) (%next-frame conn)
      (unless method
        (%unexpected-frame conn (format nil "unexpected frame type ~d while waiting for ~{~s~^ / ~}"
                                        (amqp-frame-type frame) replies)))
      (let ((name (amqp-method-name method)))
        (cond
         ((and (member name replies)
               (or (null *awaited-channel*)
                   (eql *awaited-channel* (amqp-frame-channel frame))))
          (return (values method frame)))
         ((%incidental-frame conn frame method))
         (t
          (%unexpected-frame conn (format nil "unexpected ~s on channel ~D while waiting for ~{~s~^ / ~}"
                                          name (amqp-frame-channel frame) replies))))))))

(defun %check-open (conn)
  "Signal MQ-CONNECTION-ERROR if CONN was closed (by CLOSE, by the broker,
or after an unexpected frame)."
  (when (amqp-connection-closed-p conn)
    (error 'mq-protocol:mq-connection-error
           :message (format nil "~A: the AMQP connection is closed" (%endpoint conn)))))

(defun amqp-rpc (connection name replies &rest arguments)
  "Send method NAME (ARGUMENTS: its fields, plus :CHANNEL) and wait for one
of the method names REPLIES on that channel. A broker Close of that channel
or of the connection in between is signalled (AMQP-CHANNEL-CLOSED /
AMQP-CONNECTION-CLOSED); a Channel.Close of another channel is not its
answer (%BROKER-CLOSED-CHANNEL). A broker Close of the channel read
earlier, while another channel's reply was awaited, is reported first,
once (AMQP-CHANNEL-CLOSED, nothing sent: the broker discards what goes to
a channel it closed); Channel.Open, which reopens it, only clears it. A
Channel.Close sent this way that crosses the broker's is answered by it
(AMQP-CHANNEL-CLOSED), and the broker's Close-Ok for ours, if it comes, is
skipped later (%CS-STRAY-CLOSE-OK), as pika does. Broker notifications
read on the way (AMQP-CONSUMER-CANCELLED) are signalled after the reply,
unless AMQP-RPC runs inside a backend operation, which signals them when
it is done. A non-local exit before the reply is read drops the connection
(%WITH-REPLY-GUARD). Channel.Open and Channel.Close sent this way are
tracked, so the backend knows which channels are open. → the reply
AMQP-METHOD."
  (let ((channel (or (getf arguments :channel) (amqp-connection-channel connection))))
    (%with-notifications (connection)
      (bt2:with-recursive-lock-held ((%connection-lock connection))
        (%check-open connection)
        (let* ((state (%channel-state connection channel))
               (close (shiftf (%cs-close state) nil)))
          (when (and close (not (eq name :channel-open)))
            (apply #'error 'amqp-channel-closed :message "Channel.Close" close)))
        (let ((reply (let ((*awaited-channel* channel))
                       (%with-reply-guard (connection)
                         (%with-transport-errors ((%endpoint connection))
                           (apply #'write-amqp-method connection name arguments))
                         (handler-bind ((amqp-channel-closed
                                          (lambda (c)
                                            (declare (ignore c))
                                            (when (eq name :channel-close)
                                              (setf (%cs-stray-close-ok
                                                     (%channel-state connection channel))
                                                    t)))))
                           (values (%await connection replies)))))))
          (case name
            (:channel-open (setf (%cs-open-p (%channel-state connection channel)) t))
            (:channel-close (%retire-channel connection channel)))
          reply)))))

(defun %ensure-channel (conn &optional (channel (amqp-connection-channel conn)))
  "Make CHANNEL of CONN usable. A broker Channel.Close of it that was read
while another channel's reply was awaited is reported first, once
(AMQP-CHANNEL-CLOSED: the operation does not run). A channel that is not
open (the broker closed it, or no one opened it yet) is opened."
  (let* ((state (%channel-state conn channel))
         (close (%cs-close state)))
    (when close
      (setf (%cs-close state) nil)
      (apply #'error 'amqp-channel-closed :message "Channel.Close" close))
    (unless (%cs-open-p state)
      (amqp-rpc conn :channel-open '(:channel-open-ok) :channel channel :reserved-1 "")))
  conn)

(defun %drain-incoming (conn channel &key (limit 64))
  "Read what the broker already sent on CONN, without waiting: deliveries
are kept, heartbeats and notifications handled, and a broker Close is
signalled now, a Channel.Close of CHANNEL (the operation's own) included,
rather than after more requests went to a channel the broker closed and
discards. pika's BlockingConnection reads at every call too, so a publish
after the broker closed its channel fails instead of vanishing. At most
LIMIT deliveries are read: with a consumer on a queue that keeps filling
the socket is never quiet, and the operation must still run (what is left
is read by the next one, or by an RPC on its way to its reply). A reply
here answers no request: an unexpected frame. Run at the start of every
backend operation on a live connection; the caller holds the lock."
  (let ((*awaited-channel* channel))
    (%with-reply-guard (conn)
      (loop repeat limit
            do (multiple-value-bind (frame method) (%next-frame conn :poll t)
                 (cond
                   ((null frame) (return))
                   ((null method)
                    (%unexpected-frame conn (format nil "unexpected frame type ~D, no request waits for it"
                                                    (amqp-frame-type frame))))
                   ((%incidental-frame conn frame method))
                   (t
                    (%unexpected-frame conn (format nil "unexpected ~S on channel ~D, no request waits for it"
                                                    (amqp-method-name method)
                                                    (amqp-frame-channel frame))))))))))

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
  "SASL PLAIN (RFC 4616): NUL user NUL password, UTF-8. PASSWORD may be a
function of no arguments that returns it, called only here: the frames of a
backtrace then show the function, not the password."
  (concatenate '(simple-array (unsigned-byte 8) (*))
               #(0) (%utf8 user) #(0)
               (%utf8 (if (functionp password) (funcall password) password))))

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
password are percent-decoded, so / ? # @ and % in them must be escaped; a
+ stays a + (percent-decoding only, as in pika). An IPv6 host is written
in brackets and returned without them, as pika does (amqp://[::1]:5672/ →
:host \"::1\"). URL is a string or a QURI:URI.
→ plist :host :port :user :password :vhost :tls. Errors are MQ-ERRORs that
print the URL with the password masked (amqp://user:***@host)."
  (let* ((url (cond ((stringp url) url)
                    ((quri:uri-p url) (quri:render-uri url))
                    ;; Not printed: it may hold a password.
                    (t (error 'mq-protocol:mq-error
                              :message (format nil "an AMQP URL is a string or a quri:uri, not a ~(~A~)"
                                               (type-of url))))))
         (shown (%redact-url url)))
    (flet ((refuse (what &key (hint (find #\@ url)))
             (error 'mq-protocol:mq-error
                    :message (format nil "~A: ~A~:[~; (escape % / ? # @ in the user or ~
                                          password as %25 %2F %3F %23 %40)~]"
                                     what shown hint))))
      (let* ((uri (handler-case (quri:uri url)
                    ;; Userinfo before an IPv6 literal fails here on a quri
                    ;; without cl-stack-systems#85: that is not an escaping
                    ;; problem, so no hint then.
                    (error () (refuse "malformed URL"
                                      :hint (and (find #\@ url) (not (search "@[" url)))))))
             (scheme (and (quri:uri-scheme uri) (string-downcase (quri:uri-scheme uri))))
             (host (%unbracket-host (quri:uri-host uri))))
        (unless (member scheme '("amqp" "amqps") :test #'equal)
          (refuse "not an amqp:// or amqps:// URL"))
        (unless (plusp (length (or host "")))
          (refuse "no host in URL"))
        ;; An unescaped / ? or # in the password ends quri's authority
        ;; early and puts the @host in the path, query or fragment: refuse
        ;; it rather than connect to the wrong host.
        (when (some (lambda (part) (find #\@ (or part "")))
                    (list (quri:uri-path uri) (quri:uri-query uri) (quri:uri-fragment uri)))
          (refuse "malformed URL"))
        (flet ((decode (text)
                 ;; quri:url-decode is form decoding (+ is a space); the URI
                 ;; spec and pika decode percent-escapes only, so a + is
                 ;; escaped first and comes back as a +.
                 (handler-case (quri:url-decode
                                (with-output-to-string (out)
                                  (loop for char across text
                                        do (if (char= char #\+)
                                               (write-string "%2B" out)
                                               (write-char char out)))))
                   (error () (refuse "bad percent-escape in URL")))))
          (let* ((tls (string= scheme "amqps"))
                 (userinfo (quri:uri-userinfo uri))
                 (colon (and userinfo (position #\: userinfo)))
                 (path (quri:uri-path uri)))
            (append
             (list :host host
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
                                   client-properties (connect-timeout 10) blocked-connection-timeout)
  "Connect to a broker and run the handshake. → an AMQP-CONNECTION that owns
its socket, with CHANNEL open.
HOST (\"127.0.0.1\"; an IPv6 address with or without brackets), PORT (5672,
5671 with TLS), USER / PASSWORD (\"guest\"), VHOST (\"/\"), or URL
amqp://user:pass@host:port/vhost; explicit keywords win over URL parts. TLS
through cl-stack-ssl with TLS-VERIFY (:required). HEARTBEAT: seconds, NIL
for the broker's proposal, 0 for none. CONNECT-TIMEOUT (seconds) bounds the
TCP connect, and then the TLS and AMQP handshakes together; it is not a
read timeout afterwards (heartbeats are, see %WAIT-FOR-FRAME).
BLOCKED-CONNECTION-TIMEOUT (seconds, NIL: none) bounds how long a publish
waits for the broker to lift a Connection.Blocked (AMQP-BLOCKED-TIMEOUT).
Signals AMQP-TRANSPORT-ERROR (refused, timed out, TLS, a peer that does not
speak AMQP 0-9-1) or AMQP-CONNECTION-CLOSED (bad login 403, vhost 530),
both MQ-CONNECTION-ERRORs."
  (declare (ignore host port user password vhost tls))
  (let ((from-url (and url (parse-amqp-url url))))
    (flet ((opt (key default)
             (let ((given (getf args key '%absent)))
               (if (eq given '%absent) (getf from-url key default) given))))
      (let* ((host (%unbracket-host (opt :host "127.0.0.1")))
             (tls (opt :tls nil))
             (port (opt :port (if tls 5671 5672)))
             (endpoint (%host-port host port))
             (socket (%with-transport-errors (endpoint)
                       ;; On SBCL usocket's :TIMEOUT is also the stream's read
                       ;; timeout unless :READ-TIMEOUT says otherwise.
                       (let ((socket (usocket:socket-connect host port
                                                             :element-type '(unsigned-byte 8)
                                                             :timeout connect-timeout
                                                             #+sbcl :read-timeout #+sbcl nil
                                                             :nodelay t)))
                         ;; Non-blocking, a full socket buffer makes SBCL wait
                         ;; in WAIT-UNTIL-FD-USABLE, which SB-SYS:WITH-DEADLINE
                         ;; bounds; a blocking write(2) it does not
                         ;; (%WRITE-OCTETS-WITHIN). Windows waits for
                         ;; writability instead.
                         #+(and sbcl (not win32))
                         (setf (sb-bsd-sockets:non-blocking-mode (usocket:socket socket)) t)
                         socket)))
             (conn nil)
             (done nil))
        (unwind-protect
            (%with-transport-errors (endpoint)
              ;; Two bounds, one deadline: SB-SYS:WITH-DEADLINE for the TLS
              ;; handshake and SBCL's waits, *READ-DEADLINE* for frame reads
              ;; that it does not bound (%READ-FRAME-WITHIN).
              (let ((*read-deadline* (and connect-timeout
                                          (+ (get-internal-real-time)
                                             (round (* connect-timeout
                                                       internal-time-units-per-second))))))
                (%call-with-deadline
                 connect-timeout endpoint "no AMQP handshake"
                 (lambda ()
                   (let ((stream (if tls
                                     (cl+ssl:make-ssl-client-stream (usocket:socket-stream socket)
                                                                    :hostname host
                                                                    :verify tls-verify)
                                     (usocket:socket-stream socket))))
                     (setf conn (make-instance 'amqp-connection
                                               :stream stream :socket socket
                                               :host host :port port :channel channel
                                               :blocked-timeout blocked-connection-timeout))
                     ;; A malformed frame before the handshake is done means the
                     ;; peer is no AMQP 0-9-1 broker (a TLS port spoken to in
                     ;; plain, an HTTP port): a connection error, as a refused
                     ;; connect is.
                     (handler-case
                         (%handshake conn :user (opt :user "guest") :password (opt :password "guest")
                                          :vhost (opt :vhost "/") :heartbeat heartbeat
                                          :frame-max frame-max :channel-max channel-max
                                          :locale locale :client-properties client-properties)
                       (amqp-frame-error (e)
                         (error 'amqp-transport-error
                                :message (format nil "~A: not an AMQP 0-9-1 broker (~A)"
                                                 endpoint (mq-protocol:mq-error-message e))
                                :cause e)))
                     (setf done t)
                     conn)))))
          (unless done
            (if conn
                (%drop-socket conn)
                (handler-case (usocket:socket-close socket)
                  ((or stream-error usocket:socket-error) () nil)))))))))

(defvar *close-timeout* 5
  "Seconds CLOSE waits in all for the broker's Close-Ok (%WAIT-FOR-CLOSE-OK).")

(defun %wait-for-close-ok (conn)
  "Read until Close-Ok (or the broker's own Close) after our Connection.Close.
On a socket (plain or TLS) the whole wait, heartbeats and other frames the
broker sends meanwhile included, takes at most *CLOSE-TIMEOUT* seconds;
then the close gives up (→ NIL, or AMQP-TRANSPORT-ERROR, which CLOSE
ignores)."
  (let ((deadline (+ (get-internal-real-time)
                     (round (* *close-timeout* internal-time-units-per-second)))))
    (flet ((left ()
             (max 0 (/ (- deadline (get-internal-real-time)) internal-time-units-per-second))))
      (loop
        (when (and (%connection-socket conn)
                   (or (zerop (left))
                       (not (%readable-within-p conn (left)))))
          (return nil))
        (let ((frame (%read-frame-within conn (and (%connection-socket conn) (max (left) 1/100))
                                         "no Close-Ok")))
          (when (= (amqp-frame-type frame) +amqp-frame-method+)
            (case (amqp-method-name (decode-method (amqp-frame-payload frame)))
              (:connection-close-ok (return t))
              (:connection-close
               (write-amqp-method conn :connection-close-ok :channel 0)
               (return t)))))))))

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
