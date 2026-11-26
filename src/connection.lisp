(in-package #:mq-backend-amqp)

;;; Bidirectional octet sink. Accepts a CL binary stream or OCTET-PIPE.

(defclass octet-pipe ()
  ((buffer :initform (make-array 0 :element-type '(unsigned-byte 8)
                                 :adjustable t :fill-pointer 0)
           :accessor octet-pipe-buffer)
   (read-pos :initform 0 :accessor octet-pipe-read-pos))
  (:documentation "In-memory bidirectional octet buffer for tests."))

(defun make-octet-pipe ()
  (make-instance 'octet-pipe))

(defun octet-pipe-clear (pipe)
  (setf (fill-pointer (octet-pipe-buffer pipe)) 0
        (octet-pipe-read-pos pipe) 0)
  pipe)

(defgeneric write-amqp-octets (sink octets)
  (:method ((sink stream) octets)
    (write-sequence octets sink)
    (force-output sink)
    (length octets))
  (:method ((sink octet-pipe) octets)
    (let ((buf (octet-pipe-buffer sink)))
      (loop for i from 0 below (length octets)
            do (vector-push-extend (aref octets i) buf))
      (length octets))))

(defgeneric read-amqp-octets (source n)
  (:documentation "N octets from SOURCE. The end of a stream before them is an
AMQP-TRANSPORT-ERROR (an MQ-CONNECTION-ERROR): the peer went away.")
  (:method ((source stream) n)
    (let ((buf (make-array n :element-type '(unsigned-byte 8) :initial-element 0)))
      (let ((got (read-sequence buf source)))
        (unless (= got n)
          (error 'amqp-transport-error
                 :message (format nil "unexpected EOF (got ~d of ~d)" got n)))
        buf)))
  (:method ((source octet-pipe) n)
    (let* ((buf (octet-pipe-buffer source))
           (pos (octet-pipe-read-pos source))
           (avail (- (length buf) pos)))
      (unless (>= avail n)
        (error 'mq-protocol:mq-connection-error
               :message (format nil "pipe underrun (need ~d, have ~d)" n avail)))
      (prog1 (subseq buf pos (+ pos n))
        (incf (octet-pipe-read-pos source) n)))))

(defstruct (%channel-state (:constructor %make-channel-state (&key open-p))
                           (:conc-name %cs-))
  "What the client knows of one channel of a connection. Delivery tags are
per channel, and a reopened channel numbers its deliveries from 1 again: the
tags handed out are the wire tag plus OFFSET, the highest tag handed out
before the channel last closed, so a tag at or below OFFSET belongs to a
closed channel and is never sent (AMQP-STALE-DELIVERY). UNSETTLED holds
the tags (offset included) the broker still waits to have acked; MAX-TAG
is the highest one handed out. CLOSE is a broker Channel.Close read while
another channel's reply was awaited, as (reply-code reply-text class-id
method-id), until an operation on this channel reports it. Consumer tags
are channel-local too: CONSUMERS is (consumer-tag . queue) for each live
consumer on the channel, NO-ACK the tags of those that are no-ack; a
consumer leaves both when the broker cancels it or the channel closes.
STRAY-CLOSE-OK: our Channel.Close crossed the broker's, which answered it;
the broker's Close-Ok for ours may still come and is no one's reply, so the
next method on the channel is skipped if it is a Close-Ok (%NEXT-FRAME)."
  (open-p nil)
  (offset 0 :type integer)
  (max-tag 0 :type integer)
  (unsettled (make-hash-table) :read-only t)
  (close nil)
  (consumers '())
  (no-ack '())
  (stray-close-ok nil))

(defclass amqp-connection ()
  ((stream :initarg :stream :accessor amqp-connection-stream)
   (channel :initarg :channel :accessor amqp-connection-channel :initform 1)
   (frame-max :initarg :frame-max :accessor amqp-connection-frame-max
              :initform 131072)
   (channel-max :initarg :channel-max :accessor amqp-connection-channel-max
                :initform 0)
   (heartbeat :initarg :heartbeat :accessor amqp-connection-heartbeat :initform 0
              :documentation "Negotiated heartbeat interval in seconds (0: off).")
   (last-sent :initform (get-internal-real-time) :accessor %last-sent)
   (server-properties :initform nil :accessor amqp-connection-server-properties)
   (socket :initarg :socket :initform nil :accessor %connection-socket
           :documentation "The usocket socket when the connection owns one.")
   (host :initarg :host :initform nil :reader amqp-connection-host)
   (port :initarg :port :initform nil :reader amqp-connection-port)
   (channels :initform (make-hash-table) :reader %channels
             :documentation "Channel number → %CHANNEL-STATE. The default channel
starts open.")
   (closed-p :initform nil :accessor amqp-connection-closed-p)
   (blocked :initform nil :accessor amqp-connection-blocked
            :documentation "The broker's Connection.Blocked reason while it holds publishers.")
   (cancelled :initform '() :accessor amqp-connection-cancelled-consumers
              :documentation "Consumer tags the broker cancelled with Basic.Cancel
(their queue was deleted, or its node failed), newest first.")
   (flow-active :initform t :accessor amqp-connection-flow-active
                :documentation "NIL after a broker Channel.Flow active=false, until it
sends active=true again.")
   (pending :initform '() :accessor %pending-deliveries
            :documentation "Delivered messages not handed out yet, oldest first:
Basic.Deliver frames that arrived while waiting for a reply, and messages a
POLL-MESSAGES fetched but could not hand out. Each is handed out by a poll
of its own queue and channel (%DELIVERY-QUEUE).")
   (pending-end :initform nil :accessor %pending-end
                :documentation "(head . last-cons) of the pending list when a delivery was
last added, so the next one is added in constant time (%ENQUEUE-DELIVERY).")
   (handing-out :initform '() :accessor %handing-out
                :documentation "Messages a POLL-MESSAGES fetched and has not returned
yet (it signals broker notifications first): not handed out either.")
   (consumer-count :initform 0 :accessor %consumer-count
                   :documentation "Client-picked consumer tags made so far (cl-stack.ctag.N), so none is reused.")
   (notifications :initform '() :accessor %pending-notifications
                  :documentation "Broker notifications to signal, oldest first, as
(condition-type . initargs): see %SIGNAL-NOTIFICATIONS.")
   (lock :initform (bt2:make-recursive-lock :name "amqp-connection") :reader %connection-lock))
  (:documentation
   "AMQP 0-9-1 connection over a bidirectional octet STREAM (or OCTET-PIPE).
OPEN-AMQP-CONNECTION makes one that owns its socket, after the handshake."))

(defun make-amqp-connection (&key stream (channel 1) (frame-max 131072))
  (make-instance 'amqp-connection
                 :stream stream
                 :channel channel
                 :frame-max frame-max))

(defmethod initialize-instance :after ((conn amqp-connection) &key)
  (setf (gethash (amqp-connection-channel conn) (%channels conn))
        (%make-channel-state :open-p t)))

(defun %channel-state (conn &optional (channel (amqp-connection-channel conn)))
  "CONN's %CHANNEL-STATE for CHANNEL (made on first use, closed)."
  (or (gethash channel (%channels conn))
      (setf (gethash channel (%channels conn)) (%make-channel-state))))

(defun %channel-open-p (conn &optional (channel (amqp-connection-channel conn)))
  (%cs-open-p (%channel-state conn channel)))

(defun %consumer-queues (conn &optional (channel (amqp-connection-channel conn)))
  "(consumer-tag . queue) for each live consumer on CONN's CHANNEL."
  (%cs-consumers (%channel-state conn channel)))

(defun (setf %consumer-queues) (value conn &optional (channel (amqp-connection-channel conn)))
  (setf (%cs-consumers (%channel-state conn channel)) value))

(defun %no-ack-consumers (conn &optional (channel (amqp-connection-channel conn)))
  "Tags of the no-ack consumers on CONN's CHANNEL: their deliveries are
settled on arrival and cannot be acked."
  (%cs-no-ack (%channel-state conn channel)))

(defun (setf %no-ack-consumers) (value conn &optional (channel (amqp-connection-channel conn)))
  (setf (%cs-no-ack (%channel-state conn channel)) value))

(defun %forget-consumer (conn channel tag)
  "The consumer TAG on CONN's CHANNEL is gone (the broker cancelled it):
its tag may be used again for another queue."
  (let ((state (%channel-state conn channel)))
    (setf (%cs-consumers state) (remove tag (%cs-consumers state) :key #'car :test #'equal)
          (%cs-no-ack state) (remove tag (%cs-no-ack state) :test #'equal))))

(defun %enqueue-delivery (conn message)
  "Add MESSAGE at the end of CONN's pending deliveries in constant time: the
last cons is kept while the list is the one it was taken from (anything
that replaces the list makes the next add find its end once)."
  (let* ((cell (list message))
         (head (%pending-deliveries conn))
         (end (%pending-end conn)))
    (if (null head)
        (setf (%pending-deliveries conn) cell)
        (setf (cdr (if (and end (eq (car end) head) (null (cddr end)))
                       (cdr end)
                       (last head)))
              cell))
    (setf (%pending-end conn) (cons (%pending-deliveries conn) cell))
    message))

(defun write-amqp-frame (connection frame-or-octets)
  "Write a frame (or its encoded octets) to CONNECTION. On a live socket the
write is bounded like a read: a broker that takes nothing for two heartbeat
intervals is gone (%WRITE-OCTETS-WITHIN)."
  (let ((octets (if (amqp-frame-p frame-or-octets)
                    (encode-frame (amqp-frame-type frame-or-octets)
                                  (amqp-frame-channel frame-or-octets)
                                  (amqp-frame-payload frame-or-octets))
                    frame-or-octets)))
    (%write-octets-within connection octets)
    (setf (%last-sent connection) (get-internal-real-time))
    octets))

(defun read-amqp-frame (connection &optional (source (amqp-connection-stream connection)))
  "The next frame, read from SOURCE (the connection's stream, or anything
with a READ-AMQP-OCTETS method). One larger than the connection's
frame-max (the client's before Tune, the negotiated one after; 0: no
limit) is an AMQP-FRAME-ERROR before its payload is read: no peer may send
one, and a non-AMQP peer (an HTTP port) would otherwise make it allocate
whatever its first octets say."
  (let* ((header (read-amqp-octets source 7))
         (size (first (binary-protocol:unpack "!I" header :start 3)))
         (frame-max (amqp-connection-frame-max connection)))
    (when (and (plusp frame-max) (> (+ size 8) frame-max))
      (error 'amqp-frame-error
             :offset 3
             :message (format nil "a frame of ~D octets is over the frame-max ~D (not an AMQP 0-9-1 peer?)"
                              (+ size 8) frame-max)))
    (let* ((rest (read-amqp-octets source (1+ size)))
           (octets (concatenate '(simple-array (unsigned-byte 8) (*)) header rest)))
      (decode-frame octets))))

(defun write-amqp-method (connection name &rest arguments)
  (let* ((channel (or (getf arguments :channel)
                      (amqp-connection-channel connection)))
         (args (copy-list arguments)))
    (remf args :channel)
    (write-amqp-frame connection (apply #'encode-method-frame name
                                        :channel channel args))))

(defun read-amqp-method (connection)
  (let ((frame (read-amqp-frame connection)))
    (unless (= (amqp-frame-type frame) +amqp-frame-method+)
      (error 'amqp-frame-error
             :message (format nil "expected method, got frame type ~d"
                              (amqp-frame-type frame))))
    (values (decode-method (amqp-frame-payload frame)) frame)))
