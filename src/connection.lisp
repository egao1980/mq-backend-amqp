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
  (:method ((source stream) n)
    (let ((buf (make-array n :element-type '(unsigned-byte 8) :initial-element 0)))
      (let ((got (read-sequence buf source)))
        (unless (= got n)
          (error 'mq-protocol:mq-connection-error
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
   (channel-open-p :initform t :accessor %channel-open-p)
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
of its own queue (%DELIVERY-QUEUE).")
   (consumer-queues :initform '() :accessor %consumer-queues
                    :documentation "(consumer-tag . queue) for each SUBSCRIBE on this connection.")
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

(defun write-amqp-frame (connection frame-or-octets)
  (let ((octets (if (amqp-frame-p frame-or-octets)
                    (encode-frame (amqp-frame-type frame-or-octets)
                                  (amqp-frame-channel frame-or-octets)
                                  (amqp-frame-payload frame-or-octets))
                    frame-or-octets)))
    (write-amqp-octets (amqp-connection-stream connection) octets)
    (setf (%last-sent connection) (get-internal-real-time))
    octets))

(defun read-amqp-frame (connection)
  (let* ((header (read-amqp-octets (amqp-connection-stream connection) 7))
         (size (first (binary-protocol:unpack "!I" header :start 3)))
         (rest (read-amqp-octets (amqp-connection-stream connection) (1+ size)))
         (octets (concatenate '(simple-array (unsigned-byte 8) (*)) header rest)))
    (decode-frame octets)))

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
