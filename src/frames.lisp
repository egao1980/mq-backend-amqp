(in-package #:mq-backend-amqp)

(defstruct amqp-frame
  type
  channel
  payload)

(defun encode-frame (type channel payload)
  "AMQP frame: type (octet) + channel (short) + size (long) + payload + 0xCE."
  (let* ((payload (or payload #()))
         (size (length payload))
         (out (make-array (+ 8 size) :element-type '(unsigned-byte 8)
                          :initial-element 0)))
    (binary-protocol:pack-into out "!BHI" (list type channel size))
    (replace out payload :start1 7)
    (setf (aref out (+ 7 size)) +amqp-frame-end+)
    out))

(defun frame-size (octets &key (start 0))
  "Total encoded size of the frame starting at START, or NIL if the header
is incomplete."
  (when (>= (- (length octets) start) 7)
    (let ((size (first (binary-protocol:unpack "!I" octets :start (+ start 3)))))
      (+ 8 size))))

(defun decode-frame (octets &key (start 0))
  (when (< (- (length octets) start) 8)
    (error 'amqp-frame-error
           :offset start
           :message "truncated frame header"))
  (destructuring-bind (type channel size)
      (binary-protocol:unpack "!BHI" octets :start start)
    (let ((payload-start (+ start 7))
          (end (+ start 7 size)))
      (unless (< end (length octets))
        (error 'amqp-frame-error
               :offset start
               :message "truncated frame payload"))
      (unless (= (aref octets end) +amqp-frame-end+)
        (error 'amqp-frame-error
               :offset end
               :message (format nil "bad frame-end 0x~2,'0x (expected 0xCE)"
                                (aref octets end))))
      (make-amqp-frame :type type
                       :channel channel
                       :payload (subseq octets payload-start end)))))

(defun encode-method-frame (name &rest args &key (channel 0) &allow-other-keys)
  (let ((arguments (copy-list args)))
    (remf arguments :channel)
    (encode-frame +amqp-frame-method+ channel
                  (apply #'encode-method name arguments))))

(defun decode-method-frame (octets &key (start 0))
  (let ((frame (decode-frame octets :start start)))
    (unless (= (amqp-frame-type frame) +amqp-frame-method+)
      (error 'amqp-frame-error
             :message (format nil "expected method frame, got type ~d"
                              (amqp-frame-type frame))))
    (values (decode-method (amqp-frame-payload frame)) frame)))

(defun encode-heartbeat (&key (channel 0))
  (encode-frame +amqp-frame-heartbeat+ channel #()))

(defun heartbeat-frame-p (frame)
  (and (amqp-frame-p frame)
       (= (amqp-frame-type frame) +amqp-frame-heartbeat+)))

(defparameter +content-properties+
  '((:content-type :shortstr)
    (:content-encoding :shortstr)
    (:headers :table)
    (:delivery-mode :octet)
    (:priority :octet)
    (:correlation-id :shortstr)
    (:reply-to :shortstr)
    (:expiration :shortstr)
    (:message-id :shortstr)
    (:timestamp :timestamp)
    (:type :shortstr)
    (:user-id :shortstr)
    (:app-id :shortstr)
    (:cluster-id :shortstr)))

(defun encode-content-header (&key (class-id 60) (weight 0) (body-size 0)
                                (channel 0) properties)
  "Content header frame (type 2). PROPERTIES is a plist of basic fields."
  (let ((flags 0)
        (prop-buf (make-buffer))
        (bit 15))
    (dolist (spec +content-properties+)
      (let ((name (first spec))
            (type (second spec)))
        (when (getf properties name)
          (setf flags (logior flags (ash 1 bit)))
          (encode-typed prop-buf type (getf properties name))))
      (decf bit))
    (let ((head (binary-protocol:pack "!HHQH" class-id weight body-size flags)))
      (encode-frame +amqp-frame-header+ channel
                    (concatenate '(simple-array (unsigned-byte 8) (*))
                                 head (buffer-octets prop-buf))))))

(defun decode-content-header (octets &key (start 0))
  (let ((frame (if (amqp-frame-p octets)
                   octets
                   (decode-frame octets :start start))))
    (unless (= (amqp-frame-type frame) +amqp-frame-header+)
      (error 'amqp-frame-error
             :message (format nil "expected header frame, got type ~d"
                              (amqp-frame-type frame))))
    (let* ((payload (amqp-frame-payload frame))
           (cur (make-cursor :octets payload :pos 0)))
      (when (< (length payload) 14)
        (error 'amqp-frame-error :message "truncated content header"))
      (let ((class-id (read-uint cur 2))
            (weight (read-uint cur 2))
            (body-size (read-uint cur 8))
            (flags (read-uint cur 2))
            (properties nil)
            (bit 15))
        (dolist (spec +content-properties+)
          (when (logbitp bit flags)
            (setf properties
                  (list* (first spec)
                         (decode-typed cur (second spec))
                         properties)))
          (decf bit))
        (list :class-id class-id
              :weight weight
              :body-size body-size
              :properties properties
              :channel (amqp-frame-channel frame))))))

(defun encode-body-frame (payload &key (channel 0))
  (encode-frame +amqp-frame-body+ channel
                (coerce payload '(simple-array (unsigned-byte 8) (*)))))
