(in-package #:mq-backend-amqp)

(defconstant +amqp-frame-method+ 1)
(defconstant +amqp-frame-header+ 2)
(defconstant +amqp-frame-body+ 3)
(defconstant +amqp-frame-heartbeat+ 8)
(defconstant +amqp-frame-end+ #xCE)

(defparameter +amqp-protocol-header+
  (coerce #(65 77 81 80 0 0 9 1) '(simple-array (unsigned-byte 8) (*)))
  "AMQP 0-9-1 protocol header (\"AMQP\" 0 0 9 1).")

(define-condition amqp-frame-error (mq-protocol:mq-error)
  ((offset :initarg :offset :reader amqp-frame-error-offset :initform nil))
  (:report (lambda (c s)
             (format s "amqp frame error~@[: ~a~]"
                     (mq-protocol:mq-error-message c)))))

(defun make-buffer ()
  (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0))

(defun buffer-octets (buf)
  (coerce buf '(simple-array (unsigned-byte 8) (*))))

(defun push-byte (buf b)
  (vector-push-extend b buf)
  buf)

(defun push-octets (buf octets)
  (loop for i from 0 below (length octets)
        do (vector-push-extend (aref octets i) buf))
  buf)

(defun push-uint (buf width value)
  (let ((fmt (ecase width
               (1 "!B")
               (2 "!H")
               (4 "!I")
               (8 "!Q"))))
    (push-octets buf (binary-protocol:pack fmt value))))

(defstruct cursor
  octets
  (pos 0))

(defun cursor-remaining (cur)
  (- (length (cursor-octets cur)) (cursor-pos cur)))

(defun read-uint (cur width)
  (when (< (cursor-remaining cur) width)
    (error 'amqp-frame-error
           :offset (cursor-pos cur)
           :message "truncated integer field"))
  (let* ((fmt (ecase width
                (1 "!B")
                (2 "!H")
                (4 "!I")
                (8 "!Q")))
         (vals (binary-protocol:unpack fmt (cursor-octets cur)
                                       :start (cursor-pos cur))))
    (incf (cursor-pos cur) width)
    (first vals)))

(defun read-bytes (cur n)
  (when (< (cursor-remaining cur) n)
    (error 'amqp-frame-error
           :offset (cursor-pos cur)
           :message "truncated octet field"))
  (let ((start (cursor-pos cur)))
    (incf (cursor-pos cur) n)
    (subseq (cursor-octets cur) start (+ start n))))

(defun read-float (cur width)
  "An IEEE 754 big-endian float of WIDTH octets: 4 single, 8 double."
  (when (< (cursor-remaining cur) width)
    (error 'amqp-frame-error
           :offset (cursor-pos cur)
           :message "truncated float field"))
  (prog1 (first (binary-protocol:unpack (if (= width 4) "!f" "!d") (cursor-octets cur)
                                        :start (cursor-pos cur)))
    (incf (cursor-pos cur) width)))

(defun %decimal-parts (ratio)
  "→ (values scale unscaled) with RATIO = UNSCALED / 10^SCALE, the form a
field-table decimal holds (scale octet, signed 32-bit value); NIL when
RATIO has none."
  (loop for scale from 0 to 255
        for unscaled = (* ratio (expt 10 scale))
        when (integerp unscaled)
          return (and (<= -2147483648 unscaled 2147483647)
                      (values scale unscaled))))

(defun string-octets (string)
  "The octets of an AMQP short or long string: a string in UTF-8 (AMQP 0-9-1
strings are UTF-8, so a queue named café goes out as 63 61 66 C3 A9, as pika
sends it), or octets as they are."
  (cond
    ((null string)
     (make-array 0 :element-type '(unsigned-byte 8)))
    ((stringp string)
     (handler-case (coerce (encoding-protocol:encode string :encoding :utf-8)
                           '(simple-array (unsigned-byte 8) (*)))
       (encoding-protocol:encoding-error (e)
         (error 'amqp-frame-error
                :message (format nil "~S is not encodable as UTF-8: ~A" string e)))))
    ((and (vectorp string)
          (every (lambda (b) (typep b '(unsigned-byte 8))) string))
     (coerce string '(simple-array (unsigned-byte 8) (*))))
    (t
     (error 'amqp-frame-error
            :message (format nil "expected string/octets, got ~s" string)))))

(defun octets-string (octets)
  "An AMQP short or long string read as UTF-8. Octets that are not UTF-8 (a
long string can carry anything) are read one character per octet instead,
which loses nothing, rather than failing the whole frame."
  (handler-case (encoding-protocol:decode (coerce octets '(simple-array (unsigned-byte 8) (*)))
                                          :encoding :utf-8 :strict t)
    (encoding-protocol:encoding-error ()
      (map 'string #'code-char octets))))

(defun encode-shortstr (buf value)
  (let ((octets (string-octets (or value ""))))
    (when (> (length octets) 255)
      (error 'amqp-frame-error :message "shortstr longer than 255"))
    (push-byte buf (length octets))
    (push-octets buf octets)))

(defun decode-shortstr (cur)
  (octets-string (read-bytes cur (read-uint cur 1))))

(defun encode-longstr (buf value)
  (let ((octets (string-octets (or value #()))))
    (push-uint buf 4 (length octets))
    (push-octets buf octets)))

(defun decode-longstr (cur &key as-octets)
  (let ((octets (read-bytes cur (read-uint cur 4))))
    (if as-octets octets (octets-string octets))))

(defun table-alist (value)
  (cond
    ((null value) nil)
    ((hash-table-p value)
     (let ((out nil))
       (maphash (lambda (k v) (push (cons (string k) v) out)) value)
       (nreverse out)))
    ((and (consp value) (keywordp (car value)))
     (loop for (k v) on value by #'cddr
           collect (cons (string-downcase (symbol-name k)) v)))
    ((and (consp value) (consp (car value)))
     value)
    (t
     (error 'amqp-frame-error
            :message (format nil "expected field table, got ~s" value)))))

(defun encode-field-value (buf value)
  "VALUE's type octet and encoding: :VOID V, T / NIL t, an integer I (32-bit)
or l (64-bit), a single-float f, a double-float d, a ratio D when it is a
decimal (scale ≤ 255, signed 32-bit unscaled value), a string S, an
(unsigned-byte 8) vector x, any other vector A (a field array, as the
decoder returns one: an x-death header republished as it came), an alist /
plist / hash table F."
  (cond
    ((eq value :void)
     (push-byte buf (char-code #\V)))
    ((eq value t)
     (push-byte buf (char-code #\t))
     (push-byte buf 1))
    ((eq value nil)
     (push-byte buf (char-code #\t))
     (push-byte buf 0))
    ((typep value '(signed-byte 32))
     (push-byte buf (char-code #\I))
     (push-uint buf 4 (ldb (byte 32 0) value)))
    ((typep value '(signed-byte 64))
     (push-byte buf (char-code #\l))
     (push-uint buf 8 (ldb (byte 64 0) value)))
    ((typep value 'single-float)
     (push-byte buf (char-code #\f))
     (push-octets buf (binary-protocol:pack "!f" value)))
    ((floatp value)
     (push-byte buf (char-code #\d))
     (push-octets buf (binary-protocol:pack "!d" (float value 1d0))))
    ((and (typep value 'ratio) (%decimal-parts value))
     (multiple-value-bind (scale unscaled) (%decimal-parts value)
       (push-byte buf (char-code #\D))
       (push-byte buf scale)
       (push-uint buf 4 (ldb (byte 32 0) unscaled))))
    ((stringp value)
     (push-byte buf (char-code #\S))
     (encode-longstr buf value))
    ((typep value '(vector (unsigned-byte 8)))
     (push-byte buf (char-code #\x))
     (push-uint buf 4 (length value))
     (push-octets buf value))
    ((vectorp value)
     (let ((inner (make-buffer)))
       (loop for item across value
             do (encode-field-value inner item))
       (push-byte buf (char-code #\A))
       (push-uint buf 4 (length inner))
       (push-octets buf inner)))
    ((or (consp value) (hash-table-p value))
     (push-byte buf (char-code #\F))
     (encode-table buf value))
    (t
     (error 'amqp-frame-error
            :message (format nil "unsupported table value ~s" value)))))

(defun encode-table (buf value)
  (let ((inner (make-buffer)))
    (dolist (pair (table-alist value))
      (encode-shortstr inner (car pair))
      (encode-field-value inner (cdr pair)))
    (push-uint buf 4 (length inner))
    (push-octets buf inner)))

(defun decode-field-value (cur)
  "One field-table value: its type octet, then the value (AMQP 0-9-1 with
RabbitMQ's types: t b B s u I i l L f d D S x T F V A). An unknown type
octet is an AMQP-FRAME-ERROR at its offset."
  (let* ((at (cursor-pos cur))
         (code (code-char (read-uint cur 1))))
    (case code
      (#\t (plusp (read-uint cur 1)))
      (#\b (let ((v (read-uint cur 1)))
             (if (logbitp 7 v) (- v 256) v)))
      (#\B (read-uint cur 1))
      (#\s (let ((v (read-uint cur 2)))
             (if (logbitp 15 v) (- v 65536) v)))
      (#\u (read-uint cur 2))
      (#\I (let ((v (read-uint cur 4)))
             (if (logbitp 31 v) (- v (ash 1 32)) v)))
      (#\i (read-uint cur 4))
      (#\l (let ((v (read-uint cur 8)))
             (if (logbitp 63 v) (- v (ash 1 64)) v)))
      (#\L (read-uint cur 8))
      (#\f (read-float cur 4))
      (#\d (read-float cur 8))
      (#\D (let* ((scale (read-uint cur 1))
                  (v (read-uint cur 4)))
             ;; RabbitMQ and pika read the unscaled value as signed.
             (/ (if (logbitp 31 v) (- v (ash 1 32)) v) (expt 10 scale))))
      (#\S (decode-longstr cur))
      (#\x (read-bytes cur (read-uint cur 4)))
      (#\T (read-uint cur 8))
      (#\F (decode-table cur))
      (#\V :void)
      (#\A
       (let* ((n (read-uint cur 4))
              (end (+ (cursor-pos cur) n))
              (out '()))
         (loop while (< (cursor-pos cur) end)
               do (push (decode-field-value cur) out))
         (coerce (nreverse out) 'simple-vector)))
      (t
       (error 'amqp-frame-error
              :offset at
              :message (format nil "unknown field-table value type ~S (octet ~D) at offset ~D"
                               code (char-code code) at))))))

(defun decode-table (cur)
  (let* ((n (read-uint cur 4))
         (end (+ (cursor-pos cur) n))
         (out '()))
    (loop while (< (cursor-pos cur) end)
          do (let ((name (decode-shortstr cur))
                   (value (decode-field-value cur)))
               (push (cons name value) out)))
    (nreverse out)))

(defun encode-typed (buf type value)
  (ecase type
    (:octet (push-uint buf 1 (or value 0)))
    (:short (push-uint buf 2 (or value 0)))
    (:long (push-uint buf 4 (or value 0)))
    (:longlong (push-uint buf 8 (or value 0)))
    (:timestamp (push-uint buf 8 (or value 0)))
    (:shortstr (encode-shortstr buf value))
    (:longstr (encode-longstr buf value))
    (:table (encode-table buf value))))

(defun decode-typed (cur type)
  (ecase type
    (:octet (read-uint cur 1))
    (:short (read-uint cur 2))
    (:long (read-uint cur 4))
    (:longlong (read-uint cur 8))
    (:timestamp (read-uint cur 8))
    (:shortstr (decode-shortstr cur))
    (:longstr (decode-longstr cur))
    (:table (decode-table cur))))

(defun encode-fields (fields values)
  "FIELDS is ((:name :type) ...). VALUES is a plist. :bit fields pack LSB-first."
  (let ((buf (make-buffer))
        (bit-acc 0)
        (bit-n 0)
        (have-bits nil))
    (flet ((flush-bits ()
             (when have-bits
               (push-byte buf bit-acc)
               (setf bit-acc 0 bit-n 0 have-bits nil))))
      (dolist (field fields)
        (destructuring-bind (name type) field
          (if (eq type :bit)
              (progn
                (setf have-bits t)
                (when (getf values name)
                  (setf bit-acc (logior bit-acc (ash 1 bit-n))))
                (incf bit-n)
                (when (= bit-n 8)
                  (flush-bits)))
              (progn
                (flush-bits)
                (encode-typed buf type (getf values name))))))
      (flush-bits)
      (buffer-octets buf))))

(defun decode-fields (fields cur)
  (let ((out nil)
        (bit-acc 0)
        (bit-n 8)
        (have-bits nil))
    (dolist (field fields)
      (destructuring-bind (name type) field
        (if (eq type :bit)
            (progn
              (when (or (not have-bits) (>= bit-n 8))
                (setf bit-acc (read-uint cur 1)
                      bit-n 0
                      have-bits t))
              (setf out (list* name (logbitp bit-n bit-acc) out))
              (incf bit-n))
            (progn
              (setf have-bits nil bit-n 8)
              (setf out (list* name (decode-typed cur type) out))))))
    out))
