(in-package #:mq-backend-amqp/tests)

;;; Round 3 (#2, #7): broker notifications are signalled when the operation
;;; has its reply, so a handler can unwind without leaving a reply unread;
;;; any other non-local exit mid-reply drops the connection; deliveries are
;;; handed out by polls of their own queue; URL masking to the last @.

(define-condition test-interrupt (condition) ()
  (:documentation "Signalled by INTERRUPTING-SOURCE in the middle of a read."))

(defclass interrupting-source ()
  ((inner :initarg :inner :reader %inner)
   (out :initarg :out :reader %out)
   (at :initarg :at :reader %at :documentation "Signal before this read (1-based).")
   (reads :initform 0 :accessor %reads))
  (:documentation "A connection stream that SIGNALs TEST-INTERRUPT before its
AT-th read, the way an interrupt or a timer handler can fire mid-reply."))

(defmethod mq-backend-amqp:read-amqp-octets ((source interrupting-source) n)
  (when (= (incf (%reads source)) (%at source))
    (signal 'test-interrupt))
  (mq-backend-amqp:read-amqp-octets (%inner source) n))

(defmethod mq-backend-amqp:write-amqp-octets ((source interrupting-source) octets)
  (mq-backend-amqp:write-amqp-octets (%out source) octets))

(deftest a-reply-left-unread-closes-the-connection
  ;; The reply to the first Queue.Declare is interrupted after its frame
  ;; header was read (read 2 of 2). The next Queue.Declare must not read
  ;; what is left of it, or the reply after it.
  (let* ((in (flexi-streams:make-in-memory-input-stream
              (%frames (%declare-ok "first") (%declare-ok "second"))))
         (out (flexi-streams:make-in-memory-output-stream))
         (conn (mq-backend-amqp:make-amqp-connection
                :stream (make-instance 'interrupting-source :inner in :out out :at 2)))
         (b (mq-backend-amqp:make-amqp-backend :connection conn)))
    (ok (eq :interrupted
            (handler-case (mq-backend-amqp:amqp-declare-queue b "first")
              (test-interrupt () :interrupted)))
        "the interrupt unwinds out of the first Queue.Declare")
    (ok (mq-backend-amqp:amqp-connection-closed-p conn)
        "the connection is closed: part of a reply is unread")
    (ok (eq :connection-error
            (handler-case (mq-backend-amqp:amqp-declare-queue b "second")
              (mq-protocol:mq-connection-error () :connection-error)))
        "the next Queue.Declare fails cleanly instead of reading a stale reply")))

(deftest a-channel-close-keeps-the-connection-in-step
  ;; Channel.Close is a complete answer: the connection stays usable.
  (let* ((conn (%scripted (%frames '(:channel-close 1 :reply-code 404
                                     :reply-text "NOT_FOUND" :class-id 50 :method-id 10)
                                   '(:channel-open-ok 1 :reserved-1 "")
                                   (%declare-ok "second"))))
         (b (mq-backend-amqp:make-amqp-backend :connection conn)))
    (ok (signals (mq-backend-amqp:amqp-declare-queue b "first")
                 'mq-backend-amqp:amqp-channel-closed))
    (ok (not (mq-backend-amqp:amqp-connection-closed-p conn)) "the connection is still open")
    (mq-backend-amqp::%ensure-channel conn)
    (ok (equal "second" (mq-backend-amqp:amqp-declare-queue b "second"))
        "the next Queue.Declare gets its own reply")))

(deftest consumer-cancelled-is-signalled-after-the-reply
  ;; The notification comes after the reply was read, with the connection
  ;; lock free: a handler may use the connection.
  (let* ((conn (%scripted (%frames '(:basic-cancel 1 :consumer-tag "ctag" :no-wait t)
                                   (%declare-ok "first")
                                   (%declare-ok "from-handler"))))
         (b (mq-backend-amqp:make-amqp-backend :connection conn))
         (from-handler nil))
    (ok (equal "first"
               (handler-bind ((mq-backend-amqp:amqp-consumer-cancelled
                                (lambda (c)
                                  (declare (ignore c))
                                  (setf from-handler
                                        (mq-backend-amqp:amqp-declare-queue b "from-handler")))))
                 (mq-backend-amqp:amqp-declare-queue b "first")))
        "the first Queue.Declare returns its own reply")
    (ok (equal "from-handler" from-handler)
        "a handler can run an RPC on the same connection")))

(deftest pending-deliveries-are-handed-out-per-queue
  (let* ((conn (mq-backend-amqp:make-amqp-connection :stream nil))
         (make (lambda (tag text &optional queue)
                 (make-instance 'mq-backend-amqp:amqp-delivery
                                :payload text :consumer-tag tag :queue queue))))
    (setf (mq-backend-amqp::%consumer-queues conn) '(("ca" . "qa") ("cb" . "qb")))
    (setf (mq-backend-amqp::%pending-deliveries conn)
          (list (funcall make "ca" "a1") (funcall make "cb" "b1")
                (funcall make nil "b2" "qb") (funcall make "ca" "a2")))
    (flet ((take (queue n)
             (mapcar #'mq-protocol:mq-message-payload
                     (mq-backend-amqp::%take-pending conn queue n))))
      (ok (equal '("b1" "b2") (take "qb" 5)) "qb gets its own, in order")
      (ok (equal '("a1") (take "qa" 1)) "qa gets its own, up to N")
      (ok (equal '("a2") (take "qa" 5)) "the rest of qa stays for the next poll")
      (ok (null (take "qc" 5)) "an unknown queue gets nothing"))))

(deftest no-wait-subscribe-picks-a-tag
  (multiple-value-bind (conn out) (%scripted (%frames))
    (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
           (sub (mq-protocol:subscribe b "qa" (lambda (m) (declare (ignore m))) :no-wait t))
           (tag (mq-protocol:mq-subscription-tag sub)))
      (ok (and (stringp tag) (plusp (length tag))) (format nil "a client tag: ~S" tag))
      (ok (equal tag (getf (%args (%written out) :basic-consume) :consumer-tag))
          "Basic.Consume carries it")
      (ok (equal (cons tag "qa") (assoc tag (mq-backend-amqp::%consumer-queues conn)
                                        :test #'equal))
          "its queue is recorded"))))

(deftest live-an-unwound-poll-keeps-its-messages
  ;; A handler-case unwinds a poll of QB on the broker's cancel; QB's
  ;; message is not lost: the next poll of QB returns it.
  (with-broker ()
    (let ((b (%live))
          (admin (%live))
          (qa (%queue-name "r3-cancel-a"))
          (qb (%queue-name "r3-cancel-b")))
      (unwind-protect
          (progn
            (mq-backend-amqp:amqp-declare-queue b qa :auto-delete nil)
            (mq-backend-amqp:amqp-declare-queue b qb :exclusive t)
            (mq-protocol:subscribe b qa (lambda (m) (declare (ignore m))))
            (mq-protocol:publish b qb (mq-protocol:make-mq-message :payload "for-b"))
            (mq-backend-amqp:amqp-rpc (mq-backend-amqp:amqp-backend-connection admin)
                                      :queue-delete '(:queue-delete-ok)
                                      :channel 1 :reserved-1 0 :queue qa
                                      :if-unused nil :if-empty nil :no-wait nil)
            (sleep 0.3)
            (ok (eq :cancelled
                    (handler-case (mq-protocol:poll-messages b qb :timeout 1)
                      (mq-backend-amqp:amqp-consumer-cancelled () :cancelled))))
            (let ((got (mq-protocol:poll-messages b qb :timeout 1)))
              (ok (equal '("for-b") (mapcar #'%text got))
                  (format nil "the next poll of QB returns its message (got ~S)"
                          (mapcar #'%text got)))
              (ok (not (mq-protocol:mq-message-redelivered-p (first got)))
                  "kept by the client, not redelivered")
              (when got (mq-protocol:ack b (first got)))))
        (ignore-errors (close admin))
        (ignore-errors (close b))))))

(deftest-parametrize percent-encoded-amqp-passwords-parse
    ((url password)
     ("amqp://app:s3cret%2Fx%2By%3D%3D@rabbit.internal/prod" "s3cret/x+y==")
     ("amqp://app:p%40ss%23%3F%25@rabbit.internal/prod" "p@ss#?%")
     ("amqp://app:s3cret+/=@rabbit.internal/prod" nil)
     ("amqp://app:s3cret%zz@rabbit.internal/prod" nil))
  (let ((got (handler-case (mq-backend-amqp:parse-amqp-url url)
               (mq-protocol:mq-error (e)
                 (ok (not (search "s3cret" (princ-to-string e))) "the report masks it")
                 :refused))))
    (if password
        (progn
          (ok (equal password (getf got :password)) (format nil "~A has password ~S" url password))
          (ok (equal "rabbit.internal" (getf got :host)))
          (ok (equal "prod" (getf got :vhost))))
        (ok (eq :refused got) (format nil "~A is refused, not misrouted (got ~S)" url got)))))
