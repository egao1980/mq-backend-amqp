(in-package #:mq-backend-amqp/tests)

;;; Round-2 evidence against PR #4.
;;;
;;; 1. AMQP-CONSUMER-CANCELLED is SIGNALled from inside %NEXT-FRAME, in the
;;;    middle of the RPC in flight. A HANDLER-CASE for it, the usual way
;;;    to react to a condition, unwinds out of that RPC and leaves its
;;;    reply unread in the socket. The next RPC reads it as its own: the
;;;    silent desync this PR set out to remove ("later calls then fail with
;;;    mq-connection-error instead of reading the previous call's reply").
;;;
;;; 2. POLL-MESSAGES "Up to N messages from queue DESTINATION" hands out
;;;    %PENDING-DELIVERIES first, whatever queue their consumer reads: a
;;;    poll of queue B returns a message of queue A's consumer.
;;;
;;; 3. #7 (fixed on this PR): %REDACT-URL ends the authority at the first
;;;    / ? or #, so a password holding one (base64 passwords hold /) is
;;;    printed in "malformed URL: ...".

(defun %text (message)
  (let ((p (mq-protocol:mq-message-payload message)))
    (if (stringp p) p (map 'string #'code-char p))))

(deftest unwinding-from-consumer-cancelled-does-not-desync
  (let* ((conn (%scripted (%frames '(:basic-cancel 1 :consumer-tag "ctag" :no-wait t)
                                   (%declare-ok "first")
                                   (%declare-ok "second"))))
         (b (mq-backend-amqp:make-amqp-backend :connection conn)))
    (ok (eq :cancelled
            (handler-case (mq-backend-amqp:amqp-declare-queue b "first")
              (mq-backend-amqp:amqp-consumer-cancelled () :cancelled)))
        "a handler-case sees the broker's cancel")
    (let ((next (handler-case (mq-backend-amqp:amqp-declare-queue b "second")
                  (mq-protocol:mq-connection-error () :connection-error))))
      (ok (member next '("second" :connection-error) :test #'equal)
          (format nil "the next Queue.Declare gets its own reply or a connection error, not the first one's (got ~S)"
                  next)))))

(deftest live-unwinding-from-consumer-cancelled-does-not-desync
  ;; A consumer on QA; another connection deletes QA, so the broker sends
  ;; Basic.Cancel. A poll of QB (holding "for-b") is unwound by a
  ;; handler-case; the next poll of the empty QC must not return "for-b".
  (with-broker ()
    (let ((b (%live))
          (admin (%live))
          (qa (%queue-name "r2-cancel-a"))
          (qb (%queue-name "r2-cancel-b"))
          (qc (%queue-name "r2-cancel-c")))
      (unwind-protect
          (progn
            (mq-backend-amqp:amqp-declare-queue b qa :auto-delete nil)
            (mq-backend-amqp:amqp-declare-queue b qb :exclusive t)
            (mq-backend-amqp:amqp-declare-queue b qc :exclusive t)
            (mq-protocol:subscribe b qa (lambda (m) (declare (ignore m))))
            (mq-protocol:publish b qb (mq-protocol:make-mq-message :payload "for-b"))
            (mq-backend-amqp:amqp-rpc (mq-backend-amqp:amqp-backend-connection admin)
                                      :queue-delete '(:queue-delete-ok)
                                      :channel 1 :reserved-1 0 :queue qa
                                      :if-unused nil :if-empty nil :no-wait nil)
            (sleep 0.3)
            (ok (eq :cancelled
                    (handler-case (mq-protocol:poll-messages b qb)
                      (mq-backend-amqp:amqp-consumer-cancelled () :cancelled)))
                "a handler-case around poll-messages sees the broker's cancel")
            (let ((got (handler-case (mapcar #'%text (mq-protocol:poll-messages b qc :timeout 0.5))
                         (mq-protocol:mq-connection-error () :connection-error))))
              (ok (member got '(nil :connection-error))
                  (format nil "a poll of the empty queue C returns nothing or a connection error (got ~S)"
                          got))))
        (ignore-errors (close admin))
        (ignore-errors (close b))))))

(deftest live-poll-returns-only-the-polled-queue
  (with-broker ()
    (let ((b (%live))
          (qa (%queue-name "r2-poll-a"))
          (qb (%queue-name "r2-poll-b")))
      (unwind-protect
          (progn
            (mq-backend-amqp:amqp-declare-queue b qa :exclusive t)
            (mq-backend-amqp:amqp-declare-queue b qb :exclusive t)
            (mq-protocol:subscribe b qa (lambda (m) (declare (ignore m))))
            (mq-protocol:publish b qa (mq-protocol:make-mq-message :payload "for-a"))
            (mq-protocol:publish b qb (mq-protocol:make-mq-message :payload "for-b"))
            (sleep 0.3)
            (let ((got (mapcar #'%text
                               (append (mq-protocol:poll-messages b qb :timeout 1)
                                       (mq-protocol:poll-messages b qb :timeout 0.5)))))
              (ok (equal '("for-b") got)
                  (format nil "two polls of queue B return B's one message only (got ~S)" got))))
        (ignore-errors (close b))))))

(deftest-parametrize bad-amqp-url-errors-mask-special-character-passwords
    ((url)
     ("amqp://app:s3cret/x+y==@rabbit.internal/prod")
     ("amqp://app:s3cret#1@rabbit.internal/prod")
     ("amqp://app:s3cret?1@rabbit.internal/prod"))
  (let ((e (handler-case (progn (mq-backend-amqp:parse-amqp-url url) nil)
             (error (e) e))))
    (ok (typep e 'mq-protocol:mq-error)
        (format nil "a bad URL signals mq-error (got ~A)" (type-of e)))
    (ok (and e (not (search "s3cret" (princ-to-string e))))
        (format nil "no password in the report: ~A" e))))
