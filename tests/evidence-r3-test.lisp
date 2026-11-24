(in-package #:mq-backend-amqp/tests)

;;; Round-3 evidence against PR #4.
;;;
;;; 1. After a broker Channel.Close the next operation reopens channel 1,
;;;    and its delivery tags start again at 1. A message handed out before
;;;    the close keeps its old tag, so ACK of it acknowledges whichever
;;;    message the new channel delivered under that tag. That message is
;;;    then gone from the broker without having been processed: lost, with
;;;    :at-least-once in the capabilities. (pika: the closed channel refuses
;;;    basic_ack with ChannelWrongStateError.)
;;;
;;; 2. #7 URL parsing: amqp://[::1]:5672/ keeps the brackets in the host
;;;    (usocket cannot resolve "[::1]"), and amqp://user:pw@[::1]/ is
;;;    refused with an escaping hint that does not apply. pika's
;;;    URLParameters reads both (host '::1').

(deftest an-ack-from-a-closed-channel-does-not-ack-another-message
  (with-broker ()
    (let ((qa (%queue-name "stale-a"))
          (qb (%queue-name "stale-b"))
          (a1 nil) (b1 nil) (closed nil) (ack-result nil))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (mq-backend-amqp:amqp-declare-queue b qa)
        (mq-backend-amqp:amqp-declare-queue b qb)
        (mq-protocol:publish b qa (mq-protocol:make-mq-message :payload "a1"))
        (mq-protocol:publish b qb (mq-protocol:make-mq-message :payload "b1"))
        (setf a1 (first (mq-protocol:poll-messages b qa :timeout 2)))
        ;; 406 PRECONDITION_FAILED: QA redeclared as durable closes channel 1.
        (setf closed (handler-case (progn (mq-backend-amqp:amqp-declare-queue b qa :durable t) nil)
                       (mq-backend-amqp:amqp-channel-closed (e) (mq-backend-amqp:amqp-reply-code e))))
        (setf b1 (first (mq-protocol:poll-messages b qb :timeout 2)))
        ;; A1 is done; ACK it. B1's worker then dies before acking B1.
        (setf ack-result (handler-case (progn (mq-protocol:ack b a1) :sent)
                           (mq-protocol:mq-error (e) (list :refused (princ-to-string e))))))
      (ok (eql 406 closed) (format nil "the redeclare closed the channel (~S)" closed))
      (ok (and a1 b1) "a1 was polled before the close, b1 after it")
      (format t "~&    a1 tag ~S, b1 tag ~S, ack of a1: ~S~%"
              (and a1 (mq-protocol:mq-message-delivery-tag a1))
              (and b1 (mq-protocol:mq-message-delivery-tag b1))
              ack-result)
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (let ((again (mq-protocol:poll-messages b qb :n 5 :timeout 2)))
          (ok (equal '("b1") (mapcar #'%text again))
              (format nil "b1 was never acked, so it is redelivered (got ~S)"
                      (mapcar #'%text again)))
          (dolist (m again) (mq-protocol:ack b m))
          (dolist (m (mq-protocol:poll-messages b qa :n 5 :timeout 1))
            (mq-protocol:ack b m)))))))

(deftest-parametrize ipv6-amqp-url-host-has-no-brackets
    ((url host port)
     ("amqp://[::1]:5672/%2F" "::1" 5672)
     ("amqps://[2001:db8::1]/%2F" "2001:db8::1" 5671))
  (let ((got (handler-case (mq-backend-amqp:parse-amqp-url url)
               (mq-protocol:mq-error (e) (list :refused (princ-to-string e))))))
    (ok (and (equal host (getf got :host)) (eql port (getf got :port)))
        (format nil "~A is host ~S port ~D, as in pika (got ~S)" url host port got))))

(deftest ipv6-amqp-url-with-a-password-parses
  (let ((got (handler-case (mq-backend-amqp:parse-amqp-url "amqp://app:pw@[::1]:5672/%2F")
               (mq-protocol:mq-error (e) (list :refused (princ-to-string e))))))
    (ok (and (equal "::1" (getf got :host)) (equal "app" (getf got :user))
             (equal "pw" (getf got :password)) (equal "/" (getf got :vhost)))
        (format nil "amqp://app:pw@[::1]:5672/%2F parses as in pika (got ~S)" got))))

(deftest connect-by-ipv6-amqp-url
  ;; Control first: the same broker by :host "::1"; skips without one there.
  (with-broker ()
    (let ((control (handler-case
                       (mq-backend-amqp:with-amqp-backend (b :host "::1" :port (%port)
                                                             :user (%user) :password (%password))
                         (mq-backend-amqp:amqp-declare-queue b "")
                         :ok)
                     (error () nil))))
      (if (null control)
          (skip "the broker does not answer on ::1")
          (let ((got (handler-case
                         (mq-backend-amqp:with-amqp-backend
                             (b :url (format nil "amqp://[::1]:~D/%2F" (%port))
                                :user (%user) :password (%password))
                           (mq-backend-amqp:amqp-declare-queue b "")
                           :ok)
                       (error (e) (list :error (princ-to-string e))))))
            (ok (eq :ok got)
                (format nil ":host \"::1\" works; amqp://[::1]:~D/%2F got ~S" (%port) got)))))))
