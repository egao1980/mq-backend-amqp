(in-package #:mq-backend-amqp/tests)

;;; #8 and the ack rules around it. Delivery tags are per channel: after a
;;; broker Channel.Close the reopened channel numbers its deliveries from 1
;;; again. ACK / NACK never send a tag handed out before the close; they
;;; signal AMQP-STALE-DELIVERY (pika: ChannelWrongStateError), with restart
;;; CONTINUE (the Java client's silent drop). Tags handed out after a close
;;; are offset, so an integer or a copied message still names one delivery.
;;; Nor are these sent: a second ack, another connection's delivery, a
;;; no-ack consumer's delivery, a message without a tag, and a multiple ack
;;; over deliveries not handed out yet.

(defun %octets-of (text)
  (map '(vector (unsigned-byte 8)) #'char-code text))

(defun %get-ok (tag text)
  "Frame specs of a Basic.Get-Ok on channel 1: wire delivery TAG, body TEXT."
  (list (list :basic-get-ok 1 :delivery-tag tag :redelivered nil
              :exchange "" :routing-key "q" :message-count 0)
        (list :header 1 (length text))
        (list :body 1 (%octets-of text))))

(defun %deliver (consumer tag text)
  "Frame specs of a Basic.Deliver to CONSUMER on channel 1."
  (list (list :basic-deliver 1 :consumer-tag consumer :delivery-tag tag :redelivered nil
              :exchange "" :routing-key "q")
        (list :header 1 (length text))
        (list :body 1 (%octets-of text))))

(defun %close-406 ()
  '(:channel-close 1 :reply-code 406 :reply-text "PRECONDITION_FAILED"
    :class-id 50 :method-id 10))

(defun %reopen-script ()
  "a1 (wire tag 1) from QA; the next request gets Channel.Close 406; the
channel reopens; b1 (wire tag 1 again) from QB."
  (apply #'%frames (append (%get-ok 1 "a1")
                           (list (%close-406) '(:channel-open-ok 1 :reserved-1 ""))
                           (%get-ok 1 "b1"))))

(defun %across-a-reopen (conn)
  "Run %REOPEN-SCRIPT's requests on CONN. → (values backend a1 b1)."
  (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
         (a1 (mq-backend-amqp::%basic-get b conn "qa")))
    (handler-case (mq-backend-amqp:amqp-declare-queue b "qa" :durable t)
      (mq-backend-amqp:amqp-channel-closed () nil))
    (mq-backend-amqp::%ensure-channel conn)
    (values b a1 (mq-backend-amqp::%basic-get b conn "qb"))))

(defun %acks (out)
  "The Basic.Ack / Basic.Nack frames written to OUT since it was last read,
as (name wire-tag multiple)."
  (loop for (nil nil m) in (%written out)
        when (and m (member (mq-backend-amqp:amqp-method-name m) '(:basic-ack :basic-nack)))
          collect (let ((args (mq-backend-amqp:amqp-method-arguments m)))
                    (list (mq-backend-amqp:amqp-method-name m)
                          (getf args :delivery-tag)
                          (getf args :multiple)))))

(defun %refusal (thunk)
  "→ the reason of the AMQP-STALE-DELIVERY that THUNK signals, or :SENT."
  (handler-case (progn (funcall thunk) :sent)
    (mq-backend-amqp:amqp-stale-delivery (e) (mq-backend-amqp:amqp-stale-delivery-reason e))))

(deftest stale-ack-after-a-channel-reopen-is-not-sent
  (multiple-value-bind (conn out) (%scripted (%reopen-script))
    (multiple-value-bind (b a1 b1) (%across-a-reopen conn)
      (%written out)
      (ok (and a1 b1) "a1 before the close, b1 after it")
      (ok (eql 1 (mq-protocol:mq-message-delivery-tag a1)) "a1 has tag 1")
      (ok (eql 2 (mq-protocol:mq-message-delivery-tag b1))
          "b1, wire tag 1 on the reopened channel, is tag 2 on the connection")
      (ok (eq :channel-closed (%refusal (lambda () (mq-protocol:ack b a1)))) "ack a1 is refused")
      (ok (eq :channel-closed (%refusal (lambda () (mq-protocol:nack b a1)))) "so is nack a1")
      (ok (eq :channel-closed (%refusal (lambda () (mq-protocol:ack b 1))))
          "and an ack of integer tag 1")
      (ok (eq :channel-closed (%refusal (lambda () (mq-protocol:ack b a1 :multiple t))))
          "and a multiple ack up to a1")
      (ok (null (%acks out)) "nothing was sent for a1")
      (ok (eq :sent (%refusal (lambda () (mq-protocol:ack b b1)))))
      (ok (equal '((:basic-ack 1 nil)) (%acks out)) "ack b1 sends its wire tag, 1"))))

(deftest stale-delivery-report-and-type
  (multiple-value-bind (conn out) (%scripted (%reopen-script))
    (declare (ignore out))
    (multiple-value-bind (b a1) (%across-a-reopen conn)
      (let ((e (handler-case (progn (mq-protocol:ack b a1) nil)
                 (mq-protocol:mq-error (e) e))))
        (ok (typep e 'mq-backend-amqp:amqp-stale-delivery) "an mq-error: amqp-stale-delivery")
        (ok (and e (eql 1 (mq-backend-amqp:amqp-stale-delivery-tag e))))
        (ok (and e (search "channel closed" (princ-to-string e)))
            (format nil "the report says why (~A)" e))))))

(deftest integer-and-copied-tags-name-the-reopened-channel-delivery
  (multiple-value-bind (conn out) (%scripted (%reopen-script))
    (multiple-value-bind (b a1 b1) (%across-a-reopen conn)
      (declare (ignore a1))
      (%written out)
      (mq-protocol:ack b (mq-protocol:mq-message-delivery-tag b1))
      (ok (equal '((:basic-ack 1 nil)) (%acks out)) "integer tag 2 acks wire tag 1")
      ;; A plain MQ-MESSAGE that carries b1's tag (a copy) maps the same way.
      (mq-protocol:nack b (mq-protocol:make-mq-message
                           :payload "b1" :delivery-tag (mq-protocol:mq-message-delivery-tag b1))
                        :requeue t)
      (ok (equal '((:basic-nack 1 nil)) (%acks out)) "a copy of b1 nacks wire tag 1"))))

(deftest continue-skips-a-stale-ack
  (multiple-value-bind (conn out) (%scripted (%reopen-script))
    (multiple-value-bind (b a1) (%across-a-reopen conn)
      (%written out)
      (ok (null (handler-bind ((mq-backend-amqp:amqp-stale-delivery #'continue))
                  (mq-protocol:ack b a1)))
          "with restart CONTINUE ack returns NIL")
      (ok (null (handler-bind ((mq-backend-amqp:amqp-stale-delivery #'continue))
                  (mq-protocol:nack b a1 :requeue nil)))
          "and so does nack")
      (ok (null (%acks out)) "nothing is sent"))))

(deftest a-delivery-is-settled-once
  (multiple-value-bind (conn out)
      (%scripted (apply #'%frames (append (%get-ok 1 "m1") (%get-ok 2 "m2") (%get-ok 3 "m3"))))
    (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
           (m1 (mq-backend-amqp::%basic-get b conn "q"))
           (m2 (mq-backend-amqp::%basic-get b conn "q"))
           (m3 (mq-backend-amqp::%basic-get b conn "q")))
      (%written out)
      (mq-protocol:ack b m1)
      (ok (eq :settled (%refusal (lambda () (mq-protocol:ack b m1)))) "a second ack of m1")
      (ok (eq :settled (%refusal (lambda () (mq-protocol:nack b m1)))) "a nack after the ack")
      (mq-protocol:nack b m3 :multiple t :requeue t)
      (ok (eq :settled (%refusal (lambda () (mq-protocol:ack b m2))))
          "m2, covered by the multiple nack of m3")
      (ok (eq :settled (%refusal (lambda () (mq-protocol:ack b 2)))) "or its integer tag")
      (ok (equal '((:basic-ack 1 nil) (:basic-nack 3 t)) (%acks out))
          "the broker got one ack of m1 and one multiple nack"))))

(deftest a-delivery-from-another-connection-is-not-sent
  (let* ((c1 (%scripted (apply #'%frames (%get-ok 1 "m1"))))
         (b1 (mq-backend-amqp:make-amqp-backend :connection c1))
         (m1 (mq-backend-amqp::%basic-get b1 c1 "q")))
    (multiple-value-bind (c2 out2) (%scripted (%frames))
      (let ((b2 (mq-backend-amqp:make-amqp-backend :connection c2)))
        (ok (eq :other-connection (%refusal (lambda () (mq-protocol:ack b2 m1)))))
        (ok (null (%acks out2)) "nothing is sent on the other connection")))))

(deftest a-no-ack-delivery-is-not-acked
  (multiple-value-bind (conn out)
      (%scripted (apply #'%frames (append (%deliver "cl-stack.ctag.1" 1 "m1")
                                          (list (%declare-ok "q")))))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn)))
      (mq-protocol:subscribe b "q" (lambda (m) (declare (ignore m))) :no-ack t :no-wait t)
      (mq-backend-amqp:amqp-declare-queue b "q")
      (let ((m (first (mq-backend-amqp::%take-pending conn "q" 1))))
        (%written out)
        (ok m "the Deliver to the no-ack consumer was kept")
        (ok (eq :no-ack (%refusal (lambda () (mq-protocol:ack b m)))))
        (ok (null (%acks out)))))))

(deftest a-message-without-a-delivery-tag-is-not-acked
  (multiple-value-bind (conn out) (%scripted (%frames))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn))
          (m (mq-protocol:make-mq-message :payload "built here")))
      (ok (signals (mq-protocol:ack b m) 'mq-protocol:mq-error))
      (ok (signals (mq-protocol:ack b m :multiple t) 'mq-protocol:mq-error)
          "nor with :multiple, which used to send tag 0: every delivery")
      (ok (signals (mq-protocol:nack b 0) 'mq-protocol:mq-error) "nor integer 0 alone")
      (ok (null (%acks out))))))

(deftest a-multiple-ack-never-covers-a-delivery-not-handed-out
  (multiple-value-bind (conn out)
      (%scripted (apply #'%frames (append (%deliver "ctag-a" 1 "a1") (%get-ok 2 "b1"))))
    (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
           (b1 (mq-backend-amqp::%basic-get b conn "qb")))
      (%written out)
      (ok (= 1 (length (mq-backend-amqp::%pending-deliveries conn)))
          "a1, for QA's consumer, came first and is kept")
      (ok (signals (mq-protocol:ack b b1 :multiple t) 'mq-protocol:mq-error)
          "a multiple ack of b1 would settle a1 unseen")
      (ok (signals (mq-protocol:ack b 0 :multiple t) 'mq-protocol:mq-error)
          "so would an ack of everything")
      (ok (null (%acks out)))
      (mq-protocol:ack b b1)
      (ok (equal '((:basic-ack 2 nil)) (%acks out)) "one by one is fine"))))

(deftest held-back-deliveries-of-a-closed-channel-are-dropped
  ;; A poll a handler unwinds holds its messages back; if the channel
  ;; closed meanwhile, the broker requeued them, so they are not kept.
  (let* ((conn (%scripted (apply #'%frames (append (%get-ok 1 "a1") (list (%close-406))))))
         (b (mq-backend-amqp:make-amqp-backend :connection conn))
         (a1 (mq-backend-amqp::%basic-get b conn "qa")))
    (handler-case (mq-backend-amqp:amqp-declare-queue b "qa" :durable t)
      (mq-backend-amqp:amqp-channel-closed () nil))
    (mq-backend-amqp::%hold-back conn (list a1))
    (ok (null (mq-backend-amqp::%pending-deliveries conn)))))

(deftest live-integer-tag-after-a-reopen-acks-the-right-message
  (with-broker ()
    (let ((qa (%queue-name "tag-a"))
          (qb (%queue-name "tag-b")))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (mq-backend-amqp:amqp-declare-queue b qa)
        (mq-backend-amqp:amqp-declare-queue b qb)
        (mq-protocol:publish b qa (mq-protocol:make-mq-message :payload "a1"))
        (mq-protocol:publish b qb (mq-protocol:make-mq-message :payload "b1"))
        (let ((a1 (first (mq-protocol:poll-messages b qa :timeout 2))))
          (ok (signals (mq-backend-amqp:amqp-declare-queue b qa :durable t)
                       'mq-backend-amqp:amqp-channel-closed))
          (let ((b1 (first (mq-protocol:poll-messages b qb :timeout 2))))
            (ok (and a1 b1))
            (ok (eq :channel-closed
                    (%refusal (lambda () (mq-protocol:ack b (mq-protocol:mq-message-delivery-tag a1)))))
                "a1's integer tag is refused")
            (mq-protocol:ack b (mq-protocol:mq-message-delivery-tag b1)))))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (let ((again-b (mq-protocol:poll-messages b qb :n 5 :timeout 1))
              (again-a (mq-protocol:poll-messages b qa :n 5 :timeout 2)))
          (ok (null again-b) (format nil "b1 was acked by its integer tag (QB has ~S)"
                                     (mapcar #'%text again-b)))
          (ok (equal '("a1") (mapcar #'%text again-a))
              (format nil "a1 was requeued by the close and comes back (QA has ~S)"
                      (mapcar #'%text again-a)))
          (ok (and again-a (mq-protocol:mq-message-redelivered-p (first again-a)))
              "marked redelivered")
          (dolist (m (append again-a again-b)) (mq-protocol:ack b m)))
        (mq-backend-amqp:amqp-rpc (mq-backend-amqp:amqp-backend-connection b)
                                  :queue-delete '(:queue-delete-ok)
                                  :channel 1 :reserved-1 0 :queue qa
                                  :if-unused nil :if-empty nil :no-wait nil)
        (mq-backend-amqp:amqp-rpc (mq-backend-amqp:amqp-backend-connection b)
                                  :queue-delete '(:queue-delete-ok)
                                  :channel 1 :reserved-1 0 :queue qb
                                  :if-unused nil :if-empty nil :no-wait nil)))))
