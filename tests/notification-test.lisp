(in-package #:mq-backend-amqp/tests)

;;; Broker notifications that are nobody's reply (Basic.Cancel, Channel.Flow)
;;; and frames nobody expects. The RPC in flight keeps waiting for its own
;;; reply; an unexpected frame closes the connection (505) instead of
;;; leaving later RPCs one reply behind.

(defun %declare-ok (name)
  (list :queue-declare-ok 1 :queue name :message-count 0 :consumer-count 0))

(deftest basic-cancel-without-no-wait-is-answered-and-signalled
  (multiple-value-bind (conn out)
      (%scripted (%frames '(:basic-cancel 1 :consumer-tag "ctag" :no-wait nil)
                          (%declare-ok "q")))
    (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
           (seen '())
           (declared (handler-bind ((mq-backend-amqp:amqp-consumer-cancelled
                                      (lambda (c)
                                        (push (mq-backend-amqp:amqp-cancelled-consumer-tag c)
                                              seen))))
                       (mq-backend-amqp:amqp-declare-queue b "q"))))
      (ok (equal "q" declared) "Queue.Declare gets its Declare-Ok")
      (ok (equal '("ctag") seen) "AMQP-CONSUMER-CANCELLED is signalled with the tag")
      (ok (equal '("ctag") (mq-backend-amqp:amqp-connection-cancelled-consumers conn))
          "the tag is recorded on the connection")
      (let ((w (%written out)))
        (ok (equal '(:queue-declare :basic-cancel-ok) (%names w))
            (format nil "Cancel-Ok answers a Cancel without no-wait: ~S" (%names w)))
        (ok (equal "ctag" (getf (%args w :basic-cancel-ok) :consumer-tag)))))))

(deftest basic-cancel-with-no-wait-is-not-answered
  ;; RabbitMQ's consumer cancel notification sets no-wait.
  (multiple-value-bind (conn out)
      (%scripted (%frames '(:basic-cancel 1 :consumer-tag "ctag" :no-wait t)
                          (%declare-ok "q")))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn)))
      (ok (equal "q" (mq-backend-amqp:amqp-declare-queue b "q")))
      (ok (equal '(:queue-declare) (%names (%written out))) "no Cancel-Ok"))))

(deftest channel-flow-is-answered-with-flow-ok
  (multiple-value-bind (conn out)
      (%scripted (%frames '(:channel-flow 1 :active nil)
                          (%declare-ok "q")
                          '(:channel-flow 1 :active t)
                          (%declare-ok "r")))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn)))
      (ok (equal "q" (mq-backend-amqp:amqp-declare-queue b "q")))
      (ok (null (mq-backend-amqp:amqp-connection-flow-active conn)) "flow off recorded")
      (let ((w (%written out)))
        (ok (equal '(:queue-declare :channel-flow-ok) (%names w)))
        (ok (null (getf (%args w :channel-flow-ok) :active)) "Flow-Ok echoes active=false"))
      (ok (equal "r" (mq-backend-amqp:amqp-declare-queue b "r")))
      (ok (mq-backend-amqp:amqp-connection-flow-active conn) "flow on again"))))

(deftest unexpected-frame-closes-instead-of-desyncing
  ;; A method no request waits for (here a Basic.Consume-Ok during
  ;; Queue.Declare): the RPC fails, and so does the next one, instead of
  ;; reading the first one's Declare-Ok.
  (let* ((conn (%scripted (%frames '(:basic-consume-ok 1 :consumer-tag "stray")
                                   (%declare-ok "first")
                                   (%declare-ok "second"))))
         (b (mq-backend-amqp:make-amqp-backend :connection conn)))
    (ok (signals (mq-backend-amqp:amqp-declare-queue b "first")
                 'mq-backend-amqp:amqp-frame-error)
        "the unexpected frame is an AMQP-FRAME-ERROR")
    (ok (mq-backend-amqp:amqp-connection-closed-p conn) "the connection is closed (505)")
    (let ((next (handler-case (mq-backend-amqp:amqp-declare-queue b "second")
                  (mq-protocol:mq-connection-error () :connection-error))))
      (ok (eq :connection-error next)
          (format nil "the next RPC fails cleanly, not with a stale reply (got ~S)" next)))))

(deftest live-cancel-is-recorded
  (with-broker ()
    (let ((b (%live))
          (admin (%live))
          (q (%queue-name "cancel-recorded")))
      (unwind-protect
           (let ((tag (mq-protocol:mq-subscription-tag
                       (progn
                         (mq-backend-amqp:amqp-declare-queue b q :auto-delete nil)
                         (mq-protocol:subscribe b q (lambda (m) (declare (ignore m))))))))
             (mq-backend-amqp:amqp-rpc (mq-backend-amqp:amqp-backend-connection admin)
                                       :queue-delete '(:queue-delete-ok)
                                       :channel 1 :reserved-1 0 :queue q
                                       :if-unused nil :if-empty nil :no-wait nil)
             (sleep 0.3)
             ;; Any RPC reads the pending Basic.Cancel.
             (mq-backend-amqp:amqp-declare-queue b "" :exclusive t :auto-delete t)
             (ok (member tag (mq-backend-amqp:amqp-connection-cancelled-consumers
                              (mq-backend-amqp:amqp-backend-connection b))
                         :test #'equal)
                 (format nil "the broker's cancel of ~S is recorded" tag)))
        (close admin)
        (close b)))))
