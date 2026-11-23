(in-package #:mq-backend-amqp/tests)

;;; Evidence against PR #4 (fix for #2): the handshake advertises
;;; consumer_cancel_notify in Start-Ok (*AMQP-CLIENT-PROPERTIES*), so
;;; RabbitMQ sends Basic.Cancel when a consumed queue goes away (deleted,
;;; or its node fails). %AWAIT treats that Basic.Cancel as "unexpected"
;;; and signals AMQP-FRAME-ERROR in the middle of an unrelated RPC,
;;; leaving that RPC's own reply unread on the socket. The next RPC reads
;;; the stale reply: the connection stays one reply behind.

(deftest scripted-basic-cancel-during-rpc
  ;; The broker cancels consumer "ctag" just before it answers our
  ;; Queue.Declare; then it answers the second Queue.Declare.
  (let* ((conn (%scripted (%frames '(:basic-cancel 1 :consumer-tag "ctag" :no-wait t)
                                   '(:queue-declare-ok 1 :queue "first"
                                     :message-count 0 :consumer-count 0)
                                   '(:queue-declare-ok 1 :queue "second"
                                     :message-count 0 :consumer-count 0))))
         (b (mq-backend-amqp:make-amqp-backend :connection conn)))
    (ok (equal "first"
               (handler-case (mq-backend-amqp:amqp-declare-queue b "first")
                 (mq-protocol:mq-error (e) (princ-to-string e))))
        "a server-sent Basic.Cancel does not fail the Queue.Declare in flight")
    (ok (equal "second"
               (handler-case (mq-backend-amqp:amqp-declare-queue b "second")
                 (mq-protocol:mq-error (e) (princ-to-string e))))
        "the next Queue.Declare gets its own Declare-Ok, not the previous one")))

(deftest live-deleted-queue-cancel-keeps-the-connection-usable
  (with-broker ()
    (let ((b (%live))
          (admin (%live))
          (q (%queue-name "cancelled"))
          (other (%queue-name "other")))
      (unwind-protect
           (progn
             (mq-backend-amqp:amqp-declare-queue b q :auto-delete nil)
             (mq-protocol:subscribe b q (lambda (m) (declare (ignore m))))
             ;; Another client (or an operator) deletes the queue; RabbitMQ
             ;; cancels b's consumer with a Basic.Cancel to b.
             (mq-backend-amqp:amqp-rpc (mq-backend-amqp:amqp-backend-connection admin)
                                       :queue-delete '(:queue-delete-ok)
                                       :channel 1 :reserved-1 0 :queue q
                                       :if-unused nil :if-empty nil :no-wait nil)
             (sleep 0.3)
             (let ((declared (handler-case
                                 (mq-backend-amqp:amqp-declare-queue b other
                                                                     :exclusive t :auto-delete t)
                               (mq-protocol:mq-error (e) (princ-to-string e)))))
               (ok (equal other declared)
                   (format nil "Queue.Declare after the cancel (got ~S)" declared)))
             (let ((polled (handler-case
                               (progn
                                 (mq-protocol:publish b other
                                                      (mq-protocol:make-mq-message :payload "after"))
                                 (mapcar (lambda (m) (map 'string #'code-char
                                                          (mq-protocol:mq-message-payload m)))
                                         (mq-protocol:poll-messages b other :timeout 2)))
                             (mq-protocol:mq-error (e) (princ-to-string e)))))
               (ok (equal '("after") polled)
                   (format nil "publish + poll still work (got ~S)" polled))))
        (close admin)
        (close b)))))
