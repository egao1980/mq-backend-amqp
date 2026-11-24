(in-package #:mq-backend-amqp/tests)

;;; Round-5 adversarial evidence against PR #11 (agent/amqp-hardening-0f71,
;;; 0.2.1). Each test says what a correct client does (pika 1.3.2 against
;;; the same broker). Live tests need RabbitMQ (see connect-test.lisp); the
;;; fake-broker test uses %SERVE-ONCE (heartbeat-test.lisp).

;;; Breaks (each test below fails on 89e4d6a):
;;;
;;; 1. Short and long strings are Latin-1, not UTF-8 (STRING-OCTETS /
;;;    OCTETS-STRING in codec.lisp). A publish to the queue "café" goes to
;;;    routing key 63 61 66 E9, matches no queue, and is dropped while
;;;    PUBLISH returns normally; "заказы" is refused; a UTF-8
;;;    correlation-id reads as mojibake.
;;; 2. Consumer tags are recorded per connection, not per channel, and
;;;    never forgotten (%CONSUMER-QUEUES, %NO-ACK-CONSUMERS). The same tag
;;;    on two channels files channel 1's delivery under channel 2's queue
;;;    (never handed out, never acked); a tag reused after a broker cancel
;;;    keeps its old no-ack flag, so the new delivery cannot be acked.
;;; 3. Every operation first drains the socket until it is idle, appending
;;;    each delivery to a list (%DRAIN-INCOMING, %INCIDENTAL-FRAME): with a
;;;    consumer on a queue that keeps filling, any call waits for the
;;;    queue to stop filling, and memory grows without bound.
;;; 4. A frame must arrive whole within two heartbeat intervals, even while
;;;    its octets keep coming (%NEXT-FRAME): a slow link drops the
;;;    connection, and its unacked messages are redelivered.
;;; 5. (Lesser) A connect failure's backtrace prints the URL with its
;;;    password.

(defun %utf-8 (string)
  (encoding-protocol:encode string :encoding :utf-8))

(defun %queue-rpc (conn queue &key passive)
  "Queue.Declare of QUEUE (a string, or the octets of its name) on channel 1
of CONN. → its message count."
  (getf (mq-backend-amqp:amqp-method-arguments
         (mq-backend-amqp:amqp-rpc conn :queue-declare '(:queue-declare-ok)
                                   :channel 1 :reserved-1 0 :queue queue :passive passive
                                   :durable nil :exclusive nil :auto-delete nil
                                   :no-wait nil :arguments nil))
        :message-count))

(defun %delete-queue (queue)
  "Delete QUEUE (a string, or the octets of its name) on a fresh connection."
  (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                        :user (%user) :password (%password))
    (mq-backend-amqp:amqp-rpc (mq-backend-amqp:amqp-backend-connection b)
                              :queue-delete '(:queue-delete-ok)
                              :channel 1 :reserved-1 0 :queue queue
                              :if-unused nil :if-empty nil :no-wait nil)))

(deftest live-a-publish-to-a-non-ascii-queue-reaches-that-queue
  ;; AMQP 0-9-1 short strings are UTF-8 (4.2.5.3); pika encodes the queue
  ;; name "café" as 63 61 66 C3 A9. The queue is declared under those
  ;; octets, as pika (or the management UI) names it; then this backend
  ;; publishes to "café" by name.
  (with-broker ()
    (let* ((name (format nil "caf~C.~D" (code-char #xE9) (random 100000000)))
           (wire (%utf-8 name))
           (outcome nil)
           (count nil))
      (unwind-protect
           (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                                 :user (%user) :password (%password))
             (let ((conn (mq-backend-amqp:amqp-backend-connection b)))
               (%queue-rpc conn wire)
               (setf outcome (%outcome (lambda ()
                                         (mq-protocol:publish
                                          b name (mq-protocol:make-mq-message :payload "m1")))))
               (sleep 0.3)
               (setf count (%queue-rpc conn wire :passive t))))
        (%delete-queue wire))
      (format t "~&    publish to ~S: ~S; the queue holds ~S message~:P~%" name outcome count)
      (ok (eq :returned outcome) "the publish returned normally")
      (ok (eql 1 count)
          (format nil "so the message is in queue ~S (it holds ~S)" name count)))))

(deftest a-non-ascii-queue-name-goes-out-as-utf-8
  ;; pika: basic_publish("", "café", ...) sends routing key 63 61 66 C3 A9,
  ;; and publishes to "заказы" (Cyrillic) as well.
  (multiple-value-bind (conn out) (%scripted (%frames))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn))
          (cafe (format nil "caf~C" (code-char #xE9)))
          (orders (coerce (mapcar #'code-char '(#x437 #x430 #x43A #x430 #x437 #x44B)) 'string)))
      (mq-protocol:publish b cafe (mq-protocol:make-mq-message :payload "m1"))
      (let ((octets (flexi-streams:get-output-stream-sequence out)))
        (ok (search (%utf-8 cafe) octets)
            (format nil "the routing key is 63 61 66 C3 A9 (sent ~S)"
                    (let ((at (search #(99 97 102) octets))) (and at (subseq octets at (+ at 5)))))))
      (let ((outcome (%outcome (lambda ()
                                 (mq-protocol:publish b orders
                                                      (mq-protocol:make-mq-message :payload "m2"))))))
        (ok (eq :returned outcome)
            (format nil "a publish to ~A goes out (got ~S)" orders outcome))))))

(deftest a-utf-8-correlation-id-is-read-as-utf-8
  ;; A pika publisher's correlation_id "é" is C3 A9 on the wire.
  (let* ((key (string (code-char #xE9)))
         (input (concatenate '(vector (unsigned-byte 8))
                             (%frames (list :basic-get-ok 1 :delivery-tag 1 :redelivered nil
                                            :exchange "" :routing-key "q" :message-count 0))
                             (mq-backend-amqp:encode-content-header
                              :class-id 60 :channel 1 :body-size 2
                              :properties (list :correlation-id (%utf-8 key)))
                             (%frames (list :body 1 (%octets-of "m1")))))
         (conn (%scripted input))
         (b (mq-backend-amqp:make-amqp-backend :connection conn))
         (m (mq-backend-amqp::%basic-get b conn "q")))
    (ok (equal key (mq-protocol:mq-message-key m))
        (format nil "the key is ~S (got ~S)" key (mq-protocol:mq-message-key m)))))

(deftest live-the-same-consumer-tag-on-two-channels-keeps-each-ones-queue
  ;; Consumer tags are local to a channel (AMQP 0-9-1, Basic.Consume):
  ;; channel 1 consumes QA as "worker", channel 2 consumes QB as "worker".
  (with-broker ()
    (let ((qa (%queue-name "tag-a")) (qb (%queue-name "tag-b"))
          (got nil) (acks nil) (left nil))
      (mq-backend-amqp:with-amqp-backend (b1 :host (%host) :port (%port)
                                             :user (%user) :password (%password))
        (let* ((conn (mq-backend-amqp:amqp-backend-connection b1))
               (b2 (mq-backend-amqp:make-amqp-backend :connection conn :channel 2)))
          (mq-backend-amqp:amqp-declare-queue b1 qa)
          (mq-backend-amqp:amqp-declare-queue b1 qb)
          (mq-protocol:subscribe b1 qa nil :consumer-tag "worker")
          (mq-protocol:subscribe b2 qb nil :consumer-tag "worker")
          (mq-protocol:publish b1 qa (mq-protocol:make-mq-message :payload "a1"))
          (setf got (mq-protocol:poll-messages b1 qa :timeout 2))
          (setf acks (mapcar (lambda (m) (%refusal (lambda () (mq-protocol:ack b1 m)))) got))))
      (setf left (%drain-queue qa))
      (%drain-queue qb)
      (format t "~&    poll of QA on channel 1: ~S, acks ~S; QA holds ~S afterwards~%"
              (mapcar #'%text got) acks left)
      (ok (equal '("a1") (mapcar #'%text got)) "the poll of QA hands out a1")
      (ok (null left) (format nil "a1 was acked, so QA is empty (it holds ~S)" left)))))

(deftest a-delivery-belongs-to-its-own-channels-consumer
  ;; The same rule on a scripted broker: channel 1's "worker" consumes qa,
  ;; channel 2's "worker" consumes qb with no-ack. a1 arrives on channel 1
  ;; for "worker" while a Queue.Declare waits for its reply.
  (let ((conn (%scripted (apply #'%frames (append (%deliver "worker" 1 "a1")
                                                  (list (%declare-ok "x")))))))
    (let ((b1 (mq-backend-amqp:make-amqp-backend :connection conn))
          (b2 (mq-backend-amqp:make-amqp-backend :connection conn :channel 2)))
      (mq-protocol:subscribe b1 "qa" nil :consumer-tag "worker")
      (mq-protocol:subscribe b2 "qb" nil :consumer-tag "worker" :no-ack t)
      (mq-backend-amqp:amqp-declare-queue b1 "x")
      (let* ((a1 (first (mq-backend-amqp::%take-pending conn "qa" 10 1)))
             (ack (and a1 (%refusal (lambda () (mq-protocol:ack b1 a1))))))
        (ok a1 "a1 is handed out by a poll of qa on channel 1")
        (ok (eq :sent ack) (format nil "and its ack is sent (got ~S)" ack))))))

(deftest live-a-consumer-tag-reused-after-a-cancel-acks-normally
  ;; "worker" consumes QA with no-ack; QA is deleted, so the broker cancels
  ;; it (Basic.Cancel); "worker" then consumes QB with acks.
  (with-broker ()
    (let ((qa (%queue-name "reuse-a")) (qb (%queue-name "reuse-b"))
          (got nil) (acks nil) (left nil))
      (mq-backend-amqp:with-amqp-backend (b :host (%host) :port (%port)
                                            :user (%user) :password (%password))
        (let ((conn (mq-backend-amqp:amqp-backend-connection b)))
          (mq-backend-amqp:amqp-declare-queue b qa)
          (mq-backend-amqp:amqp-declare-queue b qb)
          (mq-protocol:subscribe b qa nil :consumer-tag "worker" :no-ack t)
          (mq-backend-amqp:amqp-rpc conn :queue-delete '(:queue-delete-ok)
                                    :channel 1 :reserved-1 0 :queue qa
                                    :if-unused nil :if-empty nil :no-wait nil)
          (mq-protocol:subscribe b qb nil :consumer-tag "worker")
          (mq-protocol:publish b qb (mq-protocol:make-mq-message :payload "b1"))
          (setf got (mq-protocol:poll-messages b qb :timeout 2))
          (setf acks (mapcar (lambda (m) (%refusal (lambda () (mq-protocol:ack b m)))) got))))
      (setf left (%drain-queue qb))
      (format t "~&    poll of QB: ~S, acks ~S; QB holds ~S afterwards~%"
              (mapcar #'%text got) acks left)
      (ok (equal '("b1") (mapcar #'%text got)) "the poll of QB hands out b1")
      (ok (equal '(:sent) acks) (format nil "its ack is sent (got ~S)" acks))
      (ok (null left) (format nil "so QB is empty (it holds ~S)" left)))))

(deftest live-an-operation-returns-while-a-consumed-queue-keeps-filling
  ;; A consumer on Q (SUBSCRIBE, so no prefetch limit); another connection
  ;; publishes to Q for 4 s. One second in, a Queue.Declare on the
  ;; consumer's connection. pika's queue_declare returns in 0.3 s with a
  ;; producer that never stops. The RPC runs in its own thread, which the
  ;; test stops after 10 s.
  (with-broker ()
    (let* ((q (%queue-name "flood")) (sent 0) (took nil) (outcome nil) (pending nil))
      (unwind-protect
          (let* ((b (mq-backend-amqp:connect-amqp-backend :host (%host) :port (%port)
                                                          :user (%user) :password (%password)))
                 (conn (mq-backend-amqp:amqp-backend-connection b)))
            (mq-backend-amqp:amqp-declare-queue b q)
            (mq-protocol:subscribe b q nil)
            (let ((producer
                    (bt2:make-thread
                     (lambda ()
                       (mq-backend-amqp:with-amqp-backend (p :host (%host) :port (%port)
                                                             :user (%user) :password (%password))
                         (loop with start = (get-internal-real-time)
                               while (< (%seconds-since start) 4)
                               do (mq-protocol:publish p q (mq-protocol:make-mq-message :payload "x"))
                                  (incf sent))))
                     :name "producer")))
              (sleep 1)
              (let* ((start (get-internal-real-time))
                     (rpc (bt2:make-thread
                           (lambda ()
                             (setf outcome
                                   (%outcome (lambda ()
                                               (mq-backend-amqp:amqp-declare-queue
                                                b (%queue-name "other") :exclusive t :auto-delete t))))
                             (setf took (%seconds-since start)))
                           :name "rpc")))
                (loop until (or took (> (%seconds-since start) 10))
                      do (sleep 0.1))
                (setf pending (length (mq-backend-amqp::%pending-deliveries conn)))
                (unless took
                  (bt2:destroy-thread rpc))
                (handler-case (bt2:join-thread rpc) (error () nil))
                (bt2:join-thread producer)))
            (close b :abort t))
        (%delete-queue q))
      (format t "~&    the producer sent ~D in 4 s; the Queue.Declare: ~A; ~D deliveries read into memory by then~%"
              sent
              (if took (format nil "~S after ~,1F s" outcome took) "not returned after 10 s")
              pending)
      (ok (and took (< took 5))
          (format nil "the Queue.Declare returns in a few seconds (~A)"
                  (if took (format nil "took ~,1F s" took) "still waiting after 10 s"))))))

(deftest a-frame-that-keeps-arriving-is-not-taken-for-a-dead-broker
  ;; heartbeat 1 s. The Get-Ok's 60 000-octet body frame arrives in 4 000
  ;; octet pieces every 0.25 s (about 16 KB/s, 3.75 s for the frame): the
  ;; broker is never silent for even half an interval.
  (multiple-value-bind (port thread)
      (%serve-once
       (lambda (srv socket)
         (declare (ignore socket))
         (%fake-handshake srv :heartbeat 1)
         (%client-method srv)           ; Basic.Get
         (let ((octets (apply #'%frames
                              (list :basic-get-ok 1 :delivery-tag 1 :redelivered nil
                                    :exchange "" :routing-key "q" :message-count 0)
                              (list (list :header 1 60000)
                                    (list :body 1 (make-array 60000 :element-type '(unsigned-byte 8)
                                                                    :initial-element 120)))))
               (stream (mq-backend-amqp:amqp-connection-stream srv)))
           (loop for start from 0 below (length octets) by 4000
                 do (mq-backend-amqp:write-amqp-octets
                     stream (subseq octets start (min (length octets) (+ start 4000))))
                    (sleep 0.25)))
         (sleep 3)))
    (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 1))
           (start (get-internal-real-time))
           (got (handler-case (mq-protocol:poll-messages b "q")
                  (error (e) e)))
           (took (%seconds-since start)))
      (close b :abort t)
      (bt2:join-thread thread)
      (format t "~&    after ~,1F s: ~A~%" took (if (consp got) "the message" got))
      (ok (and (consp got) (= 60000 (length (mq-protocol:mq-message-payload (first got)))))
          (format nil "the poll returns the message (got ~A after ~,1F s)"
                  (if (consp got) "it" got) took)))))

#+sbcl
(deftest a-connect-failure-backtrace-does-not-print-the-password
  ;; Lesser: what SBCL prints for an unhandled error under
  ;; --disable-debugger (services, cl-repo run): the backtrace, each frame
  ;; with its arguments. Nothing listens on 127.0.0.1:1.
  (let ((trace (block trace
                 (handler-bind ((mq-protocol:mq-connection-error
                                  (lambda (c)
                                    (declare (ignore c))
                                    (return-from trace
                                      (with-output-to-string (s)
                                        (sb-debug:print-backtrace :stream s :count 40))))))
                   (mq-backend-amqp:connect-amqp-backend
                    :url "amqp://app:pw-s3cret@127.0.0.1:1/prod" :connect-timeout 2)
                   nil))))
    (let ((at (and trace (search "pw-s3cret" trace))))
      (ok (and trace (not at))
          (format nil "the backtrace does not show the password~@[: ...~A...~]"
                  (and at (substitute #\Space #\Newline
                                      (subseq trace (max 0 (- at 70))
                                              (min (length trace) (+ at 40))))))))))
