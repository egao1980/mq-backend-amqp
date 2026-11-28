(in-package #:mq-backend-amqp/tests)

;;; mq-backend-amqp#14: the 0.2.2 leftovers. Scripted streams and fake
;;; brokers on 127.0.0.1 (heartbeat-test.lisp), no RabbitMQ needed.

;;; --- Basic.Get: another channel's Close between Get-Ok and its content ----

(deftest a-get-ok-reads-its-content-past-another-channels-close
  (let ((get-ok (%get-ok 1 "m1")))
    (multiple-value-bind (conn out)
        (%scripted (%frames (first get-ok)
                            '(:channel-close 2 :reply-code 404 :reply-text "NOT_FOUND"
                              :class-id 60 :method-id 40)
                            (second get-ok)
                            (third get-ok)))
      (declare (ignore out))
      (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
             (got (handler-case (mq-backend-amqp::%basic-get b conn "q")
                    (mq-protocol:mq-error (e) e))))
        (ok (typep got 'mq-protocol:mq-message)
            (format nil "the Get-Ok's message comes back (got ~A)" got))
        (ok (and (typep got 'mq-protocol:mq-message)
                 (equalp (%octets-of "m1") (mq-protocol:mq-message-payload got)))
            "with its own content")
        (ok (mq-backend-amqp::%cs-close (mq-backend-amqp::%channel-state conn 2))
            "channel 2's Close is recorded for its next operation")))))

;;; --- CLOSE: one deadline for the whole wait for Close-Ok -----------------

(deftest close-gives-up-on-a-broker-that-only-sends-heartbeats
  (multiple-value-bind (port thread)
      (%serve-once (lambda (srv socket)
                     (declare (ignore socket))
                     (%fake-handshake srv :heartbeat 1)
                     ;; Never Close-Ok: heartbeats only, until the client hangs up.
                     (loop with start = (get-internal-real-time)
                           while (< (%seconds-since start) 10)
                           do (%send srv :heartbeat)
                              (sleep 0.3))))
    (let ((conn (mq-backend-amqp:open-amqp-connection :host "127.0.0.1" :port port :heartbeat 1))
          (start nil))
      (setf start (get-internal-real-time))
      (let ((mq-backend-amqp::*close-timeout* 1))
        (close conn))
      (let ((waited (%seconds-since start)))
        (ok (< waited 3) (format nil "close returned after ~,1F s (limit 1 s)" waited)))
      (ok (mq-backend-amqp:amqp-connection-closed-p conn) "the connection is closed")
      (bt2:join-thread thread))))

;;; --- The drain answers a broker Close: a broken pipe is a transport error -

(define-condition %epipe (stream-error) ()
  (:report "Broken pipe (test)"))

(defclass %broken-pipe (trivial-gray-streams:fundamental-binary-output-stream) ()
  (:documentation "An output stream whose every write fails as a broken pipe does."))

(defmethod stream-element-type ((s %broken-pipe))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-write-byte ((s %broken-pipe) byte)
  (declare (ignore byte))
  (error '%epipe :stream s))

(defmethod trivial-gray-streams:stream-write-sequence ((s %broken-pipe) seq start end &key)
  (declare (ignore seq start end))
  (error '%epipe :stream s))

(deftest a-broken-pipe-answering-a-broker-close-is-a-transport-error
  (let* ((in (flexi-streams:make-in-memory-input-stream (%frames (%close-406))))
         (conn (mq-backend-amqp:make-amqp-connection
                :stream (make-two-way-stream in (make-instance '%broken-pipe)))))
    (let ((e (handler-case (progn (mq-backend-amqp::%drain-incoming conn 1) nil)
               (error (e) e))))
      (ok (typep e 'mq-backend-amqp:amqp-transport-error)
          (format nil "amqp-transport-error (got ~S: ~A)" (and e (type-of e)) e)))))

;;; --- Field-table arrays decode to vectors, and vectors encode as arrays ---

(deftest field-table-arrays-round-trip
  (let* ((table (list (cons "x-death" (vector '(("count" . 1) ("queue" . "q"))
                                              '(("count" . 2) ("queue" . "r"))))
                      (cons "empty" (vector))
                      (cons "mixed" (vector 1 "two" t))))
         (back (handler-case (%read-table (%table-octets table))
                 (error (e) (list :error (princ-to-string e))))))
    (ok (equalp table back) (format nil "~S round-trips (got ~S)" table back))
    (ok (and (consp back) (vectorp (cdr (assoc "empty" back :test #'equal))))
        "an empty array decodes as #(), not NIL")))

(deftest octet-vectors-are-still-byte-arrays
  (let* ((octets (coerce #(1 2 255) '(vector (unsigned-byte 8))))
         (back (%read-table (%table-octets (list (cons "b" octets))))))
    (ok (equalp octets (cdr (first back))) "x round-trips")
    (ok (typep (cdr (first back)) '(vector (unsigned-byte 8))) "as octets")))

(deftest a-republished-x-death-header-is-encoded
  (let* ((x-death (%read-table (%table-octets (list (cons "x-death" (vector '(("count" . 1))))))))
         (e (handler-case (progn (%table-octets x-death) nil)
              (error (e) e))))
    (ok (null e) (format nil "the decoded table encodes again (got ~A)" e))))

;;; --- A multiple ack above every tag handed out is refused -----------------

(deftest a-multiple-ack-above-every-tag-handed-out-is-refused
  (multiple-value-bind (conn out) (%scripted (apply #'%frames (%get-ok 1 "a1")))
    (let* ((b (mq-backend-amqp:make-amqp-backend :connection conn))
           (a1 (mq-backend-amqp::%basic-get b conn "q")))
      (ok a1 "a1 handed out (tag 1)")
      (%written out)
      (ok (signals (mq-protocol:ack b 5 :multiple t) 'mq-protocol:mq-error)
          "ack 5 :multiple t → mq-error")
      (ok (null (%acks out)) (format nil "nothing sent (got ~S)" (%acks out)))
      (mq-protocol:ack b 5)
      (ok (equal '((:basic-ack 5 nil)) (%acks out))
          "a single ack of an unknown higher tag is still sent as given"))))

;;; --- AMQP-RPC on a channel whose broker Close is recorded -----------------

(deftest an-rpc-on-a-channel-the-broker-closed-reports-the-close
  (multiple-value-bind (conn out) (%scripted (%frames))
    (let ((state (mq-backend-amqp::%channel-state conn 2)))
      (setf (mq-backend-amqp::%cs-close state)
            '(:reply-code 404 :reply-text "NOT_FOUND" :class-id 60 :method-id 40)))
    (let ((e (handler-case
                 (progn (mq-backend-amqp:amqp-rpc conn :channel-close '(:channel-close-ok)
                                                  :channel 2 :reply-code 200 :reply-text "bye"
                                                  :class-id 0 :method-id 0)
                        nil)
               (error (e) e))))
      (ok (typep e 'mq-backend-amqp:amqp-channel-closed)
          (format nil "amqp-channel-closed 404 (got ~S: ~A)" (and e (type-of e)) e))
      (ok (null (%written out)) "no Channel.Close sent to a channel the broker closed")
      (ok (null (mq-backend-amqp::%cs-close (mq-backend-amqp::%channel-state conn 2)))
          "reported once"))))

;;; --- A keyword-headed cons destination is refused -------------------------

(deftest a-keyword-headed-destination-is-refused
  (multiple-value-bind (conn out) (%scripted (%frames))
    (let ((b (mq-backend-amqp:make-amqp-backend :connection conn)))
      (ok (signals (mq-protocol:publish b '(:queue . "q") "x") 'mq-protocol:mq-error)
          "(:queue . \"q\") → mq-error")
      (ok (null (%written out)) "nothing sent"))))

;;; --- Publish while the broker blocks the connection -----------------------

(defun %blocking-broker (&key unblock-after)
  "A fake broker that blocks the connection after the handshake, sends
heartbeats, and (UNBLOCK-AFTER seconds later) unblocks it. → (values port
thread methods), METHODS a cons whose car lists the methods it got."
  (let ((methods (list '())))
    (multiple-value-bind (port thread)
        (%serve-once
         (lambda (srv socket)
           (%fake-handshake srv :heartbeat 2)
           (%send srv '(:connection-blocked 0 :reason "low on memory"))
           (loop with start = (get-internal-real-time)
                 with unblocked = nil
                 while (< (%seconds-since start) 6)
                 do (when (and unblock-after (not unblocked)
                               (> (%seconds-since start) unblock-after))
                      (setf unblocked t)
                      (%send srv '(:connection-unblocked 0)))
                    (%send srv :heartbeat)
                    (when (usocket:wait-for-input socket :timeout 0.2 :ready-only t)
                      (let ((frame (handler-case (mq-backend-amqp:read-amqp-frame srv)
                                     (error () (return)))))
                        (when (= 1 (mq-backend-amqp:amqp-frame-type frame))
                          (push (mq-backend-amqp:amqp-method-name
                                 (mq-backend-amqp:decode-method
                                  (mq-backend-amqp:amqp-frame-payload frame)))
                                (car methods))))))))
      (values port thread methods))))

(deftest a-publish-blocked-past-the-blocked-timeout-fails
  (multiple-value-bind (port thread methods) (%blocking-broker)
    (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 2
                                                    :blocked-connection-timeout 1))
           (conn (mq-backend-amqp:amqp-backend-connection b)))
      (sleep 0.3)
      (let* ((start (get-internal-real-time))
             (e (handler-case (progn (mq-protocol:publish b "q" "m1") nil)
                  (mq-protocol:mq-error (e) e)))
             (waited (%seconds-since start)))
        (ok (typep e 'mq-backend-amqp:amqp-blocked-timeout)
            (format nil "amqp-blocked-timeout (got ~S: ~A)" (and e (type-of e)) e))
        (ok (< 0.7 waited 3) (format nil "after about 1 s (~,1F s)" waited))
        (ok (mq-backend-amqp:amqp-connection-closed-p conn) "the connection is closed, as in pika"))
      (close b)
      (bt2:join-thread thread)
      (ok (not (member :basic-publish (car methods))) "nothing was published"))))

(deftest a-publish-waits-for-the-unblock
  (multiple-value-bind (port thread methods) (%blocking-broker :unblock-after 0.6)
    (let ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 2
                                                   :blocked-connection-timeout 3)))
      (sleep 0.3)
      (let ((e (handler-case (progn (mq-protocol:publish b "q" "m1") nil)
                 (mq-protocol:mq-error (e) e))))
        (ok (null e) (format nil "the publish goes out once unblocked (got ~A)" e)))
      (sleep 0.5)
      (close b)
      (bt2:join-thread thread)
      (ok (member :basic-publish (car methods))
          (format nil "the broker got Basic.Publish (got ~S)" (reverse (car methods)))))))

;;; --- Write liveness counts more than 8192 unread octets -------------------

(deftest pending-input-counts-past-8192-octets
  (let ((sent (bt2:make-semaphore :name "sent")))
    (multiple-value-bind (port thread)
        (%serve-once (lambda (srv socket)
                       (declare (ignore socket))
                       (%fake-handshake srv :heartbeat 0)
                       ;; 2500 heartbeats: 20000 octets the client does not read.
                       (mq-backend-amqp:write-amqp-octets
                        (mq-backend-amqp:amqp-connection-stream srv)
                        (apply #'concatenate '(vector (unsigned-byte 8))
                               (loop repeat 2500 collect (mq-backend-amqp:encode-heartbeat))))
                       (bt2:signal-semaphore sent)
                       (sleep 2)))
      (let ((conn (mq-backend-amqp:open-amqp-connection :host "127.0.0.1" :port port :heartbeat 0)))
        (bt2:wait-on-semaphore sent :timeout 5)
        (sleep 0.3)
        (let ((n (mq-backend-amqp::%pending-input conn)))
          #+(and sbcl (not win32))
          (ok (and n (>= n 20000)) (format nil "~S unread octets counted (sent 20000)" n))
          #-(and sbcl (not win32))
          (ok t (format nil "~S (not checked here)" n)))
        (close conn :abort t)
        (bt2:join-thread thread)))))
