(in-package #:mq-backend-amqp/tests)

;;; Heartbeats both ways while a reply is awaited. A broker that sends
;;; nothing at all for two heartbeat intervals is gone (AMQP 0-9-1 4.2.7):
;;; the call in flight fails with AMQP-TRANSPORT-ERROR instead of waiting
;;; forever, as pika's heartbeat timeout does. A slow broker that still
;;; sends heartbeats is waited for. The broker here is a thread serving one
;;; client on a real 127.0.0.1 socket, so these run without RabbitMQ.

(defun %client-method (srv &optional heartbeats)
  "The next method the client sent to the fake broker connection SRV,
skipping heartbeat frames (counted in the cons HEARTBEATS)."
  (loop for frame = (mq-backend-amqp:read-amqp-frame srv)
        do (if (mq-backend-amqp:heartbeat-frame-p frame)
               (when heartbeats (incf (car heartbeats)))
               (return (mq-backend-amqp:amqp-method-name
                        (mq-backend-amqp:decode-method (mq-backend-amqp:amqp-frame-payload frame)))))))

(defun %send (srv &rest specs)
  (mq-backend-amqp:write-amqp-octets (mq-backend-amqp:amqp-connection-stream srv)
                                     (apply #'%frames specs)))

(defun %fake-handshake (srv &key (heartbeat 1))
  "Serve the handshake to the client on SRV, proposing HEARTBEAT seconds."
  (mq-backend-amqp:read-amqp-octets (mq-backend-amqp:amqp-connection-stream srv) 8)
  (%send srv (first (%broker-hello)))
  (%client-method srv)                  ; Start-Ok
  (%send srv (list :connection-tune 0 :channel-max 2047 :frame-max 131072 :heartbeat heartbeat))
  (%client-method srv)                  ; Tune-Ok
  (%client-method srv)                  ; Connection.Open
  (%send srv '(:connection-open-ok 0 :reserved-1 ""))
  (%client-method srv)                  ; Channel.Open
  (%send srv '(:channel-open-ok 1 :reserved-1 "")))

(defun %serve-once (script)
  "A broker on 127.0.0.1 for one client: SCRIPT runs in a thread with an
AMQP-CONNECTION over the accepted socket's stream and the socket itself;
the socket is closed when it returns or fails. → (values port thread)."
  (let* ((listener (usocket:socket-listen "127.0.0.1" 0 :reuse-address t
                                                        :element-type '(unsigned-byte 8)))
         (port (usocket:get-local-port listener)))
    (values port
            (bt2:make-thread
             (lambda ()
               (unwind-protect
                    (let ((socket (usocket:socket-accept listener)))
                      (unwind-protect
                           (handler-case
                               (funcall script
                                        (mq-backend-amqp:make-amqp-connection
                                         :stream (usocket:socket-stream socket))
                                        socket)
                             (error () nil))
                        (ignore-errors (usocket:socket-close socket))))
                 (ignore-errors (usocket:socket-close listener))))
             :name "fake amqp broker"))))

(defun %seconds-since (start)
  (/ (- (get-internal-real-time) start) internal-time-units-per-second))

(deftest a-silent-broker-fails-the-call-after-two-heartbeat-intervals
  (let ((heartbeats (list 0)))
    (multiple-value-bind (port thread)
        (%serve-once
         (lambda (srv socket)
           (%fake-handshake srv :heartbeat 1)
           (%client-method srv heartbeats) ; Queue.Declare, never answered
           ;; Silent: read (and count) whatever the client sends, for 8 s
           ;; at most, until it hangs up.
           (loop with start = (get-internal-real-time)
                 while (< (%seconds-since start) 8)
                 do (when (usocket:wait-for-input socket :timeout 0.2 :ready-only t)
                      (let ((frame (handler-case (mq-backend-amqp:read-amqp-frame srv)
                                     (error () (return)))))
                        (when (mq-backend-amqp:heartbeat-frame-p frame)
                          (incf (car heartbeats))))))))
      (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 1))
             (conn (mq-backend-amqp:amqp-backend-connection b))
             (start (get-internal-real-time))
             (e (handler-case (progn (mq-backend-amqp:amqp-declare-queue b "q") nil)
                  (mq-protocol:mq-error (e) e)))
             (waited (%seconds-since start)))
        (ok (= 1 (mq-backend-amqp:amqp-connection-heartbeat conn)) "heartbeat 1 s negotiated")
        (ok (typep e 'mq-backend-amqp:amqp-transport-error)
            (format nil "the Queue.Declare fails with amqp-transport-error (got ~S: ~A)"
                    (and e (type-of e)) e))
        (ok (and e (search "heartbeat" (princ-to-string e))) "the report names the heartbeats")
        (ok (< 1.5 waited 4) (format nil "after about two intervals (~,1F s)" waited))
        (ok (mq-backend-amqp:amqp-connection-closed-p conn) "the connection is closed")
        (ok (signals (mq-backend-amqp:amqp-declare-queue b "q") 'mq-protocol:mq-connection-error)
            "later calls fail at once")
        (close b)
        (bt2:join-thread thread)
        (ok (<= 2 (car heartbeats))
            (format nil "the client kept sending heartbeats while it waited (~D)" (car heartbeats)))))))

(deftest a-slow-broker-that-sends-heartbeats-is-waited-for
  (multiple-value-bind (port thread)
      (%serve-once
       (lambda (srv socket)
         (declare (ignore socket))
         (%fake-handshake srv :heartbeat 1)
         (%client-method srv)            ; Queue.Declare
         ;; 3 s, more than two intervals, before the reply; heartbeats meanwhile.
         (dotimes (i 6)
           (sleep 0.5)
           (%send srv :heartbeat))
         (%send srv (%declare-ok "slow"))
         (loop until (eq :connection-close (%client-method srv)))
         (%send srv '(:connection-close-ok 0))))
    (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 1))
           (got (handler-case (mq-backend-amqp:amqp-declare-queue b "slow")
                  (mq-protocol:mq-error (e) (list :error (princ-to-string e))))))
      (ok (equal "slow" got) (format nil "the reply after 3 s arrives (got ~S)" got))
      (ok (close b) "and the connection closes cleanly")
      (bt2:join-thread thread))))

(deftest without-heartbeats-there-is-no-deadline
  (multiple-value-bind (port thread)
      (%serve-once
       (lambda (srv socket)
         (declare (ignore socket))
         (%fake-handshake srv :heartbeat 0)
         (%client-method srv)
         (sleep 1.5)
         (%send srv (%declare-ok "late"))
         (loop until (eq :connection-close (%client-method srv)))
         (%send srv '(:connection-close-ok 0))))
    (let* ((b (mq-backend-amqp:connect-amqp-backend :host "127.0.0.1" :port port :heartbeat 0))
           (got (handler-case (mq-backend-amqp:amqp-declare-queue b "late")
                  (mq-protocol:mq-error (e) (list :error (princ-to-string e))))))
      (ok (equal "late" got) (format nil "heartbeat 0 waits for the reply (got ~S)" got))
      (close b)
      (bt2:join-thread thread))))

(deftest live-amqps-heartbeats-and-a-large-payload
  ;; Under TLS the wait for a frame polls the TLS stream, never the socket
  ;; stream underneath (whose LISTEN would take OpenSSL's octets).
  (let ((port (%tls-env)))
    (if (null port)
        (skip "MQ_AMQP_TEST_TLS_PORT / MQ_AMQP_TEST_CA not set (see tests/session-test.lisp)")
        (mq-backend-amqp:with-amqp-backend
            (b :url (format nil "amqps://~A:~A@127.0.0.1:~D" (%user) (%password) port)
               :heartbeat 2)
          (let* ((q (mq-backend-amqp:amqp-declare-queue b "" :exclusive t :auto-delete t))
                 (big (make-array 400000 :element-type '(unsigned-byte 8)
                                         :initial-element 7)))
            (sleep 3)                   ; idle, but under two intervals
            (mq-protocol:publish b q (mq-protocol:make-mq-message :payload "after-idle"))
            (mq-protocol:publish b q (mq-protocol:make-mq-message :payload big))
            (let ((got (mq-protocol:poll-messages b q :n 2 :timeout 3)))
              (ok (equal "after-idle" (and got (%text (first got))))
                  "a message after an idle gap over TLS")
              (ok (equalp big (and (second got) (mq-protocol:mq-message-payload (second got))))
                  "and a 400 000-byte one, read across many TLS records")
              (dolist (m got) (mq-protocol:ack b m))))))))
