# mq-backend-amqp

Pure Common Lisp **AMQP 0-9-1** framing for [`mq-protocol`](https://github.com/egao1980/mq-protocol). Target: RabbitMQ. No live broker is required — encode/decode and a connection over any bidirectional octet stream (or the colocated `octet-pipe` mock).

```lisp
(asdf:load-system "mq-backend-amqp")

(let* ((encoded (mq-backend-amqp:encode-method-frame
                 :basic-publish :channel 1
                 :exchange "ex" :routing-key "rk"))
       (method (mq-backend-amqp:decode-method-frame encoded)))
  (mq-backend-amqp:amqp-method-name method))
;; => :BASIC-PUBLISH

(let* ((pipe (mq-backend-amqp:make-octet-pipe))
       (conn (mq-backend-amqp:make-amqp-connection :stream pipe))
       (backend (mq-backend-amqp:make-amqp-backend :connection conn)))
  (stack-mq:publish backend "rk"
                    (stack-mq:make-mq-message :payload "hi")))
```

Frame types: method (1), content header (2), body (3), heartbeat (8). Method table covers Connection.Start/Start-Ok, Tune/Tune-Ok, Open/Open-Ok, Channel.Open/Open-Ok, Exchange.Declare, Queue.Declare, Queue.Bind, Basic.Publish/Deliver/Ack/Nack/Consume/Cancel, Heartbeat.

Transport (usocket / `cl-stack-ssl`) and a dockerized RabbitMQ parity canary are wave-1 follow-ups.

## License

MIT — see [LICENSE](LICENSE).
