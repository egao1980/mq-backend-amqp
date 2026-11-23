# mq-backend-amqp

Pure Common Lisp **AMQP 0-9-1** backend for [`mq-protocol`](https://github.com/egao1980/mq-protocol). Target: RabbitMQ. It connects by itself: TCP through usocket, TLS through [`cl-stack-ssl`](https://github.com/egao1980/cl-stack-ssl) (cl+ssl), the protocol header, `Start-Ok` with SASL PLAIN, `Tune-Ok`, `Open`, `Channel.Open`, heartbeats, and `Connection.Close` on the way out. Framing works on any bidirectional octet stream (or the colocated `octet-pipe`), so the unit tests need no broker.

```lisp
(asdf:load-system "mq-backend-amqp")

;; pika: BlockingConnection(ConnectionParameters("127.0.0.1", 5672, "/", PlainCredentials("guest", "guest")))
(mq-backend-amqp:with-amqp-backend (b :host "127.0.0.1" :port 5672 :user "guest" :password "guest")
  (mq-backend-amqp:amqp-declare-queue b "orders" :durable t)
  (stack-mq:publish b "orders" (stack-mq:make-mq-message :payload "order-1"))
  (dolist (m (stack-mq:poll-messages b "orders" :timeout 2))
    (stack-mq:ack b m)))

;; pika URLParameters("amqps://app:secret@mq.example.com/prod")
(mq-backend-amqp:with-amqp-backend (b :url "amqps://app:secret@mq.example.com/prod")
  ...)
```

## Connecting

`connect-amqp-backend` / `with-amqp-backend` / `open-amqp-connection` take:

| keyword | default | |
|---|---|---|
| `:host` `:port` | `"127.0.0.1"`, 5672 (5671 with TLS) | |
| `:user` `:password` | `"guest"` | SASL PLAIN (UTF-8) |
| `:vhost` | `"/"` | |
| `:url` | | `amqp[s]://user:pass@host:port/vhost` (RabbitMQ URI spec: `%2f` is `/`; no path is `/`). Explicit keywords win |
| `:tls` `:tls-verify` | `nil` `:required` | cl+ssl client stream with SNI and hostname check |
| `:heartbeat` | broker's (`nil`) | seconds; `0` turns heartbeats off; otherwise the lower of ours and the broker's |
| `:frame-max` `:channel-max` | `131072` `0` | negotiated with the broker's Tune |
| `:channel` | `1` | |
| `:client-properties` `:locale` `:connect-timeout` | | |

`cl:close` (or leaving `with-amqp-backend`) sends `Connection.Close`, waits for `Close-Ok` and releases the socket.

**Heartbeats.** Frames of type 8 from the broker are skipped wherever a reply is read. Every backend operation calls `amqp-heartbeat-tick`, which sends a heartbeat when nothing went out for half the interval. Like pika's BlockingConnection, there is no background thread: a program that stays idle (no backend call) longer than two intervals gets dropped by the broker. Call `amqp-heartbeat-tick` from such a loop, or pass `:heartbeat 0`.

## Operations

- `publish` → `Basic.Publish` + content header + body frames split at the negotiated `frame-max`. The destination is a queue name (default exchange) or `(exchange . routing-key)`; `mq-message-key` travels as `correlation-id`, `mq-message-headers` as headers.
- `poll-messages destination &key n timeout` → `Basic.Get` (not auto-acked). With `:timeout` an empty queue is retried until a message arrives or the time is up.
- `ack` / `nack` (`:requeue`) → `Basic.Ack` / `Basic.Nack` by delivery tag.
- `subscribe` → `Basic.Consume`, waiting for `Consume-Ok`. Deliveries that arrive while the backend waits for a reply are kept and handed out by `poll-messages`; there is no push dispatch loop yet.
- `amqp-declare-queue` (→ name, message count, consumer count), `amqp-declare-exchange`, `amqp-bind-queue`: synchronous, waiting for their `-Ok`.
- `amqp-rpc connection method replies &rest fields` for anything else in the method table.

The method table covers Connection, Channel, Exchange, Queue, Basic, Confirm and Tx of 0-9-1, with RabbitMQ's extensions (`exchange.bind`, `connection.blocked`), every `-Ok` reply included.

## Errors

| situation | condition (all `mq-error`) |
|---|---|
| refused / timed out connect, dropped socket, TLS failure | `amqp-transport-error` (`mq-connection-error`), the usocket / stream / cl+ssl condition in `cause` |
| broker `Connection.Close`: bad login (403), unknown vhost (530), forced (320) | `amqp-connection-closed` (`mq-connection-error`) with `amqp-reply-code`, `amqp-reply-text`, `amqp-failing-class-id` / `-method-id` |
| broker `Channel.Close`: missing queue (404), precondition (406), access (403) | `amqp-channel-closed` with the same readers. The connection stays up and the next operation opens the channel again |
| malformed frames | `amqp-frame-error` |

The broker's `Close` is always answered with `Close-Ok` before the condition is signalled.

## Tests

`cl-repo test`. `tests/session-test.lisp` drives the handshake, heartbeats, broker `Close`, deliveries and frame splitting against a scripted broker (a two-way in-memory stream). Live tests skip without RabbitMQ:

```sh
docker run -d --name rabbitmq-test -p 127.0.0.1:15673:5672 rabbitmq:3.13.7-alpine
```

`live-amqps` also wants `MQ_AMQP_TEST_TLS_PORT` and `MQ_AMQP_TEST_CA` (a RabbitMQ with an SSL listener; see the test file).

## License

MIT — see [LICENSE](LICENSE).
