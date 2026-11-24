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
| `:host` `:port` | `"127.0.0.1"`, 5672 (5671 with TLS) | an IPv6 address with or without brackets (`"::1"`, `"[::1]"`) |
| `:user` `:password` | `"guest"` | SASL PLAIN (UTF-8) |
| `:vhost` | `"/"` | |
| `:url` | | `amqp[s]://user:pass@host:port/vhost` (RabbitMQ URI spec: `%2f` is `/`; no path is `/`). User and password are percent-decoded: write `/ ? # @ %` in them as `%2F %3F %23 %40 %25`; errors mask the password. An IPv6 host goes in brackets and comes out without them, as in pika (`amqp://[::1]:5672/%2F` is host `"::1"`). Explicit keywords win |
| `:tls` `:tls-verify` | `nil` `:required` | cl+ssl client stream with SNI and hostname check |
| `:heartbeat` | broker's (`nil`) | seconds; `0` turns heartbeats off; otherwise the lower of ours and the broker's |
| `:frame-max` `:channel-max` | `131072` `0` | negotiated with the broker's Tune |
| `:channel` | `1` | |
| `:client-properties` `:locale` `:connect-timeout` | | |

`cl:close` (or leaving `with-amqp-backend`) sends `Connection.Close`, waits for `Close-Ok` and releases the socket.

**Heartbeats.** Frames of type 8 from the broker are skipped wherever a reply is read. Every backend operation calls `amqp-heartbeat-tick`, which sends a heartbeat when nothing went out for half the interval. Like pika's BlockingConnection, there is no background thread: a program that stays idle (no backend call) longer than two intervals gets dropped by the broker. Call `amqp-heartbeat-tick` from such a loop, or pass `:heartbeat 0`. While a call waits for the broker, it keeps sending heartbeats, and a broker that sends nothing at all (not even a heartbeat) for two intervals is taken to be gone: the call fails with `amqp-transport-error` and the connection is closed, instead of waiting forever (pika's heartbeat timeout). With `:heartbeat 0` a call waits as long as the broker takes.

## Operations

- `publish` → `Basic.Publish` + content header + body frames split at the negotiated `frame-max`. The destination is a queue name (default exchange) or `(exchange . routing-key)`; `mq-message-key` travels as `correlation-id`, `mq-message-headers` as headers.
- `poll-messages destination &key n timeout` → messages already delivered to a consumer of `destination` on this connection, then `Basic.Get` (not auto-acked). Deliveries for other queues stay for polls of those queues. With `:timeout` an empty queue is retried until a message arrives or the time is up.
- `ack` / `nack` (`:requeue`, `:multiple`) → `Basic.Ack` / `Basic.Nack` of a delivered message, a copy of one, or its integer delivery tag. Delivery tags are per channel, and a channel the broker closed (404, 406, ...) is reopened with its tags starting at 1 again. So an ack never sends a tag from before the close, which would ack another message. The tags this backend hands out go on counting across a reopen: before the first close they are the broker's, after it they are the broker's plus an offset. Nothing is sent, and `amqp-stale-delivery` (an `mq-error`) is signalled, for these:
  - a delivery from a channel that closed since (reason `:channel-closed`; the broker requeued it, so it comes back);
  - one already acked or nacked, or covered by a `:multiple` one (`:settled`);
  - one from another connection (`:other-connection`);
  - one from a `:no-ack` consumer (`:no-ack`).

  pika and aio-pika raise in these cases too. Restart `continue` skips the call, which returns `nil`, as the Java client's recovery silently does. A message with no delivery tag is an `mq-error`. So is a `:multiple` ack or nack that would also settle deliveries not handed out yet: it would ack them unseen.
- `subscribe` → `Basic.Consume`, waiting for `Consume-Ok`. Deliveries that arrive while the backend waits for a reply are kept and handed out by `poll-messages`; there is no push dispatch loop yet.
- `amqp-declare-queue` (→ name, message count, consumer count), `amqp-declare-exchange`, `amqp-bind-queue`: synchronous, waiting for their `-Ok`.
- `amqp-rpc connection method replies &rest fields` for anything else in the method table.

The method table covers Connection, Channel, Exchange, Queue, Basic, Confirm and Tx of 0-9-1, with RabbitMQ's extensions (`exchange.bind`, `connection.blocked`), every `-Ok` reply included. Field tables (headers, arguments) carry every RabbitMQ type, including `f` / `d` (single- / double-float) and `D` (decimal, read as a rational: pika's `Decimal("1.23")` is `123/100`); an unknown type octet is an `amqp-frame-error`.

## Errors

| situation | condition (all `mq-error`) |
|---|---|
| refused / timed out connect, dropped socket, TLS failure | `amqp-transport-error` (`mq-connection-error`), the usocket / stream / cl+ssl condition in `cause` |
| nothing from the broker for two heartbeat intervals | `amqp-transport-error`; the connection is closed |
| `ack` / `nack` of a delivery not outstanding on the open channel | `amqp-stale-delivery` with `amqp-stale-delivery-tag` and `-reason`; nothing is sent; restart `continue` |
| broker `Connection.Close`: bad login (403), unknown vhost (530), forced (320) | `amqp-connection-closed` (`mq-connection-error`) with `amqp-reply-code`, `amqp-reply-text`, `amqp-failing-class-id` / `-method-id` |
| broker `Channel.Close`: missing queue (404), precondition (406), access (403) | `amqp-channel-closed` with the same readers. The connection stays up and the next operation opens the channel again |
| malformed frames | `amqp-frame-error` |
| a frame no request waits for (505 UNEXPECTED_FRAME) | `amqp-frame-error`; the client sends `Connection.Close 505` and drops the socket, so later calls get `mq-connection-error` instead of the wrong reply |
| any call on a closed connection | `mq-connection-error` |

The broker's `Close` is always answered with `Close-Ok` before the condition is signalled. A call left by a non-local exit (an interrupt, a handler that unwinds) before its reply was read drops the connection, so later calls fail with `mq-connection-error` instead of reading that reply; a broker `Channel.Close` is a complete answer and keeps the connection.

**Broker notifications** are nobody's reply, so the call in flight keeps waiting for its own: `Connection.Blocked` / `Unblocked` set `amqp-connection-blocked`; `Basic.Cancel` (Start-Ok asks for `consumer_cancel_notify`, so RabbitMQ sends it when a consumed queue is deleted or its node fails) is answered with `Cancel-Ok` unless it says no-wait, pushes the tag on `amqp-connection-cancelled-consumers` and queues `amqp-consumer-cancelled` (not an error; pika's `on_cancel`). That condition is `signal`led once the operation that read the cancel has its reply (the outermost backend call of the thread), with no lock held and no reply in flight, so a handler may use the connection or unwind; a `poll-messages` it unwinds keeps its messages for the next poll of that queue; `Channel.Flow` is answered with `Flow-Ok` and sets `amqp-connection-flow-active`.

## Tests

`cl-repo test`. `tests/session-test.lisp` drives the handshake, heartbeats, broker `Close`, deliveries and frame splitting against a scripted broker (a two-way in-memory stream). Live tests skip without RabbitMQ:

```sh
docker run -d --name rabbitmq-test -p 127.0.0.1:15673:5672 rabbitmq:3.13.7-alpine
```

`live-amqps` also wants `MQ_AMQP_TEST_TLS_PORT` and `MQ_AMQP_TEST_CA` (a RabbitMQ with an SSL listener; see the test file).

## License

MIT — see [LICENSE](LICENSE).
