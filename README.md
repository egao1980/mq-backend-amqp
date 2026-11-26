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
| `:user` `:password` | `"guest"` | SASL PLAIN (UTF-8). `:password` may be a function of no arguments that returns it: a backtrace then shows the function, not the password. A password inside a `:url` string is in the caller's own frame, so with SBCL's `--disable-debugger` an unhandled connect error prints it; give the URL without it and the password this way |
| `:vhost` | `"/"` | |
| `:url` | | `amqp[s]://user:pass@host:port/vhost` (RabbitMQ URI spec: `%2f` is `/`; no path is `/`). User and password are percent-decoded: write `/ ? # @ %` in them as `%2F %3F %23 %40 %25`; errors mask the password. An IPv6 host goes in brackets and comes out without them, as in pika (`amqp://[::1]:5672/%2F` is host `"::1"`). A `+` stays a `+` (percent-decoding only, as in pika). A `quri:uri` is accepted too. Explicit keywords win |
| `:tls` `:tls-verify` | `nil` `:required` | cl+ssl client stream with SNI and hostname check |
| `:heartbeat` | broker's (`nil`) | seconds; `0` turns heartbeats off; otherwise the lower of ours and the broker's |
| `:frame-max` `:channel-max` | `131072` `0` | negotiated with the broker's Tune |
| `:channel` | `1` | |
| `:client-properties` `:locale` `:connect-timeout` | | |

`cl:close` (or leaving `with-amqp-backend`) sends `Connection.Close`, waits for `Close-Ok` and releases the socket.

**Heartbeats.** Frames of type 8 from the broker are skipped wherever a reply is read. Every backend operation calls `amqp-heartbeat-tick`, which sends a heartbeat when nothing went out for half the interval. Like pika's BlockingConnection, there is no background thread: a program that stays idle (no backend call) longer than two intervals gets dropped by the broker. Call `amqp-heartbeat-tick` from such a loop, or pass `:heartbeat 0`. While a call waits for the broker, it keeps sending heartbeats, and a broker that sends nothing at all (not even a heartbeat) for two intervals is taken to be gone: the call fails with `amqp-transport-error` and the connection is closed, instead of waiting forever (pika's heartbeat timeout). The same holds in the middle of a frame: a frame cut off for two intervals fails too, plain or TLS. A frame whose octets keep coming, over a slow link say, is waited for however long it takes, with heartbeats sent meanwhile; like pika's heartbeat checker, what counts is octets received, not whole frames. Writes are bounded the same way: a publish to a broker that stops reading (a paused container, a host gone without an RST) fails with `amqp-transport-error` once the broker takes no octets and sends none for two intervals, plain or TLS, instead of blocking in the socket write. A broker that stops reading but keeps sending (RabbitMQ under a resource alarm: `Connection.Blocked`, then heartbeats) is waited for, as in pika. With `:heartbeat 0` a call waits as long as the broker takes.

**Timeouts.** `:connect-timeout` (10 s) bounds the TCP connect, and then the TLS and AMQP handshakes together. A server that accepts and never answers fails in time, and so does one that stops after `Tune` and a port that does not speak AMQP (`amqp-transport-error`). After the handshake it is not a read timeout: the heartbeats above are. A frame larger than the negotiated `frame-max` is refused before it is read. `cl:close` waits at most 5 s for each frame of the broker's `Close-Ok`. The deadlines use SBCL's `sb-sys:with-deadline` (the socket is non-blocking, so writes wait where the deadline reaches them; on Windows plain reads wait for each octet and writes for a writable socket); on other Lisps reads and writes are not bounded.

## Operations

- `publish` → `Basic.Publish` + content header + body frames split at the negotiated `frame-max`. The destination is a queue name (default exchange) or `(exchange . routing-key)`, and it wins over the message's own exchange and topic, so a polled or delivered message published to another queue goes to that queue; only a `nil` destination reuses the message's exchange and topic; `mq-message-key` travels as `correlation-id`, `mq-message-headers` as headers. Every frame is encoded before any is written, so a message the encoder refuses (a key over 255 octets, a header value with no field-table type) is an `amqp-frame-error` with nothing sent, as in pika.
- **Every operation first reads what the broker already sent**, without waiting, as pika's BlockingConnection does on each call. It reads at most 64 deliveries: with a consumer on a queue that keeps filling, the socket is never quiet, and the operation still runs. The rest is read by the next operation, or by an RPC on its way to its reply.
  - Deliveries are kept, and heartbeats and notifications are handled.
  - A broker `Close` is signalled right away. A publish to a missing exchange makes RabbitMQ close the channel without answering anything, and the *next* publish then fails with `amqp-channel-closed` instead of going to a closed channel and vanishing.
  - A message published just before the close arrives is lost the same way it is in pika. Publisher confirms are not implemented.
- Channels:
  - A backend made with `:channel 2` on a connection opens that channel on its first operation.
  - The broker's `Channel.Close` of one channel never answers a request on another. It is recorded and reported (`amqp-channel-closed`) to the next operation on that channel, and the operation after that reopens it.
- `poll-messages destination &key n timeout` → messages already delivered to a consumer of `destination` on this connection, then `Basic.Get` (not auto-acked). Deliveries for other queues stay for polls of those queues. With `:timeout` an empty queue is retried until a message arrives or the time is up.
- `ack` / `nack` (`:requeue`, `:multiple`) → `Basic.Ack` / `Basic.Nack` of a delivered message, a copy of one, or its integer delivery tag. Delivery tags are per channel, and a channel the broker closed (404, 406, ...) is reopened with its tags starting at 1 again. So an ack never sends a tag from before the close, which would ack another message. The tags this backend hands out go on counting across a reopen: before the first close they are the broker's, after it they are the broker's plus an offset. Nothing is sent, and `amqp-stale-delivery` (an `mq-error`) is signalled, for these:
  - a delivery from a channel that closed since (reason `:channel-closed`; the broker requeued it, so it comes back);
  - one already acked or nacked, or covered by a `:multiple` one (`:settled`);
  - one from another connection or channel (`:other-connection`, `:other-channel`);
  - one from a `:no-ack` consumer (`:no-ack`).

  pika and aio-pika raise in these cases too. Restart `continue` skips the call, which returns `nil`, as the Java client's recovery silently does. A message with no delivery tag is an `mq-error`.

  `:multiple` never settles a delivery nobody has seen.
  - A multiple ack or nack of a delivery is sent with the highest unsettled tag up to it, never with one already settled (the broker would close the channel for that). It is refused (an `mq-error`) while it would also cover a delivery read but not handed out yet, one kept for a poll of another queue for instance.
  - `(ack backend 0 :multiple t)` acks every delivery handed out and still unsettled, and nothing else. It is one multiple ack up to the first delivery not handed out, then single acks for those after it. It is never wire tag 0, which would also settle deliveries still in the socket.
- `subscribe` → `Basic.Consume`, waiting for `Consume-Ok`. Deliveries that arrive while the backend waits for a reply are kept and handed out by `poll-messages` of the consumer's queue; there is no push dispatch loop yet (#5).
  - Consumer tags are channel-local: a consumer is known on its own channel until the broker cancels it or the channel closes, and its tag can then be used again.
  - `:prefetch n` sends `Basic.Qos` first. RabbitMQ then has at most n unacked deliveries out on the channel. Without it, the broker pushes the whole queue, and a consumer that is not polled holds all of it in memory.
- **Strings are UTF-8**, as AMQP 0-9-1 says: queue and exchange names, routing keys, the vhost, `correlation-id` and string headers. A queue named `café` is `63 61 66 C3 A9` on the wire, as in pika. A string from the broker that is not UTF-8 is read one character per octet rather than failing.
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

The broker's `Close` is always answered with `Close-Ok` before the condition is signalled. A call left by a non-local exit (an interrupt, a handler that unwinds) before its reply was read drops the connection, so later calls fail with `mq-connection-error` instead of reading that reply; a broker `Channel.Close` is a complete answer and keeps the connection. A `Channel.Close` of yours (through `amqp-rpc`) that crosses the broker's is answered by the broker's (`amqp-channel-closed`), and the `Close-Ok` RabbitMQ still sends for yours is skipped, as in pika.

**Broker notifications** are nobody's reply, so the call in flight keeps waiting for its own: `Connection.Blocked` / `Unblocked` set `amqp-connection-blocked`; `Basic.Cancel` (Start-Ok asks for `consumer_cancel_notify`, so RabbitMQ sends it when a consumed queue is deleted or its node fails) is answered with `Cancel-Ok` unless it says no-wait, pushes the tag on `amqp-connection-cancelled-consumers` and queues `amqp-consumer-cancelled` (not an error; pika's `on_cancel`). That condition is `signal`led once the operation that read the cancel has its reply (the outermost backend call of the thread), with no lock held and no reply in flight, so a handler may use the connection or unwind; a `poll-messages` it unwinds keeps its messages for the next poll of that queue; `Channel.Flow` is answered with `Flow-Ok` and sets `amqp-connection-flow-active`.

## Tests

`cl-repo test`. `tests/session-test.lisp` drives the handshake, heartbeats, broker `Close`, deliveries and frame splitting against a scripted broker (a two-way in-memory stream). Live tests skip without RabbitMQ:

```sh
docker run -d --name rabbitmq-test -p 127.0.0.1:15673:5672 rabbitmq:3.13.7-alpine
```

`live-amqps` also wants `MQ_AMQP_TEST_TLS_PORT` and `MQ_AMQP_TEST_CA` (a RabbitMQ with an SSL listener; see the test file).

## License

MIT — see [LICENSE](LICENSE).
