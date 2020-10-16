Teecp-rs
========

This is a toy project to port film42/teecp to rust using tokio. It is a transparent TCP proxy,
but it can also tee the proxied data to other TCP connections.

General flow for a configuration with one upstream proxy connection, and two upstream tees.

```
# Bytes from client are delivered to the upstream connection
# Bytes from the upsream connection are delivered to the client.
client -> teecp-rs -> upstream
client <- teecp-rs <- upstream

# Bytes from client are delivered to all upstream tees.
client -> teecp-rs -> upstream tee 1
client -> teecp-rs -> upstream tee 2

# Bytes from the upstream tee are dropped (captured by a sink).
sink <- teecp-rs <- upstream tee 1
sink <- teecp-rs <- upstream tee 2
```

(c) film42 + MIT License
