# I/O multiplexing with epoll in Go
A minimal TCP echo server built from scratch in Go, exploring the same I/O model that makes Redis fast: **epoll-based non-blocking I/O** on a single thread.

## How it works

Instead of spawning a goroutine per connection (the standard Go net approach), this server uses the Linux `epoll` syscall directly:

1. A single server loop calls `epoll_wait`, which blocks until one or more file descriptors are ready
2. New connections on the server FD are `Accept`-ed and registered with epoll
3. Readable client FDs are read and echoed back — all in the same goroutine, no blocking

This means thousands of concurrent connections are handled with a single OS thread, no context switching.

```
client 1 ──┐
client 2 ──┼──► epoll_wait ──► read/write loop
client N ──┘
```

### Key config (`server/conf.go`)

| Constant     | Value | Meaning                              |
|-------------|-------|--------------------------------------|
| `port`       | 7474  | TCP port the server listens on       |
| `backlog`    | 128   | Max pending connections in accept queue |
| `epollBatch` | 100   | Max events processed per epoll_wait  |

## Build & run

**The server uses Linux-only syscalls (`epoll`). Cross-compile from Mac:**

```bash
GOOS=linux GOARCH=amd64 go build -buildvcs=false -o redis-clone ./server/...
```

**On Linux directly:**

```bash
go build -o redis-clone ./server/...
./redis-clone
```

**In a Docker container:**

```bash
# copy binary into a running container
docker cp redis-clone <container>:/redis-clone
docker exec -it <container> /redis-clone
```

Make sure port `7474` is forwarded if running inside a container (`-p 7474:7474`).

## Test it

Connect with netcat:

```bash
nc localhost 7474
hello
hello        # echoed back
```

Test concurrent connections — open multiple terminals:

```bash
nc localhost 7474 &
nc localhost 7474 &
nc localhost 7474 &
```

All three connect simultaneously without blocking each other.

## Architecture comparison

| | `cmd/` (stdlib) | `server/` (this) |
|---|---|---|
| Concurrency | goroutine per conn | single goroutine, epoll |
| Syscall | `net.Listen` / `net.Conn` | raw `socket`, `bind`, `listen` |
| I/O model | blocking per conn | non-blocking, event-driven |
| Platform | cross-platform | Linux only |

The `cmd/` directory contains the original stdlib version as a reference.
