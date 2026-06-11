# Oshima

**⚠️ WARNING:** Experimental and vibecoded – untested, unstable, not for production. 

A runtime-agnostic actor framework for Rust.

Define your actors once. Choose the runtime at the call site — no actor
code ever changes.

---

## Workspace layout

```
oshima/
├── oshima-core      # Core traits only — no_std compatible, zero runtime deps
├── oshima-sync      # Synchronous backend  (one OS thread per actor)
├── oshima-tokio     # Async backend        (one Tokio task per actor)
└── oshima-macros    # Reserved for future derive macros
```

---

## The design principle

Actor code (`Actor`, `Handler`, `Message` impls) lives in a crate that
depends only on `oshima-core`. The runtime backend is a **separate
dependency** selected by Cargo features. Swapping from sync to Tokio is a
one-line change in `Cargo.toml`.

```
┌──────────────────────────────────────────┐
│  Your actor code  (oshima-core only)     │
│  struct MyActor; impl Handler<C, Msg>    │
├──────────────────────────────────────────┤
│  Context / Addr abstraction  ← SEAM      │
│  ActorBase, ActorContext<A>, Handler<C,M>│
├──────────────┬───────────────────────────┤
│ oshima-sync  │  oshima-tokio             │
│ SyncRuntime  │  TokioRuntime             │
│ SyncContext  │  TokioContext             │
│ SyncAddr<A>  │  TokioAddr<A>             │
└──────────────┴───────────────────────────┘
```

---

## Quick start

Add to `Cargo.toml`:

```toml
# Synchronous (thread-per-actor):
oshima-core = { path = "oshima-core" }
oshima-sync  = { path = "oshima-sync" }

# OR async (task-per-actor):
oshima-core  = { path = "oshima-core" }
oshima-tokio = { path = "oshima-tokio" }
```

---

## Defining actors (backend-agnostic)

```rust
use oshima_core::prelude::*;

// ── Messages ──────────────────────────────────────────────────────────────

struct Add(u32);
impl Message for Add { type Result = u32; }

struct Reset;
impl Message for Reset { type Result = (); }

struct GetValue;
impl Message for GetValue { type Result = u32; }

// ── Actor ─────────────────────────────────────────────────────────────────

struct Counter { value: u32 }

// ActorBase is a required marker (breaks a circular trait bound).
impl ActorBase for Counter {}

// Actor<C> is generic over any context — works for every backend.
impl<C: ActorContext<Self>> Actor<C> for Counter {
    fn started(&mut self, _ctx: &mut C) {
        println!("Counter started at {}", self.value);
    }
    fn stopped(&mut self, _ctx: &mut C) {
        println!("Counter stopped at {}", self.value);
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────

impl<C: ActorContext<Self>> Handler<C, Add> for Counter {
    fn handle(&mut self, msg: Add, _ctx: &mut C) -> u32 {
        self.value += msg.0;
        self.value
    }
}

impl<C: ActorContext<Self>> Handler<C, Reset> for Counter {
    fn handle(&mut self, _msg: Reset, _ctx: &mut C) { self.value = 0; }
}

impl<C: ActorContext<Self>> Handler<C, GetValue> for Counter {
    fn handle(&mut self, _msg: GetValue, _ctx: &mut C) -> u32 { self.value }
}
```

This code compiles and runs identically on both backends — nothing changes.

---

## Using the sync backend

```rust
use oshima_sync::prelude::*;

fn main() {
    // Spawn returns a cloneable SyncAddr<Counter>
    let addr = SyncRuntime::spawn(Counter { value: 0 });

    // send() blocks the calling thread and returns the result
    let v = addr.send(Add(10)).unwrap();
    assert_eq!(v, 10);

    // do_send() is fire-and-forget (non-blocking)
    addr.do_send(Add(5)).unwrap();

    let v = addr.send(GetValue).unwrap();
    assert_eq!(v, 15);

    // Clone the address — both reach the same actor
    let addr2 = addr.clone();
    addr2.send(Reset).unwrap();
    assert_eq!(addr.send(GetValue).unwrap(), 0);
}
```

---

## Using the Tokio backend

```rust
use oshima_tokio::prelude::*;

#[tokio::main]
async fn main() {
    // Spawn returns a cloneable TokioAddr<Counter>
    let addr = TokioRuntime::spawn(Counter { value: 0 });

    // send() suspends the current task (non-blocking) and awaits the reply
    let v = addr.send(Add(10)).await.unwrap();
    assert_eq!(v, 10);

    // do_send() is async fire-and-forget
    addr.do_send(Add(5)).await.unwrap();

    // try_send() is fully non-async, non-blocking fire-and-forget
    addr.try_send(Reset).unwrap();

    let v = addr.send(GetValue).await.unwrap();
    assert_eq!(v, 0);
}
```

---

## Lifecycle hooks

Every actor gets three lifecycle hooks, all optional:

```rust
impl<C: ActorContext<Self>> Actor<C> for MyActor {
    /// Called once before the first message. Good for initialization.
    fn started(&mut self, _ctx: &mut C) {}

    /// Called when the actor is about to stop.
    /// Return Running::Continue to veto the shutdown.
    fn stopping(&mut self, _ctx: &mut C) -> Running {
        Running::Stop
    }

    /// Called after fully stopped. Good for cleanup / final logging.
    fn stopped(&mut self, _ctx: &mut C) {}
}
```

An actor can also stop itself at any time from inside a handler:

```rust
impl<C: ActorContext<Self>> Handler<C, Shutdown> for MyActor {
    fn handle(&mut self, _msg: Shutdown, ctx: &mut C) {
        ctx.stop(); // graceful self-shutdown
    }
}
```

---

## Address API comparison

| Method | Sync (`SyncAddr`) | Tokio (`TokioAddr`) |
|---|---|---|
| Send + wait for result | `addr.send(msg)` — blocks thread | `addr.send(msg).await` — suspends task |
| Fire and forget | `addr.do_send(msg)` — non-blocking | `addr.do_send(msg).await` — async |
| Non-blocking instant | — | `addr.try_send(msg)` — sync, no await |

All address types are `Clone + Send` and can be shared freely across threads or tasks.

---

## Error handling

Both `send` and `do_send` return `Result<_, SendError>`:

```rust
match addr.send(GetValue) {
    Ok(v)                        => println!("value: {v}"),
    Err(SendError::ActorStopped) => println!("actor is gone"),
    Err(SendError::MailboxFull)  => println!("mailbox full, retry later"),
}
```

---

## Adding a new runtime backend

Implement three types in a new crate:

1. `MyContext<A>` — implement `ActorContext<A> for MyContext<A>`
2. `MyAddr<A>` — wraps a channel sender, exposes `send` / `do_send`
3. `MyRuntime` — exposes `spawn(actor) -> MyAddr<A>`

Actor code requires zero changes.

---

## no_std status

`oshima-core` is `no_std` compatible (disable the default `std` feature).
`oshima-sync` and `oshima-tokio` require `std`.

For bare-metal targets, implement a custom backend against `oshima-core`
using static channels (e.g. Embassy's `Channel<_, M, N>`) and a cooperative
or interrupt-driven executor.
