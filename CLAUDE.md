# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

`github.com/floatdrop/grpcproc` gives goroutines Erlang-style network
transparency over gRPC: processes with
typed mailboxes, `Send`, `Call`, `Monitor`, `Link` and `Exit`, the same
whether the target is in this binary or on another node. Go 1.27+, built on
generic methods; the core depends on gRPC, protobuf and
[fsm](https://github.com/floatdrop/fsm) only.
[`docs/DESIGN.md`](docs/DESIGN.md) is the model — the wire protocol, links,
sessions, admission, observability, and why each choice was made over what
other frameworks do. This file is the working detail behind it; the two are
edited together.

Core: `node.go` (`Config`, `Node`, `NewNode`, `Start`, `Stop`, `Disconnect`,
discovery interfaces, `dispatch`, `Query`), `link.go` (outbound and inbound links,
dialing and backoff, the handshake, sessions: `supersede`, `admit`,
`outLost`, `inLost`, `takeLinks`), `process.go` (`Process`, `Msg`, spawn
options, `WithInspect` and `WithQuery` among them, mailbox, monitors and
links), `addr.go` (`Addr[M]`, typed `Send` and
`Call`, `Caller`, metadata), `pid.go` (`PID`, `Name`, `Ref`, `Down`, the
error types), `queue.go` (the mailbox and link queue), `admit.go` (`Policy`,
`Export`), `names.go` (global names), `hooks.go`, `events.go`, `info.go`
(the inspection types). Packages of the core module: `grpcproctest`
(clusters over bufconn), `inspect` (the Inspector service), `actor`
(handler loop, supervisor, `Workers`), `pubsub`, `cron`, `leader`, `saga`
(durable sagas behind a `Store`; `saga/sagatest` checks a store). Nested modules:
`otel`, `etcd`, `tools`, `saga/postgres` (released), `examples`, `benchmarks`
(not). `site/`
is the documentation site. Protos are under `proto/` and beside the packages
that own them.

## Working rules

- **Target modern Go.** Invoke the `modern-go-guidelines:use-modern-go` skill
  before writing Go here, and follow it: prefer `slices`, `maps`, `cmp`,
  range-over-func, and the rest of what Go 1.27 offers over legacy patterns.
  (If the skill is not listed, install it:
  `/plugin install modern-go-guidelines@goland-claude-marketplace`.)
- **Never reimplement the standard library.** No hand-rolled `max`/`min`,
  `slices.Contains`, `slices.SortFunc`, `maps.Keys`, `cmp.Or`, `errors.Join`,
  `sync.OnceValue`. If a builtin or stdlib function does it, call it.
- **Comments are short.** State what the code does or which invariant it
  carries, in one line where possible. Do not narrate the change, justify the
  edit, compare with the previous version, or record history — the deep
  rationale belongs in `docs/DESIGN.md` and in commit messages.
- **Review every change before committing it.** Run the full gate below,
  then run the `code-review` skill over the diff, uncommitted changes
  included, and act on its findings; re-run it after a large follow-up
  change. A change is not finished, and not committed, until both pass.
  Only a trivial edit to prose or a test may skip the review. Reviews here
  have caught real bugs the gate passed — a pre-candidate ignoring a
  hand-over, README claims the code contradicted — and skipping them was
  the gap this rule closes.
- **Check behaviour claims against the code.** A README or DESIGN sentence
  about what the library does is a claim a reviewer verifies line by line;
  write it as one.
- **Before 1.0, prefer the cleaner API.** A minor release may break the API
  or the node protocol (see `RELEASING.md`); do not keep a worse design for
  compatibility. The release notes say how to move.
- **Before pushing:** fetch `origin/main` and rebase on it, then run the
  whole gate of every module the change touches, `gofmt -l` included. Fetch
  before starting a change that spans many files, too.

## Commands

```sh
go test -race -count=1 ./...                         # the suite; always with -race
go test -race -count=1 -run '^TestCallMonitorAcrossABrokenLink$' .   # one test
go vet ./... && golangci-lint run ./...              # lint (config in .golangci.yml)
test -z "$(gofmt -l .)"                              # formatting gate
go run github.com/campoy/embedmd@v1.0.0 -d README.md # README in sync with examples/?
go run github.com/campoy/embedmd@v1.0.0 -w README.md # re-embed after editing an example
buf lint && buf generate                             # after editing a .proto
go test -run '^$' -fuzz '^FuzzScenario$' -fuzztime 5m .        # FuzzDispatch, FuzzServeLink too
go test -run '^$' -fuzz '^FuzzParse$' -fuzztime 5m ./cron
go test -race -count=25 -cpu=1,2,4,8 -timeout=80m ./...  # the nightly stress, for a concurrency change
go test -count=1 -run 'Machines|Diagrams' ./leader -update   # leader's diagrams after a machine change
(cd examples && go test ./quickstart ./actors ./supervisor ./blockingio ./pubsub ./guide -update)  # pinned output
scripts/release.sh -n v0.7.0                         # a release plan; see RELEASING.md
```

The full gate, as CI runs it — run it before committing. In the root,
where `gofmt -l .` also walks every nested module:

```sh
test -z "$(gofmt -l .)" && go vet ./... && golangci-lint run ./... \
  && go test -race -count=1 ./... \
  && go run github.com/campoy/embedmd@v1.0.0 -d README.md
```

In `examples`, and in `otel`, `etcd`, `tools` and `saga/postgres` when touched,
`go vet ./... && golangci-lint run ./... && go test -race -count=1 ./...`.
In `benchmarks`, which builds against the working tree, so any change to
the core's API can break it, `go vet ./... && golangci-lint run ./... &&
go test -run '^$' -bench . -benchtime=50ms ./...`.

Then coverage, which must stay at 100% for each package CI lists — in the
root `. ./inspect ./actor ./pubsub ./cron ./leader ./saga`, in `otel`, `etcd` and
`saga/postgres` `.`, in `tools` `./client ./dot ./cli ./mcpserver ./web`:

```sh
for pkg in . ./inspect ./actor ./pubsub ./cron ./leader ./saga; do
  go test -count=1 -coverprofile=coverage.out -coverpkg=$pkg $pkg >/dev/null
  echo "$pkg $(go tool cover -func=coverage.out | awk '/^total:/ {print $3}')"
done
```

## Architecture

The pieces below only make sense together; `docs/DESIGN.md` has the long
form of each.

**One link per direction, and the inbound one ending is the node-down
signal.** A node sends to a peer over the client stream it opened
(`outLink`) and receives over the one the peer opened (`inLink`), so two
nodes never race to dial one bidirectional stream. Everything between them —
messages, calls, replies, `Down`s — rides those links in order, which is
what gives "a process's last message arrives before its `Down`". An
outbound failure alone only drops that link (`outLost`); the peer is
declared down once its inbound link has ended, everything it sent then
dispatched, or when there is none (`inLost`, `peerDown`). A per-link lock
held while dispatching and while closing means nothing from a closed link
is dispatched after its `Down{noconnection}`.

**Sessions are counted on both ends.** `Node.epochs` counts, per peer, the
sessions with it that ended. The dialer sends its count in metadata
(`grpcproc-session`); the server answers in its `Hello` with the larger of
its own and the dialer's, its own taken as 0 for an incarnation it has not
met before. A node hearing a larger count than its own ends the session it
thought was current and *takes* the count, without counting one more, so
the two agree, and a restarted peer's count is taken whatever it is. The
`Hello` is never smaller than what the dialer sent, so a dialer that finds
it smaller than its count *now* dialed in a session it ended during the
dial, and drops the link without backing off (`finishDial`). This closed a
lost `Down` the fuzzer found (`supersede`, `admit`).

**Incarnations fence links.** A node remembers per peer name the newest
incarnation it has seen (`newest`, `meet`) and refuses an older one; a newer
one's links replace the older one's. The default incarnation is the start
time, made strictly increasing, which also keeps it fresh inside a
`synctest` bubble whose clock stands still.

**`settling` orders a peer's sessions.** A change to a peer's links decided
under `n.mu` is counted in `settling` until it is carried out: a teardown
(`takeLinks`, and `outLost` when no inbound link is left) until the peer is
declared down, a new inbound link until it is announced. A new link either
way waits on `settled` until none is under way — `serveLink` before it goes
in, a dial in `admit` — so nothing the new session carries overtakes the old
session's `Down`s, and the old session's end fails nothing that went on the
new one. Every `settling[peer]++` has its `settle(peer)`, or the peer's next
link waits for ever.

**A call or a watch of a peer's process is recorded once its link is
known.** `callRemote` puts the call in `pending`, `placeWatch` records the
monitor or link, and a CallMonitor's `await` records its watch, only after
`getOut` returned the link they go on; a session that ends while they wait
for a dial is not theirs, and its `nodeDown`, which matches by peer name,
must not reach them. For the same reason `getOut` dials once more when the
dial it joined made a link of a session that ended meanwhile
(`errSessionEnded`).

**Link events are per session.** `linkUp` runs when the first link with a
peer goes in, either way (`began`), `linkDown` when the peer is declared
down. `announcing` counts the link-ups decided and not yet published, and
`linkDown` waits for it, so a session's end is never announced before its
start; `Stop` waits for it too, so no hook runs once it returns. `Stop`
announces no session's end.

**Lock order.** An inbound link's `mu` is outermost: it is held while a frame
is dispatched and while the link closes, and dispatch takes `n.mu`, process
locks and `n.pendingMu` under it, so nothing holding those may take it.
`n.mu` (an `RWMutex`: deliveries and sends over a live link take it shared)
comes before a process's `mu`, never inside it; two processes' locks are
held at once only under `n.mu` held exclusively. `n.pendingMu` is a leaf:
nothing is locked while it is held. `n.mu` is let go before links are closed
or a peer declared down (`supersede` unlocks around `linksLost`), so no hook
or user code runs under it.

**Dispatch never waits for a dial.** The goroutine reading a link dispatches
each frame itself; answers it makes (no such process, wrong type) to a peer
it has no link to yet are queued on the dial and written first once it is
up.

**An answer that cannot go back cuts the link its request came by.** A
peer's call carries `via`, the inbound link it arrived on, into its open
call (`answerTo.via`), and a watch into the watched process's `watchers`;
`routeOrCut` aborts that link, never `n.in[peer]`, which may belong to a
newer session that is owed nothing. The inbound handler never selects on the stream's context: a peer's
cancel is seen through `Recv`, after everything that preceded it.

**Types live on the address.** `Addr[M]` carries the message type, so every
send is checked by the compiler; the untyped process is
`Process[proto.Message]`, one implementation. Types do not cross the wire:
the receiving node checks `decoded.(M)`, and a mismatch is a dead letter
with reason `type`, never a panic in the process. Local sends pass the
pointer (`Config.CopyLocal` clones).

**Delivery is at-most-once, and errors say what is safe.** A failed dial
backs off (`DialBackoff`, a half-open breaker: one send redials while the
rest fail at once), and a link that ends within `DialTimeout` of coming up
counts as a failed dial. Only a `LinkError` whose `Unsent` is set is known
safe to send again; a call that ends with its context, or is waiting when its
node stops, may have been handled. Replies and `Down`s are never refused by
`MaxQueued`, and one that cannot be routed cuts the peer's link, or its
monitor would never fire. A mailbox bounded by `WithMailboxLimit` refuses a
message as a dead letter and a call with `ErrMailboxFull` (sent as
`STATUS_NOPROC` with reason `mailbox full`, which no handler can send, so no
protocol change), never a `Down` or an `Exited`.

**Admission is one decision per link.** `Config.Admit` is required, since a
peer is whoever its metadata says: `AdmitTLS` checks that the peer's
certificate names its node, `AdmitAll` trusts the network. It refuses a link,
which the peer sees as `PermissionDenied`, or returns the `Policy` that judges the
sends, calls, monitors (a link travels as one) and exits on it, by process
name. What a `Policy` refuses is answered as for a process that does not
exist: `ErrNoProc`, `Down{noproc}`, or dropped. Answers (replies, `Down`s,
demonitors) are never judged.

**The leader's state machines are fsm machines.** `leader/role.go` (the
election's stance), `singleton.go` (the singleton's phase) and `peers.go`
(each peer's standing, `fixedView` or `openView`) are package-level
`fsm.MustNew` values, fired through `e.machines` and reached from hooks
through the payload, never built in `init`. Their diagrams are
`leader/testdata/*.dot` and the Mermaid blocks in `leader/README.md`, both
kept current by `TestDiagrams`; `TestMachines` asserts no dead ends.

**A saga's run is a record, and its engine has no state.** `saga` keeps a
run's fsm state and data in a `Store`, saved before each effect and when
the run waits; an
engine claims due runs by a lease, each claim raising the run's epoch, and
only the owner writes its state (`Save` is fenced by the epoch). An effect
runs at least once, with `Run.Key` the same for every attempt; signals are
kept beside the state and added by anyone. An engine claims only runs whose
`SagaVersion` its `Definition.Version` reaches, so an older program never
works on a newer one's runs. A store must pass `sagatest.Store`.

## Invariants that are easy to break

- A change to the wire format, the handshake metadata or what a peer may
  send bumps `protoVersion` in `link.go`, and the release notes say whether
  nodes of the last release and this one can share a cluster.
- Nothing dispatched from a link after its `Down`; nothing from a new
  session before the old session's `Down`s (`settling`).
- A peer is declared down from its inbound link, not from an outbound
  failure.
- `epochs` is taken, not incremented, on a larger count, and a restarted
  peer's count is taken as it comes, not reset; either change makes the two
  ends' counts disagree and drops sessions for nothing.
- No user code, hook or `Hooks` method runs under `n.mu` or a process's
  `mu`; no panic is reachable from a peer's bytes (`FuzzDispatch`,
  `FuzzServeLink`).
- A local `Send` allocates nothing and never panics; configuration errors
  come from `NewNode`, not later.
- Nothing is queued on a link that its peer would refuse, since a frame the
  peer cannot take ends the link for everything on it: sends and calls are
  checked and fail (`encode`, `fits`: `ErrTooLarge`, UTF-8), answers are
  made to fit (`replyEnv`, `wireText`), and a name that cannot travel
  travels as none (`wireName`), never as another name. A peer whose name
  cannot travel gets no link (`getOut`, `serveLink`), and a member whose
  text cannot is not listed (`recordMember`). A new string or
  unbounded field on an envelope goes through one of them.
- The library opens no listener, reads no environment and installs no
  global; the application owns the `*grpc.Server`, credentials, discovery
  and logging. Every goroutine it starts — a process, a link's reader and
  writer, a dial — belongs to a node, and `Stop` waits for it (`n.wg`,
  `n.dialWG`).

## Testing strategy

- **Deterministic time is `testing/synctest`, not a clock interface.** Nodes
  and `grpcproctest` clusters run inside a bubble as they are: processes wait
  on channels the bubble sees, bufconn connections are channels, and timers
  keep the fake clock. Tests in the core, `actor`, `grpcproctest`, `pubsub`,
  `cron` and `leader` rely on it. A test that needs time to pass sleeps
  inside the bubble; one that waits for quiet calls `synctest.Wait`.
- **`grpcproctest` is the cluster.** `New(t, "a", "b", "c")`, `Partition`,
  `Heal`, `Kill`, `Restart`: a node dying is a plain `go test`. Its nodes
  redial at once (`DialBackoff` negative), so a send right after `Heal`
  reaches the peer.
- **Black-box first.** Most tests are `package grpcproc_test` against the
  public API; `*_internal_test.go` files reach internals only for what the
  API cannot set up (sessions, dispatch, backoff).
- **The fuzzers check promises, not outputs.** `FuzzScenario` reads bytes as
  operations on a three-node cluster — spawns, sends, calls, monitors, links,
  exits, partitions, kills, restarts — and checks what grpcproc promises of
  any run: per-sender order, at most once, never to a restarted node's
  process, a call gets its own answer or an error, a monitor fires at most
  once and exactly once for a target that ended, nothing is left running. Its
  settle loop waits until a minute passes with nothing new, since processes
  calling themselves keep the cluster busy. `FuzzDispatch` and
  `FuzzServeLink` feed a node arbitrary frames. CI replays the corpus; the
  nightly job fuzzes each target for 15 minutes and prints a failing input,
  which goes under `testdata/fuzz` as a seed.
- **Flakes are measured, not guessed.** The nightly job runs the suite 25
  times on 1, 2, 4 and 8 CPUs under `-race`: core flakes have shown up in
  about 2% of race runs. Run that locally for any change to links, sessions
  or locking.
- **Coverage is at 100% and CI fails below it** for the listed packages. A
  new branch with no test is a branch nobody has read.
- **Pinned output is updated, not edited.** `examples/` tests pin what each
  example prints (the site shows it), `leader/testdata/*.dot` and its README
  diagrams pin the machines; regenerate with `-update` and read the diff.

## Repo conventions

- **The core depends on gRPC, protobuf and fsm only.** Anything that would
  pull in another dependency is a nested module that plugs into an interface
  the core defines, as `otel` (`Hooks`), `saga/postgres` (`saga.Store`)
  and `etcd` (`Resolver`, `Registrar`, `Membership`, `Names`) do.
- **Released nested modules require a published core, never a `replace`**,
  since a `replace` is ignored by whoever imports them. `examples` and
  `benchmarks` use `replace ../` and are never released. For work across
  modules, or across this repository and fsm, use a `go.work` (ignored by
  git) or `GOWORK=/path/to/go.work`.
- **Releases follow `RELEASING.md`:** the minor version is shared by every
  module, patches are not, and `scripts/release.sh` does the tagging and
  pinning. Never tag, or `go get`, a version of this repository that is not
  meant to exist: the module proxy caches it for good.
- **Generated code is generated.** `.pb.go` files come from `buf generate`
  (`buf.yaml`, `buf.gen.yaml`); CI regenerates them and fails on a diff.
- **README code is embedded** from `examples/` with embedmd markers;
  `gofmt -w` the example before re-embedding.
- **`site/`** is the documentation at <https://floatdrop.github.io/grpcproc/>;
  its code blocks are files of `examples/` and its output what their tests
  pin. `site/README.md` has the rest.
- **Errors are values with what a caller needs:** `LinkError` with `Peer`
  and `Unsent`, `ExitError` with the reason, `RemoteError` matching by
  message; sentinels (`ErrNoProc`, `ErrNoConnection`, `ErrType`,
  `ErrLinkBusy`, `ErrMailboxFull`, `ErrNoQuery`, `ErrNodeStopped`) are matched with
  `errors.Is`.

## Tooling caveats

- **Generic methods need Go 1.27** (`go.mod` says 1.27.1): `Addr.Call[R]`
  and `Process.CallTo[R]` do not compile on 1.26. They also satisfy no
  interface, which is why `Caller` has one unexported method and the address
  calls.
- **golangci-lint v2** (CI pins v2.13.1) with `revive`'s `unused-receiver`
  and `unused-parameter` only, and `revive` excluded for `_test.go`, where
  hooks and process functions have fixed signatures. `tools` also excludes
  `fmt.Fprint*` from errcheck.
- **`scripts/release.sh` stops without a terminal** to ask on: check the plan
  with `-n`, then run it again with `-y`.
