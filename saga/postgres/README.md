# grpcproc/saga/postgres

A [`saga.Store`](../README.md#stores) in PostgreSQL: the runs of
[grpcproc/saga](../README.md) kept in a table, so that a run outlasts the
program that began it. A nested module, so that grpcproc itself depends on
no database.

```sh
go get github.com/floatdrop/grpcproc/saga/postgres
```

```go
db, err := sql.Open("pgx", dsn) // any driver for PostgreSQL
store, err := postgres.NewStore(db, "grpcproc_sagas")
err = store.Migrate(ctx) // or apply store.Schema() with your migrations
eng, err := saga.Start(node, saga.Config{Store: store}, orders)
```

It takes a `*sql.DB` the application opens, with the driver, credentials and
pool it chooses. Its tests run on pgx and on lib/pq, on PostgreSQL 14; it
needs 12 or later, for a generated column.

## The tables

`NewStore(db, "grpcproc_sagas")` keeps the runs in `grpcproc_sagas`, a row
per run, with the signals waiting on the row as a JSON array. A generated
column says when a run is due, at once if it holds a signal its owner has
not seen, and a partial index on it serves `Claim`. The name is lower case
letters, digits and `_`, quoted, in the `search_path`'s schema.

`Migrate` makes the table, or takes it on from where an earlier version of
this module left it: `grpcproc_sagas_schema` keeps how many of its steps
were taken. It runs in a transaction, under an advisory lock, so programs
that start at once take turns, and on tables that are current it only
reads, so a program that may not create tables can call it. `Schema` returns the same SQL, for an
application that applies it with its own migrations; a release that
changes the tables says in its notes what to apply.

Engines on several nodes, and programs that share the database, share the
runs.

## How it keeps the promises

Every change to a run is one statement on its row. A statement that
changes a run waits for one that holds its row, and works on what that one
wrote; `Claim` skips such a run instead, and takes it at its next poll.

- **Claim** picks the due runs with `FOR UPDATE SKIP LOCKED`, so engines
  that claim at once never take one run twice and never wait for each
  other, and raises each one's epoch in the same statement.
- **Save** is fenced by the epoch, and takes the signals it consumed out of
  the inbox as the row has it then: a signal that came while the owner
  worked is kept, and returned.
- **Signal** numbers a signal and counts the inbox on the row, so signals
  sent at once get distinct numbers and `MaxInbox` holds. A run with no
  room is neither locked nor written, so senders that retry on a full inbox
  do not keep its owner from claiming it. The refusal says why:
  `ErrNoRun`, `ErrEnded` or `ErrInboxFull`, for a run that ended or filled
  while the signal waited for its row too.
- **Create** of a run that is there returns it, though another `Create`
  made it a moment before.

IDs and saga names are ordered by their bytes, as Go orders strings and
`saga.Memory` does, whatever the database's collation. A payload or data
with no bytes reads back as nil through any driver.

An effect's `Error`, and an `Otherwise`'s `Cause`, are kept with NUL bytes
dropped and invalid UTF-8 replaced, since PostgreSQL text takes neither and
a save refused for them would be refused every time. Saga names, IDs, event
names and states are kept as they are, so they must be UTF-8 with no NUL:
a run whose state prints otherwise cannot be saved.

Leases and due times are read on the database's clock, which every engine
shares; the times an engine sets, a run's wake among them, are its own. A
timer of an engine whose clock is set apart from the database's fires that
much early or late. A lease is safe all the same: an engine counts its own
lease from before it asked for it, by its own clock.

Versions are kept as `bigint`: a `Definition.Version` past
`math.MaxInt64` is refused by `Create`, `Claim`, `Save` and `Signal`.

## Tests

```sh
go test -race ./...
```

The tests start a PostgreSQL 14 with
[embedded-postgres](https://github.com/fergusstrange/embedded-postgres),
which downloads it on the first run. They run
[`sagatest.Store`](../sagatest/store.go) through two drivers, signals and
claims, creates and saves at once, migrations, and a saga whose program is stopped in
the middle of a step and which a new program, on a new node, finishes.
