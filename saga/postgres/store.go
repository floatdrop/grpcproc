// Package postgres keeps the runs of grpcproc/saga in PostgreSQL: Store is
// a saga.Store, in one table, a row per run, its inbox on the row.
//
//	db, err := sql.Open("pgx", dsn) // any driver for PostgreSQL
//	store, err := postgres.NewStore(db, "grpcproc_sagas")
//	err = store.Migrate(ctx) // or apply store.Schema() with your migrations
//	eng, err := saga.Start(node, saga.Config{Store: store}, orders)
//
// It takes a *sql.DB the application opens, with the driver, credentials and
// pool it chooses. Every change to a run is one statement on its row, so a
// run's state and its signals change together. A statement that changes a
// run waits for one that holds its row, and works on what that one wrote;
// Claim skips such a run instead. Create, Claim and Save return the row as
// they left it.
//
// Leases and due times are read on the database's clock, which every engine
// shares; the times an engine sets, a run's Wake among them, are its own. A
// timer of an engine whose clock is set apart from the database's fires that
// much early or late; a lease is safe all the same, since an engine counts
// its own lease from before it asked for it.
package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/floatdrop/grpcproc/saga"
)

// Store is a saga.Store in PostgreSQL. Engines on several nodes, and
// programs that share the database, share the runs.
type Store struct {
	db *sql.DB
	q  queries
}

var _ saga.Store = (*Store)(nil)

// table is what a table name may be: short enough for the index's name made
// from it. The schema is the search_path's.
var table = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,55}$`)

// NewStore returns the store kept in the table name of db. It does not
// touch the database: Migrate, or the application's own migrations applying
// Schema, creates the table.
func NewStore(db *sql.DB, name string) (*Store, error) {
	if !table.MatchString(name) {
		return nil, fmt.Errorf("saga/postgres: %q is not a table name: lower case letters, digits and _, at most 56", name)
	}
	return &Store{db: db, q: newQueries(name)}, nil
}

// Schema is the SQL that makes the store's tables as this version of the
// package wants them, each statement ending in a semicolon: for an
// application that applies it with its own migrations.
func (s *Store) Schema() string {
	var b strings.Builder
	b.WriteString(s.q.version + ";\n")
	for _, step := range s.q.steps {
		for _, stmt := range step {
			b.WriteString(stmt + ";\n")
		}
	}
	fmt.Fprintf(&b, "INSERT INTO %s VALUES (%d);\n", s.q.versionTable, len(s.q.steps))
	return b.String()
}

// Migrate brings the store's tables to what this version of the package
// wants: it makes them, or takes them on from where an earlier version
// left them, by the version kept beside them. Programs that start at once
// may each call it: they take turns.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrap("migrate", err)
	}
	if err = s.migrate(ctx, tx); err != nil {
		_ = tx.Rollback()
		return wrap("migrate", err)
	}
	return wrap("migrate", tx.Commit())
}

func (s *Store) migrate(ctx context.Context, tx *sql.Tx) error {
	// Each step runs only if those before it went well.
	var err error
	do := func(step func() error) {
		if err == nil {
			err = step()
		}
	}
	exec := func(query string, args ...any) func() error {
		return func() error {
			_, err := tx.ExecContext(ctx, query, args...)
			return err
		}
	}
	// The lock is held to the end of the transaction.
	do(exec(`SELECT pg_advisory_xact_lock(hashtext($1))`, "grpcproc/saga/postgres "+s.q.versionTable))
	var made bool
	do(func() error {
		return tx.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, s.q.versionTable).Scan(&made)
	})
	var at int
	if made {
		do(func() error {
			return tx.QueryRowContext(ctx, `SELECT COALESCE(max(version), 0) FROM `+s.q.versionTable).Scan(&at)
		})
	}
	// Tables as current as this version wants, or a later version's, are
	// left as they are: a program that may not create tables starts on
	// them all the same.
	if err != nil || at >= len(s.q.steps) {
		return err
	}
	if !made {
		do(exec(s.q.version))
	}
	for _, step := range s.q.steps[at:] {
		for _, stmt := range step {
			do(exec(stmt))
		}
	}
	do(exec(`DELETE FROM ` + s.q.versionTable))
	do(exec(`INSERT INTO `+s.q.versionTable+` VALUES ($1)`, len(s.q.steps)))
	return err
}

// columns of a run, in the order scanRun reads them.
const columns = `r.saga, r.id, r.saga_version, r.state, r.data, r.visit, r.effect_done, r.attempts,
	r.error, r.cause, r.status, r.wake, r.retry_at, r.deadline, r.owner, r.lease_until, r.epoch,
	r.seen, r.revision, r.created_at, r.updated_at, r.inbox`

type queries struct {
	// version makes the table that keeps how many steps were taken; steps
	// are the migrations, in order, each its statements.
	version, versionTable                                         string
	steps                                                         [][]string
	create, get, list, claim, save, renew, signal, status, resume string
}

func newQueries(name string) queries {
	t := `"` + name + `"`
	active, done, stuck := strconv.Itoa(int(saga.Active)), strconv.Itoa(int(saga.Done)), strconv.Itoa(int(saga.Stuck))
	versions := `"` + name + `_schema"`
	return queries{
		version:      `CREATE TABLE IF NOT EXISTS ` + versions + ` (version integer NOT NULL)`,
		versionTable: versions,
		steps: [][]string{{
			`CREATE TABLE IF NOT EXISTS ` + t + ` (
	-- Ordered by their bytes, as Go orders strings.
	saga         text COLLATE "C" NOT NULL,
	id           text COLLATE "C" NOT NULL,
	saga_version bigint NOT NULL,
	state        text NOT NULL,
	data         bytea,
	visit        bigint NOT NULL,
	effect_done  boolean NOT NULL,
	attempts     bigint NOT NULL,
	error        text NOT NULL,
	cause        text NOT NULL,
	status       smallint NOT NULL,
	wake         timestamptz,
	retry_at     timestamptz,
	deadline     timestamptz,
	owner        text NOT NULL,
	lease_until  timestamptz,
	epoch        bigint NOT NULL,
	seen         bigint NOT NULL,
	-- The signals waiting, a JSON array of {seq, event, payload, version},
	-- payload in base64; and the Seq last given.
	inbox        jsonb NOT NULL,
	last_seq     bigint NOT NULL,
	revision     bigint NOT NULL,
	created_at   timestamptz NOT NULL,
	updated_at   timestamptz NOT NULL,
	-- When the run is due: at once if the newest signal waiting is past
	-- what its owner has seen, else at its wake.
	due          timestamptz GENERATED ALWAYS AS
		(CASE WHEN (inbox->-1->>'seq')::bigint > seen THEN '-infinity'::timestamptz ELSE wake END) STORED,
	PRIMARY KEY (saga, id)
)`,
			`CREATE INDEX IF NOT EXISTS "` + name + `_due" ON ` + t + ` (saga, due) WHERE status = ` + active,
		}},

		create: `INSERT INTO ` + t + ` AS r (saga, id, saga_version, state, data, visit, effect_done, attempts,
	error, cause, status, wake, retry_at, deadline, owner, lease_until, epoch, seen, inbox, last_seq,
	revision, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, '', NULL, 0, 0, '[]', 0, 1, now(), now())
ON CONFLICT (saga, id) DO NOTHING
RETURNING ` + columns,

		get:  `SELECT ` + columns + ` FROM ` + t + ` r WHERE r.saga = $1 AND r.id = $2`,
		list: `SELECT ` + columns + ` FROM ` + t + ` r WHERE r.saga = $1 ORDER BY r.id`,

		// sagas is a JSON object of the sagas' versions. Runs another
		// statement holds are skipped, not waited on; a run changed since
		// the statement began is read again, and taken only if still due.
		claim: `WITH versions AS (
	SELECT key AS saga, value::bigint AS version FROM jsonb_each_text($2::jsonb)
), due AS (
	SELECT r.saga, r.id FROM ` + t + ` r JOIN versions v ON v.saga = r.saga
	WHERE r.status = ` + active + ` AND r.due <= now() AND r.saga_version <= v.version
		AND (r.owner = '' OR r.lease_until <= now())
	ORDER BY r.wake NULLS FIRST, r.saga, r.id
	LIMIT $4
	FOR UPDATE OF r SKIP LOCKED
), claimed AS (
	UPDATE ` + t + ` r SET owner = $1, lease_until = now() + $3::bigint * interval '1 microsecond',
		epoch = r.epoch + 1, revision = r.revision + 1
	FROM due WHERE r.saga = due.saga AND r.id = due.id
	RETURNING r.*
)
SELECT ` + columns + ` FROM claimed r ORDER BY r.wake NULLS FIRST, r.saga, r.id`,

		// The save, fenced by the epoch, takes the signals it consumed, a
		// JSON array of their Seqs, out of the inbox as the row has it then,
		// those that came since included.
		save: `UPDATE ` + t + ` r SET saga_version = GREATEST(r.saga_version, $3), state = $4, data = $5,
	visit = $6, effect_done = $7, attempts = $8, error = $9, cause = $10, status = $11, wake = $12,
	retry_at = $13, deadline = $14, seen = $15,
	owner = CASE WHEN $16 = '' THEN '' ELSE r.owner END,
	lease_until = CASE WHEN $16 = '' THEN NULL ELSE r.lease_until END,
	inbox = (SELECT COALESCE(jsonb_agg(e ORDER BY (e->>'seq')::bigint), '[]'::jsonb)
		FROM jsonb_array_elements(r.inbox) e
		WHERE NOT ($18::jsonb @> to_jsonb((e->>'seq')::bigint))),
	revision = r.revision + 1, updated_at = now()
WHERE r.saga = $1 AND r.id = $2 AND r.epoch = $17
RETURNING ` + columns,

		renew: `UPDATE ` + t + ` SET lease_until = now() + $4::bigint * interval '1 microsecond'
WHERE saga = $1 AND id = $2 AND epoch = $3 AND owner <> ''
RETURNING 1`,

		// A run with no room is neither locked nor written. Why there was
		// none is what the run said as the statement began.
		signal: `WITH run AS (
	SELECT status, jsonb_array_length(inbox) AS n FROM ` + t + ` WHERE saga = $1 AND id = $2
), added AS (
	UPDATE ` + t + ` SET
		inbox = inbox || jsonb_build_array(jsonb_build_object('seq', GREATEST(last_seq, seen) + 1,
			'event', $3::text, 'payload', translate(encode($4::bytea, 'base64'), E'\n', ''), 'version', $5::bigint)),
		last_seq = GREATEST(last_seq, seen) + 1, saga_version = GREATEST(saga_version, $5::bigint),
		revision = revision + 1, updated_at = now()
	WHERE saga = $1 AND id = $2 AND status <> ` + done + ` AND jsonb_array_length(inbox) < $6
	RETURNING 1
)
SELECT EXISTS (SELECT 1 FROM added), EXISTS (SELECT 1 FROM run), COALESCE((SELECT status FROM run), 0),
	COALESCE((SELECT n FROM run), 0)`,
		status: `SELECT status FROM ` + t + ` WHERE saga = $1 AND id = $2`,

		// The run is looked for as the statement begins: none is ever
		// deleted, so one that is not there then was not there before.
		resume: `WITH run AS (
	SELECT 1 FROM ` + t + ` WHERE saga = $1 AND id = $2
), up AS (
	UPDATE ` + t + ` SET status = ` + active + `, attempts = 0, wake = now(), retry_at = NULL,
		revision = revision + 1, updated_at = now()
	WHERE saga = $1 AND id = $2 AND status = ` + stuck + `
)
SELECT count(*) FROM run`,
	}
}

// Create adds r, unless its saga has a run with its ID: then it returns
// that run, and false.
func (s *Store) Create(ctx context.Context, r saga.Record) (saga.Record, bool, error) {
	version, err := pgUint(r.SagaVersion)
	if err != nil {
		return saga.Record{}, false, wrap("create", err)
	}
	run, err := scanRun(s.db.QueryRowContext(ctx, s.q.create, r.Saga, r.ID, version, r.State, nullBytes(r.Data),
		int64(r.Visit), r.EffectDone, r.Attempts, text(r.Error), text(r.Cause), int16(r.Status),
		nullTime(r.Wake), nullTime(r.RetryAt), nullTime(r.Deadline)))
	if errors.Is(err, sql.ErrNoRows) {
		// It is there: the insert that made it was done when this one
		// found it, so a statement begun now sees it.
		run, err = scanRun(s.db.QueryRowContext(ctx, s.q.get, r.Saga, r.ID))
		return run, false, wrap("create", err)
	}
	return run, err == nil, wrap("create", err)
}

// Claim gives owner up to n runs that are due.
func (s *Store) Claim(ctx context.Context, owner string, sagas map[string]uint64, ttl time.Duration, n int) ([]saga.Record, error) {
	if n <= 0 || len(sagas) == 0 {
		return nil, nil
	}
	versions := make(map[string]int64, len(sagas))
	for name, v := range sagas {
		version, err := pgUint(v)
		if err != nil {
			return nil, wrap("claim", err)
		}
		versions[name] = version
	}
	arg, err := json.Marshal(versions)
	if err != nil {
		return nil, wrap("claim", err)
	}
	rows, err := s.db.QueryContext(ctx, s.q.claim, owner, string(arg), ttl.Microseconds(), n)
	if err != nil {
		return nil, wrap("claim", err)
	}
	runs, err := scanRuns(rows)
	return runs, wrap("claim", err)
}

// Save writes the state of a run its caller owns.
func (s *Store) Save(ctx context.Context, r saga.Record) (saga.Record, error) {
	version, err := pgUint(r.SagaVersion)
	if err != nil {
		return saga.Record{}, wrap("save", err)
	}
	consumed, _ := json.Marshal(r.Consumed) // a slice of integers always encodes
	run, err := scanRun(s.db.QueryRowContext(ctx, s.q.save, r.Saga, r.ID, version, r.State, nullBytes(r.Data),
		int64(r.Visit), r.EffectDone, r.Attempts, text(r.Error), text(r.Cause), int16(r.Status),
		nullTime(r.Wake), nullTime(r.RetryAt), nullTime(r.Deadline), int64(r.Seen), r.Owner, int64(r.Epoch),
		string(consumed)))
	if errors.Is(err, sql.ErrNoRows) { // the epoch was not the run's
		return saga.Record{}, saga.ErrLost
	}
	return run, wrap("save", err)
}

// Renew extends the lease of a run to ttl from now.
func (s *Store) Renew(ctx context.Context, sagaName, id string, epoch uint64, ttl time.Duration) error {
	var one int
	err := s.db.QueryRowContext(ctx, s.q.renew, sagaName, id, int64(epoch), ttl.Microseconds()).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return saga.ErrLost
	}
	return wrap("renew", err)
}

// Signal adds sig to a run's inbox, with the next Seq.
func (s *Store) Signal(ctx context.Context, sagaName, id string, sig saga.Signal) error {
	version, err := pgUint(sig.Version)
	if err != nil {
		return wrap("signal", err)
	}
	var (
		added, found bool
		status       saga.Status
		n            int
	)
	err = s.db.QueryRowContext(ctx, s.q.signal, sagaName, id, sig.Event, nullBytes(sig.Payload), version,
		saga.MaxInbox).Scan(&added, &found, &status, &n)
	switch {
	case err != nil:
		return wrap("signal", err)
	case added:
		return nil
	case !found:
		return saga.ErrNoRun
	case status == saga.Done:
		return saga.ErrEnded
	case n >= saga.MaxInbox:
		return saga.ErrInboxFull
	}
	// It had room as the statement began, and none once the signal had its
	// row: the run ended or filled meanwhile, and says which now.
	err = s.db.QueryRowContext(ctx, s.q.status, sagaName, id).Scan(&status)
	if err == nil && status == saga.Done {
		return saga.ErrEnded
	}
	return cmp.Or(wrap("signal", err), saga.ErrInboxFull)
}

// Resume makes a Stuck run Active, due at once.
func (s *Store) Resume(ctx context.Context, sagaName, id string) error {
	var found int
	if err := s.db.QueryRowContext(ctx, s.q.resume, sagaName, id).Scan(&found); err != nil {
		return wrap("resume", err)
	}
	if found == 0 {
		return saga.ErrNoRun
	}
	return nil
}

// Get returns a run, and whether there is one.
func (s *Store) Get(ctx context.Context, sagaName, id string) (saga.Record, bool, error) {
	run, err := scanRun(s.db.QueryRowContext(ctx, s.q.get, sagaName, id))
	if errors.Is(err, sql.ErrNoRows) {
		return saga.Record{}, false, nil
	}
	return run, err == nil, wrap("get", err)
}

// List returns the runs of a saga, by ID.
func (s *Store) List(ctx context.Context, sagaName string) ([]saga.Record, error) {
	rows, err := s.db.QueryContext(ctx, s.q.list, sagaName)
	if err != nil {
		return nil, wrap("list", err)
	}
	runs, err := scanRuns(rows)
	return runs, wrap("list", err)
}

// signal is a signal as the inbox column keeps it.
type signal struct {
	Seq     uint64 `json:"seq"`
	Event   string `json:"event"`
	Payload []byte `json:"payload"` // base64, null for none
	Version uint64 `json:"version"`
}

// scanRun reads a run, as columns lists it.
func scanRun(row interface{ Scan(...any) error }) (saga.Record, error) {
	var (
		r                                  saga.Record
		version, visit, epoch, seen, rev   int64
		status                             int16
		wake, retryAt, deadline, leaseEnds sql.NullTime
		inbox                              []byte
	)
	if err := row.Scan(&r.Saga, &r.ID, &version, &r.State, &r.Data, &visit, &r.EffectDone, &r.Attempts,
		&r.Error, &r.Cause, &status, &wake, &retryAt, &deadline, &r.Owner, &leaseEnds, &epoch,
		&seen, &rev, &r.CreatedAt, &r.UpdatedAt, &inbox); err != nil {
		return saga.Record{}, err
	}
	var signals []signal
	if err := json.Unmarshal(inbox, &signals); err != nil {
		return saga.Record{}, err
	}
	for _, sig := range signals {
		r.Inbox = append(r.Inbox, saga.Signal{Seq: sig.Seq, Event: sig.Event, Payload: nullBytes(sig.Payload), Version: sig.Version})
	}
	r.SagaVersion, r.Visit, r.Epoch, r.Seen, r.Revision = uint64(version), uint64(visit), uint64(epoch), uint64(seen), uint64(rev)
	r.Status = saga.Status(status)
	r.Data = nullBytes(r.Data)
	r.Wake, r.RetryAt, r.Deadline, r.LeaseUntil = wake.Time, retryAt.Time, deadline.Time, leaseEnds.Time
	return r, nil
}

// scanRuns reads the runs rows has, and closes it.
func scanRuns(rows *sql.Rows) ([]saga.Record, error) {
	defer func() { _ = rows.Close() }()
	var out []saga.Record
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// nullBytes is b, or nil for none: a driver may send an empty slice, or
// read one, where another has NULL.
func nullBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

// nullTime is t, or NULL for the zero time, which a Record uses for never.
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// text is s as PostgreSQL text keeps it, with no NUL and valid UTF-8: an
// effect's error is anything its code wrote, and a save that is refused for
// it is refused every time.
func text(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "�")
}

// pgUint is v as a bigint, which holds versions up to math.MaxInt64.
func pgUint(v uint64) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("version %d is past what a bigint holds", v)
	}
	return int64(v), nil
}

// wrap says which of the store's operations err comes from.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("saga/postgres: %s: %w", op, err)
}
