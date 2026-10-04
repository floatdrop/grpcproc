// Package saga runs sagas that outlast a process: work that spans services,
// kept in a Store, so that a crash, a restart or a lost node is followed by
// the run going on from where it was.
//
// A saga is an fsm machine and a protobuf message. Define gives states of the
// machine an effect, the function that runs when a run enters the state, and
// the effect fires the event that moves the run on:
//
//	orders := saga.Define[*ordersv1.Order]("order", machine).
//		Do(Reserving, reserve).
//		Do(Charging, charge)
//	eng, err := saga.Start(node, saga.Config{Store: saga.Memory()}, orders)
//	created, err := orders.Begin(ctx, eng, "order-123", &ordersv1.Order{Sku: "apple"})
//	snap, err := orders.Wait(ctx, eng, "order-123")
//
// The run's state and data are saved before each effect and when the run
// waits, and nothing is replayed: after a crash, the code that runs is the
// effect of the state the record is in. So an effect runs at least once, and what it asks of others
// must be safe to ask twice: Run.Key is the same for every attempt, and
// travels in the metadata of what the effect sends with its ctx.
//
// Sequence builds the saga of the textbooks, steps in order with what undoes
// each, as such a machine.
package saga

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/floatdrop/grpcproc"
)

// Status is where a run stands with its engine.
type Status uint8

const (
	// Active is a run that goes on: at work, or waiting for a signal, a
	// timer or its next attempt.
	Active Status = iota
	// Done is a run whose machine reached a state with no way out.
	Done
	// Stuck is a run whose effect failed for good with no Otherwise to
	// fire. It is kept as it is until Resume.
	Stuck
)

func (s Status) String() string {
	switch s {
	case Active:
		return "active"
	case Done:
		return "done"
	case Stuck:
		return "stuck"
	}
	return "status(" + strconv.Itoa(int(s)) + ")"
}

// Record is a run as a Store keeps it.
type Record struct {
	Saga string // the Definition's name
	ID   string // the run's, unique within its saga
	// SagaVersion is the highest Definition.Version that began, signalled or
	// worked on the run. An engine claims a run only if its saga's version
	// is at least that: an older program leaves a newer one's runs alone.
	SagaVersion uint64

	State string // the machine's state, as fmt prints it
	Data  []byte // the run's data, a protobuf encoding
	// Visit counts the states the run has entered, the first included. It
	// tells two visits to one state apart, in Run.Key and for a timer.
	Visit uint64
	// EffectDone is set once the state's effect is through for this visit:
	// it returned nil without firing, or failed for good and its Otherwise
	// kept the run in the state. The run waits, and the effect is not run
	// again in this visit.
	EffectDone bool
	Attempts   int    // how many times the effect failed in this visit, while it is tried
	Error      string // why it failed last while it is tried, or why the run is Stuck
	// Cause is the error an Otherwise last fired with: why the run left its
	// way, kept for the rest of it.
	Cause string

	Status Status
	// Wake is when the run is next due: the sooner of RetryAt and Deadline
	// when its owner let it go. Zero is never. A signal its owner has not
	// seen makes a run due whatever its Wake.
	Wake time.Time
	// RetryAt is when the effect, having failed, is next tried; zero if it
	// is not waiting for that.
	RetryAt time.Time
	// Deadline is when the timer of this visit fires; zero if it has none.
	Deadline time.Time

	// Owner is the engine working on the run, "" for none, until
	// LeaseUntil. Epoch counts the claims of the run: it is the fence a
	// Save must present.
	Owner      string
	LeaseUntil time.Time
	Epoch      uint64

	// Inbox is the signals waiting, in the order they came. A Save removes
	// those in Consumed, by Seq, and says in Seen the highest Seq its owner
	// has looked at.
	Inbox    []Signal
	Consumed []uint64
	Seen     uint64

	Revision  uint64 // counts the writes to the record
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Signal is an event sent to a run from outside it, kept until its machine
// is in a state that takes it.
type Signal struct {
	Seq     uint64 // set by the Store, growing per run
	Event   string // the fsm event's name
	Payload []byte // its payload, a protobuf encoding; nil for a signal with none
	// Version is the Definition.Version of the program that sent it: the
	// run's SagaVersion is raised to it.
	Version uint64
}

// MaxInbox is how many signals a run holds before Store.Signal refuses more.
// A signal no state of the run takes is held until the run ends, so a sender
// that repeats itself can fill it.
const MaxInbox = 64

// consume takes the i'th signal out of the inbox, for the owner and, at its
// next Save, for the store.
func (r *Record) consume(i int) {
	r.Consumed = append(r.Consumed, r.Inbox[i].Seq)
	r.Inbox = slices.Delete(r.Inbox, i, i+1)
}

// unseen reports whether the run holds a signal its owner has not looked at.
func (r *Record) unseen() bool {
	n := len(r.Inbox)
	return n > 0 && r.Inbox[n-1].Seq > r.Seen
}

// A Store keeps the runs. It is the one thing a saga needs that outlasts a
// process: everything else is the engine's, which has no state of its own.
// sagatest.Store checks an implementation.
//
// One engine owns a run at a time, by a lease, and only the owner writes its
// state; anyone adds signals, which are kept beside it. Every claim raises
// the run's Epoch, and a Save or Renew with another is refused with ErrLost,
// so an engine that lost its lease cannot write over its successor.
//
// Every method returns when its ctx ends: an engine gives up a lease it
// could not renew in time, and a Renew that outlasts its ctx would keep it
// from knowing.
type Store interface {
	// Create adds r, unless its saga has a run with its ID: then it returns
	// that run, and false.
	Create(ctx context.Context, r Record) (Record, bool, error)
	// Claim gives owner up to n runs that are due, each for ttl, of the sagas
	// named in sagas, whose SagaVersion is no more than the version given
	// there. A run is due if it is Active, has no owner or one whose lease
	// ended, and its Wake has come or it holds a signal past its Seen. Claim
	// raises each one's Epoch, and returns them oldest Wake first.
	Claim(ctx context.Context, owner string, sagas map[string]uint64, ttl time.Duration, n int) ([]Record, error)
	// Save writes the state of a run its caller owns: every field but its
	// lease, Inbox, CreatedAt and Revision, and its SagaVersion only if
	// r's is higher: a signal may have raised it since the caller read it. It removes the signals in
	// r.Consumed. The lease stays as Claim and Renew left it, unless r.Owner
	// is "": that lets the run go. Save returns the record as it is kept,
	// its inbox as it is now; ErrLost if r.Epoch is not the run's.
	Save(ctx context.Context, r Record) (Record, error)
	// Renew extends the lease of a run to ttl from now; ErrLost if epoch is
	// not the run's, or the run has no owner.
	Renew(ctx context.Context, saga, id string, epoch uint64, ttl time.Duration) error
	// Signal adds s to a run's inbox, with the next Seq, which makes an
	// Active run due, and raises the run's SagaVersion to s.Version. ErrNoRun
	// if there is none, ErrEnded if it is Done, ErrInboxFull if it holds
	// MaxInbox.
	Signal(ctx context.Context, saga, id string, s Signal) error
	// Resume makes a Stuck run Active, due at once, its attempts counted
	// from none and its RetryAt cleared. ErrNoRun if there is none; a run
	// that is not Stuck is left as it is.
	Resume(ctx context.Context, saga, id string) error
	// Get returns a run, and whether there is one.
	Get(ctx context.Context, saga, id string) (Record, bool, error)
	// List returns the runs of a saga, by ID.
	List(ctx context.Context, saga string) ([]Record, error)
}

var (
	// ErrLost is a write by an engine that no longer owns the run.
	ErrLost = errors.New("saga: the run has another owner")
	// ErrNoRun is a signal to, or a question about, a run that is not there.
	ErrNoRun = errors.New("saga: no such run")
	// ErrEnded is a signal to a run that is Done.
	ErrEnded = errors.New("saga: the run has ended")
	// ErrInboxFull is a signal to a run that holds MaxInbox already.
	ErrInboxFull = errors.New("saga: the run's inbox is full")
	// ErrFired is a second Fire from one effect.
	ErrFired = errors.New("saga: the effect has fired already")
)

// The metadata an effect's ctx carries, and so every send and call made with
// it: the run's key, the same for every attempt of one visit to a state, and
// its fence, which grows with each owner the run has.
const (
	MetadataKey   = "grpcproc-saga-key"
	MetadataFence = "grpcproc-saga-fence"
)

// KeyOf reads the key and the fence a saga's effect sent with a message:
// what a participant keeps with what it did, to answer a repeat as it
// answered before, and to refuse a fence lower than it has seen for the key.
// ok is false for a message that no effect sent.
func KeyOf(md grpcproc.Metadata) (key string, fence uint64, ok bool) {
	key, ok = md[MetadataKey]
	if !ok {
		return "", 0, false
	}
	fence, err := strconv.ParseUint(md[MetadataFence], 10, 64)
	return key, fence, err == nil
}

// permanent marks an error no attempt will get past.
type permanent struct{ error }

func (p permanent) Unwrap() error { return p.error }

// Permanent marks err as one that trying again cannot get past: the effect
// that returns it is not run again. Its state's Otherwise fires, or the run
// is Stuck.
func Permanent(err error) error { return permanent{err} }
