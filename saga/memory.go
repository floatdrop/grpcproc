package saga

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"
)

// Memory returns a Store that keeps the runs in memory: for tests, and for
// sagas that need not outlast their program. Engines on several nodes of one
// program may share it.
func Memory() Store { return &memory{runs: map[runKey]*Record{}} }

type runKey struct{ saga, id string }

type memory struct {
	mu   sync.Mutex
	runs map[runKey]*Record
}

// clone copies r with slices of its own: what a store hands out is not what
// it keeps.
func clone(r *Record) Record {
	c := *r
	c.Data = slices.Clone(r.Data)
	c.Inbox = slices.Clone(r.Inbox)
	c.Consumed = nil
	return c
}

func (m *memory) Create(_ context.Context, r Record) (Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := runKey{r.Saga, r.ID}
	if have, ok := m.runs[k]; ok {
		return clone(have), false, nil
	}
	now := time.Now()
	kept := clone(&r)
	kept.Inbox, kept.Seen, kept.Epoch, kept.Owner = nil, 0, 0, ""
	kept.Revision, kept.CreatedAt, kept.UpdatedAt = 1, now, now
	m.runs[k] = &kept
	return clone(&kept), true, nil
}

func (m *memory) Claim(_ context.Context, owner string, sagas map[string]uint64, ttl time.Duration, n int) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	var due []*Record
	for _, r := range m.runs {
		free := r.Owner == "" || !r.LeaseUntil.After(now)
		woken := !r.Wake.IsZero() && !r.Wake.After(now) || r.unseen()
		version, runs := sagas[r.Saga]
		if r.Status == Active && free && woken && runs && r.SagaVersion <= version {
			due = append(due, r)
		}
	}
	slices.SortFunc(due, func(a, b *Record) int {
		return cmp.Or(a.Wake.Compare(b.Wake), cmp.Compare(a.Saga, b.Saga), cmp.Compare(a.ID, b.ID))
	})
	due = due[:min(len(due), max(n, 0))]
	out := make([]Record, len(due))
	for i, r := range due {
		r.Owner, r.LeaseUntil = owner, now.Add(ttl)
		r.Epoch++
		r.Revision++
		out[i] = clone(r)
	}
	return out, nil
}

func (m *memory) Save(_ context.Context, r Record) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	have, ok := m.runs[runKey{r.Saga, r.ID}]
	if !ok || have.Epoch != r.Epoch {
		return Record{}, ErrLost
	}
	now := time.Now()
	inbox := slices.DeleteFunc(have.Inbox, func(s Signal) bool { return slices.Contains(r.Consumed, s.Seq) })
	created, version, owner, until := have.CreatedAt, have.Revision, have.Owner, have.LeaseUntil
	r.SagaVersion = max(r.SagaVersion, have.SagaVersion)
	*have = clone(&r)
	have.Inbox, have.CreatedAt, have.Revision, have.UpdatedAt = inbox, created, version+1, now
	have.Owner, have.LeaseUntil = owner, until
	if r.Owner == "" {
		have.Owner, have.LeaseUntil = "", time.Time{}
	}
	return clone(have), nil
}

func (m *memory) Renew(_ context.Context, saga, id string, epoch uint64, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	have, ok := m.runs[runKey{saga, id}]
	if !ok || have.Epoch != epoch || have.Owner == "" {
		return ErrLost
	}
	have.LeaseUntil = time.Now().Add(ttl)
	return nil
}

func (m *memory) Signal(_ context.Context, saga, id string, s Signal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	have, ok := m.runs[runKey{saga, id}]
	switch {
	case !ok:
		return ErrNoRun
	case have.Status == Done:
		return ErrEnded
	case len(have.Inbox) >= MaxInbox:
		return ErrInboxFull
	}
	now := time.Now()
	s.Seq = have.Seen + 1
	if n := len(have.Inbox); n > 0 {
		s.Seq = max(s.Seq, have.Inbox[n-1].Seq+1)
	}
	s.Payload = slices.Clone(s.Payload)
	have.Inbox = append(have.Inbox, s)
	have.SagaVersion = max(have.SagaVersion, s.Version)
	have.Revision++
	have.UpdatedAt = now
	return nil
}

func (m *memory) Resume(_ context.Context, saga, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	have, ok := m.runs[runKey{saga, id}]
	if !ok {
		return ErrNoRun
	}
	if have.Status == Stuck {
		now := time.Now()
		have.Status, have.Attempts, have.Wake, have.RetryAt = Active, 0, now, time.Time{}
		have.Revision++
		have.UpdatedAt = now
	}
	return nil
}

func (m *memory) Get(_ context.Context, saga, id string) (Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	have, ok := m.runs[runKey{saga, id}]
	if !ok {
		return Record{}, false, nil
	}
	return clone(have), true, nil
}

func (m *memory) List(_ context.Context, saga string) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Record
	for k, r := range m.runs {
		if k.saga == saga {
			out = append(out, clone(r))
		}
	}
	slices.SortFunc(out, func(a, b Record) int { return cmp.Compare(a.ID, b.ID) })
	return out, nil
}
