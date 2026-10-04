package conversationsv1

import (
	"context"
	"errors"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/pubsub"
)

const (
	// Service is the conversations service's name in a deployment's
	// placement, and the name its supervisor is registered under.
	Service = "conversations"
	// Factory is the factory its supervisor starts a conversation with.
	Factory = "conversation"
)

// ConversationAddr addresses one conversation: its process, by the name it
// is registered under, and the supervisor that starts it. Say and Follow are
// its protocol; each starts the conversation's process if none runs.
type ConversationAddr struct {
	grpcproc.Addr[*Request]
	node, id string
}

// ValidID reports whether id can name a conversation: letters, digits, "-"
// and "_", at most 64. An id is part of two registered names, the process's
// and its topic's, so one may not look like another's topic.
func ValidID(id string) bool {
	ok := func(r rune) bool {
		return r == '-' || r == '_' || '0' <= r && r <= '9' || 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z'
	}
	return id != "" && len(id) <= 64 && strings.IndexFunc(id, func(r rune) bool { return !ok(r) }) < 0
}

// Conversation addresses the conversation id on node, the node that runs
// the conversations service. A conversation is registered under a name made
// from its id, for as long as its process runs.
func Conversation(node, id string) ConversationAddr {
	return ConversationAddr{Addr: grpcproc.Named[*Request](node, "conversation/"+id), node: node, id: id}
}

// open starts the conversation's process, unless it runs already. A
// conversation that has been idle ends its process; opening it again starts
// one, with what was said before.
func (c ConversationAddr) open(ctx context.Context, from grpcproc.Caller) error {
	sup := grpcproc.Name{Node: c.node, Name: Service}
	_, _, err := actor.StartChildFrom(ctx, from, sup, Factory, &Open{Id: c.id})
	if errors.Is(err, actor.ErrAlreadyStarted) {
		return nil
	}
	return err
}

// ask opens the conversation and calls it. A process that was idle may end
// between the two: it is then opened again, once.
func (c ConversationAddr) ask[R proto.Message](ctx context.Context, from grpcproc.Caller, req *Request) (r R, err error) {
	for range 2 {
		if err = c.open(ctx, from); err != nil {
			return r, err
		}
		if r, err = c.Call[R](ctx, from, req); !errors.Is(err, grpcproc.ErrNoProc) {
			break
		}
	}
	return r, err
}

// Say tells the conversation what the user said, and returns once its turn
// has begun: after the turns before it. The answer is published to whoever
// follows the conversation.
func (c ConversationAddr) Say(ctx context.Context, from grpcproc.Caller, text string) (*Said, error) {
	return c.ask[*Said](ctx, from, &Request{Op: &Request_Say{Say: &Say{Text: text}}})
}

// Follow subscribes p to the conversation's events: what the user says,
// every token of its answers, the tools it calls, and the end of each turn,
// from the last it keeps on. The Down with the subscription's Ref means no
// more come: the conversation's process has ended.
//
// A process that has just started has not made its topic yet, and one that
// is in a turn answers nothing until the turn is over. So Follow asks the
// conversation nothing: it subscribes, and again for a moment while there is
// no topic.
func (c ConversationAddr) Follow(ctx context.Context, p *grpcproc.Process[*Event]) (pubsub.Subscription, error) {
	if err := c.open(ctx, p); err != nil {
		return pubsub.Subscription{}, err
	}
	topic := pubsub.Named[*Event](c.node, TopicName(c.id))
	for range 100 {
		sub, err := topic.Subscribe(ctx, p)
		if !errors.Is(err, grpcproc.ErrNoProc) {
			return sub, err
		}
		select {
		case <-ctx.Done():
			return pubsub.Subscription{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return pubsub.Subscription{}, grpcproc.ErrNoProc
}

// TopicName is the name the topic of conversation id is registered under,
// on the conversation's node: what a model is told to publish its tokens to.
func TopicName(id string) string { return "conversation/" + id + "/events" }
