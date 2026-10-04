package models

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	modelsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/models/v1"
	toolsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/tools/v1"
)

// script stands in for a model. It answers by rule, a word at a time, so
// that a test knows what it says: it asks for the calc tool when it sees a
// sum, for the sleep tool when told to wait, reports what a tool returned,
// and otherwise says back what it was told.
type script struct {
	// pace is the wait between two tokens: what makes the stream visible
	// in a browser.
	pace time.Duration
}

func newScript(pace Pace) *script { return &script{pace: time.Duration(pace)} }

// Pace is the script's wait between two tokens.
type Pace time.Duration

var (
	sum  = regexp.MustCompile(`(-?\d+)\s*([-+*/])\s*(-?\d+)`)
	wait = regexp.MustCompile(`wait (\S+)`)
)

func (s *script) Generate(ctx context.Context, history []*modelsv1.Turn, emit func(string) error) (*toolsv1.Run, error) {
	if len(history) == 0 {
		return nil, errors.New("models: nothing to answer")
	}
	last := history[len(history)-1]
	say, tool := "You said: "+last.Text, (*toolsv1.Run)(nil)
	switch {
	case last.Role == modelsv1.Role_ROLE_TOOL && strings.HasPrefix(last.Text, modelsv1.ToolFailed):
		say = "The tool " + last.Text + "."
	case last.Role == modelsv1.Role_ROLE_TOOL:
		say = "The answer is " + last.Text + "."
	case sum.MatchString(last.Text):
		m := sum.FindStringSubmatch(last.Text)
		say, tool = "Let me calculate that.", &toolsv1.Run{Name: "calc", Args: m[1] + " " + m[2] + " " + m[3]}
	case wait.MatchString(last.Text):
		say, tool = "Waiting.", &toolsv1.Run{Name: "sleep", Args: wait.FindStringSubmatch(last.Text)[1]}
	}
	for i, word := range strings.Fields(say) {
		if i > 0 {
			word = " " + word
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(s.pace):
			}
		}
		if err := emit(word); err != nil {
			return nil, err
		}
	}
	return tool, nil
}
