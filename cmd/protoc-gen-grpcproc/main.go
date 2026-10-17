// Command protoc-gen-grpcproc is a protoc plugin that writes the Go of a
// process's contract: for each service marked with the grpcproc.v1.process
// option, an address whose methods are the service's rpcs (a Call per rpc,
// a Send and a timer per one marked grpcproc.v1.send, a typed pubsub topic
// per one marked grpcproc.v1.topic), its constructors by the process's name
// or prefix, a handler interface with one method per rpc, the HandleXCall
// and HandleXMessage functions that dispatch a message to it, for an
// actor's HandleCall and HandleMessage, and UnimplementedXHandler to embed.
// The file goes beside protoc-gen-go's, as <name>_grpcproc.pb.go, and takes
// the same module= and paths= parameters. docs/DESIGN.md, "Contracts as
// services", has the forms and the rules.
//
//	go get -tool github.com/floatdrop/grpcproc/cmd/protoc-gen-grpcproc@latest
//
// and in buf.gen.yaml:
//
//	plugins:
//	  - local: ["go", "tool", "protoc-gen-grpcproc"]
//	    out: .
//	    opt: module=example.com/app
package main

import (
	"google.golang.org/protobuf/compiler/protogen"

	"github.com/floatdrop/grpcproc/internal/protocgen"
)

func main() {
	protogen.Options{}.Run(protocgen.Generate)
}
