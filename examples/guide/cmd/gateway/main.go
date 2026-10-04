// Command gateway is the node that faces browsers: the web front and the
// conversations. Its configuration places the models and the tools on the
// other nodes.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/conversations"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	"github.com/floatdrop/grpcproc/examples/guide/internal/web"
)

func main() { platform.Run(conversations.Module, web.Module) }
