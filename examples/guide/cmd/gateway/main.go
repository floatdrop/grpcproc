// Command gateway is the node that faces browsers: the web front and the
// conversations. Its configuration places the tools on another node; the
// conversations find the models in their group.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/conversations"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	"github.com/floatdrop/grpcproc/examples/guide/internal/web"
)

func main() { platform.Run(conversations.Module, web.Module) }
