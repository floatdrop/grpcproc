// Command sandbox is the node the tools run on, apart from the others: the
// other nodes are told not to trust it, and it offers them its runner and
// nothing else.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	"github.com/floatdrop/grpcproc/examples/guide/internal/tools"
)

func main() { platform.Run(tools.Module) }
