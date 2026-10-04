// Command local runs the whole runtime as one program, the way a developer
// runs it: one node, every service on it. It composes the same modules that
// cmd/gateway, cmd/gpu and cmd/sandbox split between their nodes.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/conversations"
	"github.com/floatdrop/grpcproc/examples/guide/internal/models"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	"github.com/floatdrop/grpcproc/examples/guide/internal/tools"
	"github.com/floatdrop/grpcproc/examples/guide/internal/web"
)

func main() {
	platform.Run(models.Module, tools.Module, conversations.Module, web.Module)
}
