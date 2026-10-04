// Command gpu is a node with a GPU: it runs the model. A deployment runs as
// many as it has, each under its own name.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/models"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
)

func main() { platform.Run(models.Module) }
