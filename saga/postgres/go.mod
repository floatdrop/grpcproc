// Separate module, so grpcproc itself does not depend on a database. It
// requires no driver: its tests, with the drivers and the embedded
// PostgreSQL they use, are the module in integration/. Each minor release
// pins it to the core of the same version (RELEASING.md). For local work on
// it and the core at once, use a go.work (ignored by git).
module github.com/floatdrop/grpcproc/saga/postgres

go 1.27.1

require github.com/floatdrop/grpcproc v0.11.0

require (
	github.com/floatdrop/fsm v0.8.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
