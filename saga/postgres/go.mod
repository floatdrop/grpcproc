// Separate module, so grpcproc itself does not depend on a database. The
// drivers and the embedded PostgreSQL the tests use are its requirements:
// an application that imports it has them in its module graph, though it
// builds none of them. Each minor release pins it to the core of the same
// version (RELEASING.md). For local work on it and the core at once, use a
// go.work (ignored by git).
module github.com/floatdrop/grpcproc/saga/postgres

go 1.27.1

require (
	github.com/fergusstrange/embedded-postgres v1.34.0
	github.com/floatdrop/grpcproc v0.8.1-0.20261004183920-b2e97c5fa5c2
	github.com/jackc/pgx/v5 v5.11.0
	github.com/lib/pq v1.10.9
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/floatdrop/fsm v0.8.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/xi2/xz v0.0.0-20171230120015-48954b6210f8 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
)
