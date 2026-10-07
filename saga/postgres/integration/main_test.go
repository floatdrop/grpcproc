package integration

import (
	"fmt"
	"net"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

// dsn is the database every test shares, on a PostgreSQL TestMain starts;
// each store is in tables of its own.
var dsn string

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "sagapostgres")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	// PostgreSQL 14, on a port nothing
	// listened on a moment ago: another may take it first, and then the
	// next is tried.
	var port int
	for try := 0; ; try++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		port = ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.V14).Port(uint32(port)).RuntimePath(dir).Logger(nil))
		if err = pg.Start(); err == nil {
			defer func() { _ = pg.Stop() }()
			break
		}
		if try == 4 {
			panic(err)
		}
	}
	dsn = fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres", port)
	return m.Run()
}
