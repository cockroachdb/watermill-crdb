package crdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/watermill-crdb/pkg/crdb/testutils"
)

var PGURL string

func TestMain(m *testing.M) {
	pgURL, cleanup, err := testutils.MaybeStartCockroachCluster()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to set up cockroach: %v\n", err)
		os.Exit(1)
	}
	PGURL = pgURL
	var code int
	defer os.Exit(code)
	defer cleanup()
	code = m.Run()
}
