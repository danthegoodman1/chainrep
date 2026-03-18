package coordserver_test

import (
	"testing"

	"github.com/danthegoodman1/chainrep/coordserver"
	"github.com/danthegoodman1/chainrep/coordserver/hastoretest"
)

func TestInMemoryHAStoreConformance(t *testing.T) {
	hastoretest.Run(t, func(t *testing.T) coordserver.HAStore {
		t.Helper()
		return coordserver.NewInMemoryHAStore()
	})
}
