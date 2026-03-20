package api

import (
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
)

func TestBackoff(t *testing.T) {

	p := backoff.NewExponentialBackOff()
	p.InitialInterval = 30 * time.Second
	p.MaxInterval = 5 * time.Minute
	p.RandomizationFactor = 0.0

	total := time.Duration(0)

	for i := range 25 {
		bo := p.NextBackOff()
		total += bo
		t.Log("Backoff", i, "this", bo, "total", total)
	}

	t.Fail()

}
