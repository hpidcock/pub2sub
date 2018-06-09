package workerpool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorkPool(t *testing.T) {
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	wp := New(ctx)

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		err = wp.Push(func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
		})
		assert.Nil(t, err)
	}
	wg.Wait()

	cancel()

	time.Sleep(time.Millisecond)

	err = wp.Push(func() {
		assert.FailNow(t, "never should be run")
	})
	assert.Equal(t, context.Canceled, err)
}
