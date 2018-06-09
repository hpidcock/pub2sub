package workerpool

import (
	"context"
	"runtime"
)

type WorkerPool struct {
	acquireWorker chan interface{}
	jobChan       chan func()
	ctx           context.Context
	count         int
}

func New(ctx context.Context) *WorkerPool {
	count := runtime.NumCPU() * 10
	pw := &WorkerPool{
		ctx:           ctx,
		jobChan:       make(chan func(), 0),
		acquireWorker: make(chan interface{}, count),
		count:         count,
	}
	for i := 0; i < count; i++ {
		pw.acquireWorker <- nil
		go pw.run()
	}
	go pw.close()
	return pw
}

func (wp *WorkerPool) Push(fn func()) error {
	_, ok := <-wp.acquireWorker
	if ok == false {
		return context.Canceled
	}
	wp.jobChan <- fn
	return nil
}

func (wp *WorkerPool) close() {
	<-wp.ctx.Done()
	for i := 0; i < wp.count; i++ {
		<-wp.acquireWorker
	}
	close(wp.acquireWorker)
	close(wp.jobChan)
}

func (wp *WorkerPool) run() {
	closeChan := wp.ctx.Done()
	for {
		select {
		case <-closeChan:
			return
		case job, ok := <-wp.jobChan:
			if ok == false {
				return
			}
			job()
			wp.acquireWorker <- nil
		}
	}
}
