package deadline_list

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeadlineList(t *testing.T) {
	l := New(1 * time.Millisecond)
	item := l.Pop()
	assert.Nil(t, item)

	idA := l.Push("a")
	assert.Equal(t, int64(0), idA)
	idB := l.Push("b")
	assert.Equal(t, int64(1), idB)
	idC := l.Push("c")
	assert.Equal(t, int64(2), idC)

	time.Sleep(2 * time.Millisecond)

	c := l.Remove(idC)
	assert.Equal(t, "c", c)

	z := l.Remove(idC)
	assert.Nil(t, z)

	a := l.Pop()
	assert.Equal(t, "a", a)
	b := l.Remove(idB)
	assert.Equal(t, "b", b)

	idD := l.Push("d")
	assert.Equal(t, int64(3), idD)
	idE := l.Push("e")
	assert.Equal(t, int64(4), idE)

	rest := l.Close()
	assert.Len(t, rest, 2)
	assert.Contains(t, rest, "d")
	assert.Contains(t, rest, "e")
}

func TestDeadlineListTimed(t *testing.T) {
	l := New(50 * time.Millisecond)

	l.Push("a")
	l.Push("b")
	idC := l.Push("c")
	l.Push("d")

	<-l.Wait()
	a := l.Pop()
	assert.Equal(t, "a", a)

	<-l.Wait()
	b := l.Pop()
	assert.Equal(t, "b", b)

	c := l.Remove(idC)
	assert.Equal(t, "c", c)

	<-l.Wait()
	d := l.Pop()
	assert.Equal(t, "d", d)
}
