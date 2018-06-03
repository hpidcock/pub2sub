package main

import (
	"testing"
)

func BenchmarkCalc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bruteforce("c26a3636-2b7b-46ff-b839-e4e439ac698d",
			"c26a3636-2b7b-46ff-b839-e4e439ac698d-1527943698-")
	}
}
