package main

import (
	"fmt"
	"log"

	"github.com/dustinkirkland/golang-petname"
	"github.com/howeyc/crc16"
)

const (
	width = 16384
)

func bruteforce(slot int, to int) {
	for {
		text := fmt.Sprintf("gc-slots-%d-%d-%s", slot, to, petname.Generate(3, "-"))
		checksum := int(crc16.Checksum([]byte(text), crc16.CCITTFalseTable))
		calcSlot := checksum % width
		if calcSlot == slot {
			fmt.Println(text)
			break
		}
	}
}

func main() {
	if crc16.Checksum([]byte("123456789"), crc16.CCITTFalseTable) != 0x31c3 {
		log.Fatal("bad sum")
	}

	for i := 0; i < 16384; i += 128 {
		bruteforce(i, i+127)
	}
}
