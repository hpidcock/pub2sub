package main

import (
	"fmt"
	"log"

	"github.com/howeyc/crc16"
)

const (
	width = 16384
)

func bruteforce(same string, target string) string {
	sameSum := crc16.Checksum([]byte(same), crc16.CCITTFalseTable)
	calcSlot := int(sameSum) % width

	checksum := crc16.Checksum([]byte(target), crc16.CCITTFalseTable)
	bytes := make([]byte, 4)
	for a := 'a'; a <= 'z'; a++ {
		bytes[0] = byte(a)
		for b := 'a'; b <= 'z'; b++ {
			bytes[1] = byte(b)
			for c := 'a'; c <= 'z'; c++ {
				bytes[2] = byte(c)
				for d := 'a'; d <= 'z'; d++ {
					bytes[3] = byte(d)
					checksum2 := crc16.Update(checksum, crc16.CCITTFalseTable, bytes)
					calcSlot2 := int(checksum2) % width
					if calcSlot == calcSlot2 {
						return fmt.Sprintf("%s%c%c%c%c", target, a, b, c, d)
					}
				}
			}
		}
	}
	panic("unreachable")
}

func main() {
	if crc16.Checksum([]byte("123456789"), crc16.CCITTFalseTable) != 0x31c3 {
		log.Fatal("bad sum")
	}

	bruteforce("wow", "wow-121222-")
}
