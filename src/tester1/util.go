package tester

import "log"

// Debugging
const Lab3 = false
const Lab4 = true

func DPrintf(format string, a ...interface{}) {
	if Lab3 {
		log.Printf(format, a...)
	}
}

func D4Printf(format string, a ...interface{}) {
	if Lab4 {
		log.Printf(format, a...)
	}
}