// Command loadgen is the project's S3 load generator. It speaks the S3 REST
// API with plain net/http and the repository's own SigV4 signer, so it has
// no dependency on an AWS SDK.
//
//	loadgen run       drive PUT / GET / mixed load and write a JSON result
//	loadgen summarize print medians of repeated runs as a Markdown table
//	loadgen gap       longest window without a successful op after a time
//	loadgen report    all scenario tables for a bench/run.sh results directory
//
// Every PUT body is fresh pseudo-random data (ChaCha8 stream): the store
// deduplicates identical content, so reusing a buffer would measure
// deduplication instead of writes.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(os.Args[2:])
	case "summarize":
		err = summarizeCmd(os.Args[2:])
	case "gap":
		err = gapCmd(os.Args[2:])
	case "report":
		err = reportCmd(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: loadgen run|summarize|gap|report [flags]  (-h for flags)")
	os.Exit(2)
}
