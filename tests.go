package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"
)

var duration time.Duration

var tests = map[string]func(clusterName string){
	"kv_95": kv95,
}

func registerTest(name string, fn func(clusterName string)) {
	if _, ok := tests[name]; ok {
		log.Fatalf("%s is an already registered test name", name)
	}
	tests[name] = fn
}

func runTest(name, clusterName string) error {
	fn := tests[name]
	if fn == nil {
		return fmt.Errorf("unknown test: %s", name)
	}
	fn(clusterName)
	return nil
}

func allTests() []string {
	var r []string
	for k := range tests {
		r = append(r, k)
	}
	sort.Strings(r)
	return r
}

func kv95(clusterName string) {
	c, err := newCluster(clusterName)
	if err != nil {
		log.Fatal(err)
	}
	versions := c.cockroachVersions()
	if len(versions) == 0 {
		log.Fatalf("unknown to determine cockroach version")
	} else if len(versions) > 1 {
		log.Fatalf("mismatched cockroach versions: %v", versions)
	}
	var vers string
	for v := range versions {
		vers = v
		break
	}

	dir := fmt.Sprintf("%s.kv_95.%s", time.Now().Format("2006-01-02T15_04_05"), vers)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 1; i++ {
		func() {
			concurrency := i * c.count
			f, err := os.Create(fmt.Sprintf("%s/%d", dir, concurrency))
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			c.wipe()
			c.start()
			baseCmd := fmt.Sprintf("./kv --duration=%s --read-percent=95 --splits=1000", duration)
			c.runLoad(fmt.Sprintf("%s --concurrency=%d", baseCmd, concurrency),
				io.MultiWriter(f, os.Stdout), io.MultiWriter(f, os.Stderr))
			c.stop()
		}()
	}
}
