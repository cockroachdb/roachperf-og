package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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

func isTest(name string) bool {
	_, ok := tests[name]
	return ok
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

func testCluster(name string) *cluster {
	c, err := newCluster(name)
	if err != nil {
		log.Fatal(err)
	}
	if c.loadGen == 0 {
		log.Fatalf("%s: no load generator node specified", c.name)
	}
	return c
}

func cockroachVersion(c *cluster) string {
	versions := c.cockroachVersions()
	if len(versions) == 0 {
		log.Fatalf("unable to determine cockroach version")
	} else if len(versions) > 1 {
		log.Fatalf("mismatched cockroach versions: %v", versions)
	}
	for v := range versions {
		return "cockroach-" + v
	}
	panic("not reached")
}

func testDir(name, vers string) string {
	dir := fmt.Sprintf("%s.%s.%s", time.Now().Format("2006-01-02T15_04_05"), name, vers)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}
	return dir
}

type testMetadata struct {
	Bin  string
	Env  string
	Test string
}

func prettyJSON(v interface{}) string {
	data, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	return string(data)
}

func saveJSON(path string, v interface{}) {
	data, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	if err := ioutil.WriteFile(path, data, 0666); err != nil {
		log.Fatal(err)
	}
}

func kv95(clusterName string) {
	c := testCluster(clusterName)
	m := testMetadata{
		Env: c.env,
		Bin: cockroachVersion(c),
		Test: fmt.Sprintf(
			"./kv --duration=%s --read-percent=95 --splits=1000 --concurrency=%%d",
			duration),
	}
	dir := testDir("kv_95", m.Bin)
	saveJSON(filepath.Join(dir, "metadata"), m)

	for i := 1; i <= 64; i++ {
		func() {
			concurrency := i * c.count
			f, err := os.Create(fmt.Sprintf("%s/%d", dir, concurrency))
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			c.wipe()
			c.start()
			cmd := fmt.Sprintf(m.Test, concurrency)
			stdout := io.MultiWriter(f, os.Stdout)
			stderr := io.MultiWriter(f, os.Stderr)
			c.runLoad(cmd, stdout, stderr)
		}()
	}
	c.stop()
}
