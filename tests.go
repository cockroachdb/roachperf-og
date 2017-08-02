package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

var duration time.Duration

var tests = map[string]func(clusterName, dir string){
	"kv_0":  kv0,
	"kv_95": kv95,
}

var dirRE = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}_[0-9]{2}_[0-9]{2}\.([^.]+)\.`)

func findTest(name string) (_ func(clusterName, dir string), dir string) {
	fn := tests[name]
	if fn != nil {
		return fn, ""
	}
	m := dirRE.FindStringSubmatch(name)
	if len(m) != 2 {
		return nil, ""
	}
	return tests[m[1]], name
}

func isTest(name string) bool {
	fn, _ := findTest(name)
	return fn != nil
}

func runTest(name, clusterName string) error {
	fn, dir := findTest(name)
	if fn == nil {
		return fmt.Errorf("unknown test: %s", name)
	}
	fn(clusterName, dir)
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
	Bin   string
	Nodes int
	Env   string
	Test  string
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

func kvTest(clusterName, testName, dir, cmd string) {
	c := testCluster(clusterName)
	m := testMetadata{
		Bin:   cockroachVersion(c),
		Nodes: c.count,
		Env:   c.env,
		Test:  fmt.Sprintf("%s --duration=%s --concurrency=%%d", cmd, duration),
	}
	if dir == "" {
		dir = testDir(testName, m.Bin)
		saveJSON(filepath.Join(dir, "metadata"), m)
	} else {
		existing := &testMetadata{}
		if err := loadJSON(filepath.Join(dir, "metadata"), existing); err != nil {
			log.Fatal(err)
		}
		if m.Bin != existing.Bin {
			log.Fatalf("cockroach binary changed: %s != %s", m.Bin, existing.Bin)
		}
		if m.Nodes != existing.Nodes {
			log.Fatalf("node count changed: %d != %d", m.Nodes, existing.Nodes)
		}
		if m.Env != existing.Env {
			log.Fatalf("environment changed: \"%s\" != \"%s\"", m.Env, existing.Env)
		}
	}
	fmt.Printf("%s: %s\n", c.name, dir)

	for i := 1; i <= 64; i++ {
		concurrency := i * c.count
		runName := fmt.Sprint(concurrency)
		if run, err := loadTestRun(dir, runName); err == nil && run != nil {
			continue
		}

		err := func() error {
			f, err := os.Create(filepath.Join(dir, runName))
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			c.wipe()
			c.start()
			cmd := fmt.Sprintf(m.Test, concurrency)
			stdout := io.MultiWriter(f, os.Stdout)
			stderr := io.MultiWriter(f, os.Stderr)
			return c.runLoad(cmd, stdout, stderr)
		}()
		if err != nil {
			if !isSigKill(err) {
				fmt.Printf("%s\n", err)
			}
			break
		}
	}
	c.stop()
}

func kv0(clusterName, dir string) {
	kvTest(clusterName, "kv_0", dir, "./kv --read-percent=0 --splits=1000")
}

func kv95(clusterName, dir string) {
	kvTest(clusterName, "kv_95", dir, "./kv --read-percent=95 --splits=1000")
}
