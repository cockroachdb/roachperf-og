package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

var duration time.Duration

var tests = map[string]func(clusterName, dir string){
	"kv_0":  kv0,
	"kv_95": kv95,
}

var dirRE = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}_[0-9]{2}_[0-9]{2}\.([^.]+)\.`)

type testMetadata struct {
	Bin     string
	Cluster string
	Nodes   int
	Env     string
	Test    string
}

type testRun struct {
	concurrency int
	elapsed     float64
	errors      int64
	ops         int64
	opsSec      float64
	avgLat      float64
	p50Lat      float64
	p95Lat      float64
	p99Lat      float64
}

func loadTestRun(dir, name string) (*testRun, error) {
	n, err := strconv.Atoi(name)
	if err != nil {
		return nil, nil
	}
	r := &testRun{concurrency: n}

	b, err := ioutil.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}

	const header = `_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)`
	i := bytes.Index(b, []byte(header))
	if i == -1 {
		return nil, nil
	}
	b = b[i+len(header)+1:]

	_, err = fmt.Fscanf(bytes.NewReader(b), " %fs %d %d %f %f %f %f %f",
		&r.elapsed, &r.errors, &r.ops, &r.opsSec, &r.avgLat, &r.p50Lat, &r.p95Lat, &r.p99Lat)
	if err != nil {
		return nil, err
	}
	return r, nil
}

type testData struct {
	metadata testMetadata
	runs     []*testRun
}

func loadTestData(dir string) (*testData, error) {
	d := &testData{}
	if err := loadJSON(filepath.Join(dir, "metadata"), &d.metadata); err != nil {
		return nil, err
	}

	ents, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, e := range ents {
		r, err := loadTestRun(dir, e.Name())
		if err != nil {
			return nil, err
		}
		if r != nil {
			d.runs = append(d.runs, r)
		}
	}

	sort.Slice(d.runs, func(i, j int) bool {
		return d.runs[i].concurrency < d.runs[j].concurrency
	})
	return d, nil
}

func (d *testData) exists(concurrency int) bool {
	i := sort.Search(len(d.runs), func(j int) bool {
		return d.runs[j].concurrency >= concurrency
	})
	return i < len(d.runs) && d.runs[i].concurrency == concurrency
}

func (d *testData) get(concurrency int) *testRun {
	i := sort.Search(len(d.runs), func(j int) bool {
		return d.runs[j].concurrency >= concurrency
	})
	if i+1 >= len(d.runs) {
		return d.runs[len(d.runs)-1]
	}
	if i < 0 {
		return d.runs[0]
	}
	a := d.runs[i]
	b := d.runs[i+1]
	t := float64(concurrency-a.concurrency) / float64(b.concurrency-a.concurrency)
	return &testRun{
		concurrency: concurrency,
		elapsed:     a.elapsed + float64(b.elapsed-a.elapsed)*t,
		ops:         a.ops + int64(float64(b.ops-a.ops)*t),
		opsSec:      a.opsSec + float64(b.opsSec-a.opsSec)*t,
		avgLat:      a.avgLat + float64(b.avgLat-a.avgLat)*t,
		p50Lat:      a.p50Lat + float64(b.p50Lat-a.p50Lat)*t,
		p95Lat:      a.p95Lat + float64(b.p95Lat-a.p95Lat)*t,
		p99Lat:      a.p99Lat + float64(b.p99Lat-a.p99Lat)*t,
	}
}

func alignTestData(d1, d2 *testData) (*testData, *testData) {
	minConcurrency := d1.runs[0].concurrency
	if c := d2.runs[0].concurrency; minConcurrency < c {
		minConcurrency = c
	}
	maxConcurrency := d1.runs[len(d1.runs)-1].concurrency
	if c := d2.runs[len(d2.runs)-1].concurrency; maxConcurrency > c {
		maxConcurrency = c
	}

	var r1 []*testRun
	var r2 []*testRun
	for i := minConcurrency; i <= maxConcurrency; i++ {
		if !d1.exists(i) && !d2.exists(i) {
			continue
		}
		r1 = append(r1, d1.get(i))
		r2 = append(r2, d2.get(i))
	}

	d1 = &testData{
		metadata: d1.metadata,
		runs:     r1,
	}
	d2 = &testData{
		metadata: d2.metadata,
		runs:     r2,
	}
	return d1, d2
}

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

func kvTest(clusterName, testName, dir, cmd string) {
	var existing *testMetadata
	if dir != "" {
		existing = &testMetadata{}
		if err := loadJSON(filepath.Join(dir, "metadata"), existing); err != nil {
			log.Fatal(err)
		}
		clusterName = existing.Cluster
	}

	c := testCluster(clusterName)
	m := testMetadata{
		Bin:     cockroachVersion(c),
		Cluster: c.name,
		Nodes:   c.count,
		Env:     c.env,
		Test:    fmt.Sprintf("%s --duration=%s --concurrency=%%d", cmd, duration),
	}
	if existing == nil {
		dir = testDir(testName, m.Bin)
		saveJSON(filepath.Join(dir, "metadata"), m)
	} else {
		if m.Bin != existing.Bin {
			log.Fatalf("cockroach binary changed: %s != %s", m.Bin, existing.Bin)
		}
		m.Nodes = existing.Nodes
		m.Env = existing.Env
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
