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
	"strings"
	"time"
)

var duration time.Duration
var concurrency string

var tests = map[string]func(clusterName, dir string){
	"kv_0":    kv0,
	"kv_95":   kv95,
	"nightly": nightly,
	"splits":  splits,
}

var dirRE = regexp.MustCompile(`([^.]+)\.`)

type testMetadata struct {
	Bin     string
	Cluster string
	Nodes   []int
	Env     string
	Args    []string
	Test    string
	Date    string
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
	if len(d1.runs) == 0 || len(d2.runs) == 0 {
		return &testData{metadata: d1.metadata}, &testData{metadata: d2.metadata}
	}

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
	m := dirRE.FindStringSubmatch(filepath.Base(name))
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
		// TODO(peter): If we're running on existing test, rather than dying let
		// the test upload the correct cockroach binary.
		log.Fatalf("unable to determine cockroach version")
	} else if len(versions) > 1 {
		// TODO(peter): Rather than dying, allow the test to upload the version to
		// run on each node.
		log.Fatalf("mismatched cockroach versions: %v", versions)
	}
	for v := range versions {
		return "cockroach-" + v
	}
	panic("not reached")
}

func testDir(name, vers string) string {
	dir := fmt.Sprintf("%s.%s", name, vers)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}
	return dir
}

func parseConcurrency(s string, numNodes int) (lo int, hi int, step int) {
	// <lo>[-<hi>[/<step>]]
	//
	// If <step> is not specified, assuming numNodes. The <lo> and <hi> values
	// are always multiplied by the number of nodes.

	parts := strings.Split(s, "/")
	switch len(parts) {
	case 1:
		step = numNodes
	case 2:
		var err error
		step, err = strconv.Atoi(parts[1])
		if err != nil {
			log.Fatal(err)
		}
		s = parts[0]
	}

	parts = strings.Split(s, "-")
	switch len(parts) {
	case 1:
		lo, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Fatal(err)
		}
		return lo * numNodes, lo * numNodes, step
	case 2:
		lo, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Fatal(err)
		}
		hi, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Fatal(err)
		}
		return lo * numNodes, hi * numNodes, step
	default:
		log.Fatalf("unable to parse concurrency setting: %s", s)
	}
	return 0, 0, 0
}

func getBin(c *cluster, dir string) {
	bin := filepath.Join(dir, "cockroach")
	if _, err := os.Stat(bin); err == nil {
		return
	}
	t := *c
	t.nodes = t.nodes[:1]
	t.get("./cockroach", bin)
}

func putBin(c *cluster, dir string) error {
	bin := filepath.Join(dir, "cockroach")
	if _, err := os.Stat(bin); err != nil {
		return err
	}
	c.put(bin, "./cockroach")
	return nil
}

func kvTest(clusterName, testName, dir, cmd string) {
	var existing *testMetadata
	if dir != "" {
		existing = &testMetadata{}
		if err := loadJSON(filepath.Join(dir, "metadata"), existing); err != nil {
			log.Fatal(err)
		}
		clusterName = existing.Cluster
		cockroachArgs = existing.Args
	}

	c := testCluster(clusterName)
	m := testMetadata{
		Bin:     cockroachVersion(c),
		Cluster: c.name,
		Nodes:   c.nodes,
		Env:     c.env,
		Args:    c.args,
		Test:    fmt.Sprintf("%s --duration=%s --concurrency=%%d", cmd, duration),
		Date:    time.Now().Format("2006-01-02T15_04_05"),
	}
	if existing == nil {
		dir = testDir(testName, m.Bin)
		saveJSON(filepath.Join(dir, "metadata"), m)
	} else {
		if m.Bin != existing.Bin {
			if err := putBin(c, dir); err != nil {
				log.Fatalf("cockroach binary changed: %s != %s\n%s", m.Bin, existing.Bin, err)
			}
		}
		m.Nodes = existing.Nodes
		m.Env = existing.Env
	}
	fmt.Printf("%s: %s\n", c.name, dir)
	getBin(c, dir)

	lo, hi, step := parseConcurrency(concurrency, len(c.cockroachNodes()))
	for concurrency := lo; concurrency <= hi; concurrency += step {
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

func nightly(clusterName, dir string) {
	var existing *testMetadata
	if dir != "" {
		existing = &testMetadata{}
		if err := loadJSON(filepath.Join(dir, "metadata"), existing); err != nil {
			log.Fatal(err)
		}
		clusterName = existing.Cluster
		cockroachArgs = existing.Args
	}

	cmds := []struct {
		name string
		cmd  string
	}{
		{"kv_0", "./kv --read-percent=0 --splits=1000 --concurrency=384 --duration=10m"},
		{"kv_95", "./kv --read-percent=95 --splits=1000 --concurrency=384 --duration=10m"},
		// TODO(tamird/petermattis): this configuration has been observed to hang
		// indefinitely. Re-enable when it is more reliable.
		//
		// {"splits", "./kv --read-percent=0 --splits=100000 --concurrency=384 --max-ops=1"},
	}

	c := testCluster(clusterName)
	m := testMetadata{
		Bin:     cockroachVersion(c),
		Cluster: c.name,
		Nodes:   c.nodes,
		Env:     c.env,
		Args:    c.args,
		Test:    "nightly",
		Date:    time.Now().Format("2006-01-02T15_04_05"),
	}
	if existing == nil {
		dir = testDir("nightly", m.Bin)
		saveJSON(filepath.Join(dir, "metadata"), m)
	} else {
		if m.Bin != existing.Bin {
			if err := putBin(c, dir); err != nil {
				log.Fatalf("cockroach binary changed: %s != %s\n%s", m.Bin, existing.Bin, err)
			}
		}
		m.Nodes = existing.Nodes
		m.Env = existing.Env
	}
	fmt.Printf("%s: %s\n", c.name, dir)
	getBin(c, dir)

	for _, cmd := range cmds {
		runName := fmt.Sprint(cmd.name)
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
			stdout := io.MultiWriter(f, os.Stdout)
			stderr := io.MultiWriter(f, os.Stderr)
			return c.runLoad(cmd.cmd, stdout, stderr)
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

func splits(clusterName, dir string) {
	var existing *testMetadata
	if dir != "" {
		existing = &testMetadata{}
		if err := loadJSON(filepath.Join(dir, "metadata"), existing); err != nil {
			log.Fatal(err)
		}
		clusterName = existing.Cluster
		cockroachArgs = existing.Args
	}

	const cmd = "./kv --splits=500000 --concurrency=384 --max-ops=1"
	c := testCluster(clusterName)
	m := testMetadata{
		Bin:     cockroachVersion(c),
		Cluster: c.name,
		Nodes:   c.nodes,
		Env:     c.env,
		Args:    c.args,
		Test:    "splits",
		Date:    time.Now().Format("2006-01-02T15_04_05"),
	}
	if existing == nil {
		dir = testDir("splits", m.Bin)
		saveJSON(filepath.Join(dir, "metadata"), m)
	} else {
		if m.Bin != existing.Bin {
			if err := putBin(c, dir); err != nil {
				log.Fatalf("cockroach binary changed: %s != %s\n%s", m.Bin, existing.Bin, err)
			}
		}
		m.Nodes = existing.Nodes
		m.Env = existing.Env
	}
	fmt.Printf("%s: %s\n", c.name, dir)
	getBin(c, dir)

	for i := 1; i <= 100; i++ {
		runName := fmt.Sprint(i)
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
			stdout := io.MultiWriter(f, os.Stdout)
			stderr := io.MultiWriter(f, os.Stderr)
			if err := c.runLoad(cmd, stdout, stderr); err != nil {
				return err
			}
			c.stop()
			time.Sleep(5 * time.Second)

			const metaCheck = `./cockroach debug meta-check /mnt/data1/cockroach`
			return c.run(stdout, c.cockroachNodes(), []string{metaCheck})
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
