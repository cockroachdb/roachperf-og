package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
)

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

func loadJSON(path string, v interface{}) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
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

func analyze(dirs []string) error {
	if len(dirs) == 0 {
		return fmt.Errorf("no test directories specified")
	}

	var tests []*testData
	for _, dir := range dirs {
		d, err := loadTestData(dir)
		if err != nil {
			return err
		}
		for _, r := range d.runs {
			fmt.Printf("  %3d %8.1f %5.1f %5.1f %5.1f %5.1f\n", r.concurrency,
				r.opsSec, r.avgLat, r.p50Lat, r.p95Lat, r.p99Lat)
		}
		tests = append(tests, d)
	}
	return nil
}
