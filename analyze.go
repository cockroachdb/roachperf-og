package main

import (
	"fmt"
)

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
