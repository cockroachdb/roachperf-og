package main

import (
	"fmt"
)

func dump(dirs []string) error {
	switch n := len(dirs); n {
	case 0:
		return fmt.Errorf("no test directory specified")
	case 1, 2:
		d1, err := loadTestData(dirs[0])
		if err != nil {
			return err
		}
		if n == 1 {
			return dump1(d1)
		}
		d2, err := loadTestData(dirs[1])
		if err != nil {
			return err
		}
		return dump2(d1, d2)
	default:
		return fmt.Errorf("too many test directories: %s", dirs)
	}
}

func dump1(d *testData) error {
	fmt.Println(d.metadata.Test)
	fmt.Println("_____N_____ops/sec__avg(ms)__p50(ms)__p95(ms)__p99(ms)")
	for _, r := range d.runs {
		fmt.Printf("%6d %10.1f %8.1f %8.1f %8.1f %8.1f\n", r.concurrency,
			r.opsSec, r.avgLat, r.p50Lat, r.p95Lat, r.p99Lat)
	}
	return nil
}

func dump2(d1, d2 *testData) error {
	d1, d2 = alignTestData(d1, d2)
	fmt.Println(d1.metadata.Test)
	fmt.Println("_____N__ops/sec(1)__ops/sec(2)_____delta")
	for i := range d1.runs {
		r1 := d1.runs[i]
		r2 := d2.runs[i]
		fmt.Printf("%6d %11.1f %11.1f %8.2f%%\n",
			r1.concurrency, r1.opsSec, r2.opsSec, 100*(r2.opsSec-r1.opsSec)/r1.opsSec)
	}
	return nil
}
