package main

import (
	"fmt"
)

func visualize(dirs []string) error {
	if len(dirs) == 0 {
		return fmt.Errorf("no test directories specified")
	}

	var tests []*testData
	for _, dir := range dirs {
		d, err := loadTestData(dir)
		if err != nil {
			return err
		}
		tests = append(tests, d)
	}
	return nil
}
