package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

const (
	defaultHostDir = "${HOME}/.roachprod/hosts"
	local          = "local"
)

func newInvalidHostsLineErr(line string) error {
	return fmt.Errorf("invalid hosts line, expected <username>@<host> [locality], got %q", line)
}

func loadClusters() error {
	hd := os.ExpandEnv(defaultHostDir)
	files, err := ioutil.ReadDir(hd)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}

		filename := filepath.Join(hd, file.Name())
		contents, err := ioutil.ReadFile(filename)
		if err != nil {
			return errors.Wrapf(err, "could not read %s", filename)
		}
		lines := strings.Split(string(contents), "\n")

		c := &cluster{
			name: file.Name(),
		}

		for _, l := range lines {
			fields := strings.Fields(l)
			if len(fields) == 0 {
				continue
			} else if len(fields[0]) > 0 && fields[0][0] == '#' {
				// Comment line.
				continue
			} else if len(fields) > 2 {
				return newInvalidHostsLineErr(l)
			}

			parts := strings.Split(fields[0], "@")
			var n, u string
			if len(parts) == 1 {
				user, err := user.Current()
				if err != nil {
					return errors.Wrapf(err, "failed to lookup current user")
				}
				u = user.Username
				n = parts[0]
			} else if len(parts) == 2 {
				u = parts[0]
				n = parts[1]
			} else {
				return newInvalidHostsLineErr(l)
			}

			var l string
			if len(fields) == 2 {
				l = fields[1]
			}

			c.vms = append(c.vms, n)
			c.users = append(c.users, u)
			c.localities = append(c.localities, l)
		}
		clusters[file.Name()] = c
	}

	clusters[local] = &cluster{
		name: local,
	}
	return nil
}
