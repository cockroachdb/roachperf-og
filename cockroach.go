package main

import (
	"fmt"
	"strings"
)

type cockroach struct{}

func (r cockroach) start(c *cluster) {
	display := fmt.Sprintf("%s: starting", c.name)
	host1 := c.host(1)
	nodes := c.serverNodes()
	c.parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		host := c.host(nodes[i])
		user := c.user(nodes[i])
		join := host1
		session, err := newSSHSession(user, host)
		if err != nil {
			return nil, err
		}
		defer session.Close()

		var args []string
		if c.secure {
			args = append(args, "--certs-dir=certs")
		} else {
			args = append(args, "--insecure")
		}
		args = append(args, "--store=path=/mnt/data1")
		args = append(args, "--logtostderr")
		args = append(args, "--log-dir=")
		args = append(args, "--background")
		args = append(args, "--cache=50%")
		args = append(args, "--max-sql-memory=10%")
		if join != host {
			args = append(args, "--join="+join)
		}
		args = append(args, c.args...)
		cmd := c.env + " " + binary + " start " + strings.Join(args, " ") +
			" > cockroach.stdout 2> cockroach.stderr"
		return session.CombinedOutput(cmd)
	})

	// Check to see if node 1 was started indicating the cluster was
	// bootstrapped.
	var bootstrapped bool
	for _, i := range nodes {
		if i == 1 {
			bootstrapped = true
			break
		}
	}

	if bootstrapped {
		var msg string
		display = fmt.Sprintf("%s: initializing cluster settings", c.name)
		c.parallel(display, 1, 0, func(i int) ([]byte, error) {
			session, err := newSSHSession(c.user(1), c.host(1))
			if err != nil {
				return nil, err
			}
			defer session.Close()

			cmd := `./cockroach sql --url ` + r.nodeURL(c, "localhost") + ` -e "
set cluster setting kv.allocator.stat_based_rebalancing.enabled = false;
set cluster setting server.remote_debugging.mode = 'any';
"`
			out, err := session.CombinedOutput(cmd)
			if err != nil {
				msg = err.Error()
			} else {
				msg = strings.TrimSpace(string(out))
			}
			return nil, nil
		})

		fmt.Println(msg)
	}
}

func (cockroach) nodeURL(c *cluster, host string) string {
	url := fmt.Sprintf("'postgres://root@%s:26257", host)
	if c.secure {
		url += "?sslcert=certs%2Fnode.crt&sslkey=certs%2Fnode.key&" +
			"sslrootcert=certs%2Fca.crt&sslmode=verify-full"
	} else {
		url += "?sslmode=disable"
	}
	url += "'"
	return url
}
