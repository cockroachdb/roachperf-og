package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type cluster struct {
	name       string
	nodes      []int
	loadGen    int
	secure     bool
	hostFormat string
	env        string
	args       []string
}

func (c *cluster) host(index int) string {
	return fmt.Sprintf(c.hostFormat, c.name, index)
}

func (c *cluster) cockroachNodes() []int {
	if c.loadGen == -1 {
		return c.nodes
	}
	newNodes := make([]int, 0, len(c.nodes))
	for _, i := range c.nodes {
		if i != c.loadGen {
			newNodes = append(newNodes, i)
		}
	}
	return newNodes
}

func (c *cluster) startNode(host, join string) ([]byte, error) {
	session, err := newSSHSession("cockroach", host)
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
	args = append(args, "--store=path=/mnt/data1/cockroach")
	// args = append(args, "--log-dir=/home/cockroach/logs")
	args = append(args, "--logtostderr")
	args = append(args, "--log-dir=")
	args = append(args, "--background")
	if join != host {
		args = append(args, "--join="+join)
	}
	args = append(args, c.args...)
	cmd := c.env + " ./cockroach start " + strings.Join(args, " ") +
		"> logs/cockroach.stdout 2> logs/cockroach.stderr"
	return session.CombinedOutput(cmd)
}

func (c *cluster) start() {
	display := fmt.Sprintf("%s: starting", c.name)
	host1 := c.host(1)
	nodes := c.cockroachNodes()
	bootstrapped := false
	c.parallel(display, len(nodes), func(i int) ([]byte, error) {
		if nodes[i] == 1 {
			bootstrapped = true
		}
		return c.startNode(c.host(nodes[i]), host1)
	})

	if bootstrapped {
		var msg string
		display = fmt.Sprintf("%s: initializing cluster settings", c.name)
		c.parallel(display, 1, func(i int) ([]byte, error) {
			session, err := newSSHSession("cockroach", c.host(1))
			if err != nil {
				return nil, err
			}
			defer session.Close()

			cmd := `./cockroach sql --url '` + c.pgURL(26257) + `' -e "
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

func (c *cluster) stop() {
	display := fmt.Sprintf("%s: stopping", c.name)
	c.parallel(display, len(c.nodes), func(i int) ([]byte, error) {
		session, err := newSSHSession("cockroach", c.host(c.nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		const cmd = `
sudo pkill -9 "cockroach|java|mongo|kv|ycsb" || true ;
sudo kill -9 $(lsof -t -i :26257 -i :27183) 2>/dev/null || true ;
`
		return session.CombinedOutput(cmd)
	})
}

func (c *cluster) wipe() {
	display := fmt.Sprintf("%s: wiping", c.name)
	c.parallel(display, len(c.nodes), func(i int) ([]byte, error) {
		session, err := newSSHSession("cockroach", c.host(c.nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		const cmd = `
sudo pkill -9 "cockroach|java|mongo|kv|ycsb" || true ;
sudo kill -9 $(lsof -t -i :26257 -i :27183) 2>/dev/null || true ;
sudo find /mnt/data* -maxdepth 1 -type f -exec rm -f {} \; ;
sudo rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,mongo-data} \; ;
sudo find /home/cockroach/logs -type f -not -name supervisor.log -exec rm -f {} \; ;
`
		return session.CombinedOutput(cmd)
	})
}

func (c *cluster) status() {
	display := fmt.Sprintf("%s: status", c.name)
	results := make([]string, len(c.nodes))
	c.parallel(display, len(c.nodes), func(i int) ([]byte, error) {
		session, err := newSSHSession("cockroach", c.host(c.nodes[i]))
		if err != nil {
			results[i] = err.Error()
			return nil, nil
		}
		defer session.Close()

		const cmd = `
out=$(sudo lsof -i :26257 -i :27183 | awk '!/COMMAND/ {print $1, $2}' | sort | uniq);
vers=$(./cockroach version 2>/dev/null | awk '/Build Tag:/ {print $NF}')
if [ -n "${out}" -a -n "${vers}" ]; then
  echo ${out} | sed "s/cockroach/cockroach-${vers}/g"
else
  echo ${out}
fi
`
		out, err := session.CombinedOutput(cmd)
		var msg string
		if err != nil {
			msg = err.Error()
		} else {
			msg = strings.TrimSpace(string(out))
			if msg == "" {
				msg = "not running"
			}
		}
		results[i] = msg
		return nil, nil
	})

	for i, r := range results {
		fmt.Printf("  %2d: %s\n", c.nodes[i], r)
	}
}

func (c *cluster) run(w io.Writer, nodes []int, args []string) error {
	cmd := strings.TrimSpace(strings.Join(args, " "))
	short := cmd
	if len(cmd) > 30 {
		short = cmd[:27] + "..."
	}

	display := fmt.Sprintf("%s: %s", c.name, short)
	errors := make([]error, len(nodes))
	results := make([]string, len(nodes))
	c.parallel(display, len(nodes), func(i int) ([]byte, error) {
		session, err := newSSHSession("cockroach", c.host(nodes[i]))
		if err != nil {
			results[i] = err.Error()
			return nil, nil
		}
		defer session.Close()

		out, err := session.CombinedOutput(cmd)
		msg := strings.TrimSpace(string(out))
		if err != nil {
			errors[i] = err
			msg += fmt.Sprintf("\n%v", err)
		}
		results[i] = msg
		return nil, nil
	})

	for i, r := range results {
		fmt.Fprintf(w, "  %2d: %s\n", nodes[i], r)
	}

	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) cockroachVersions() map[string]int {
	sha := make(map[string]int)
	var mu sync.Mutex

	display := fmt.Sprintf("%s: cockroach version", c.name)
	c.parallel(display, len(c.nodes), func(i int) ([]byte, error) {
		session, err := newSSHSession("cockroach", c.host(c.nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := "./cockroach version | awk '/Build Tag:/ {print $NF}'"
		out, err := session.CombinedOutput(cmd)
		var s string
		if err != nil {
			s = err.Error()
		} else {
			s = strings.TrimSpace(string(out))
		}
		mu.Lock()
		sha[s]++
		mu.Unlock()
		return nil, err
	})

	return sha
}

func (c *cluster) runLoad(cmd string, stdout, stderr io.Writer) error {
	if c.loadGen == 0 {
		log.Fatalf("%s: no load generator node specified", c.name)
	}

	session, err := newSSHSession("cockroach", c.host(c.loadGen))
	if err != nil {
		return err
	}
	defer session.Close()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer func() {
		signal.Stop(ch)
		close(ch)
	}()
	go func() {
		_, ok := <-ch
		if ok {
			c.stopLoad()
		}
	}()

	session.Stdout = stdout
	session.Stderr = stderr
	fmt.Fprintln(stdout, cmd)
	return session.Run(cmd + " '" + c.pgURL(27183) + "'")
}

func (c *cluster) pgURL(port int) string {
	url := fmt.Sprintf("postgres://root@localhost:%d", port)
	if c.secure {
		url += "?sslcert=certs%2Fnode.crt&sslkey=certs%2Fnode.key&" +
			"sslrootcert=certs%2Fca.crt&sslmode=verify-full"
	} else {
		url += "?sslmode=disable"
	}
	return url
}

const progressDone = "=======================================>"
const progressTodo = "----------------------------------------"

func formatProgress(p float64) string {
	i := int(math.Ceil(float64(len(progressDone)) * (1 - p)))
	return fmt.Sprintf("[%s%s] %.0f%%", progressDone[i:], progressTodo[:i], 100*p)
}

func (c *cluster) put(src, dest string) {
	// TODO(peter): Only put 10 nodes at a time. When a node completes, output a
	// line indicating that.
	fmt.Printf("%s: putting %s %s\n", c.name, src, dest)

	type result struct {
		index int
		err   error
	}

	var writer uiWriter
	results := make(chan result, len(c.nodes))
	lines := make([]string, len(c.nodes))
	var linesMu sync.Mutex

	var wg sync.WaitGroup
	for i := range c.nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			session, err := newSSHSession("cockroach", c.host(c.nodes[i]))
			if err == nil {
				defer session.Close()
				err = scp(src, dest, func(p float64) {
					linesMu.Lock()
					defer linesMu.Unlock()
					lines[i] = formatProgress(p)
				}, session)
			}
			results <- result{i, err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	haveErr := false

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
		case r, ok := <-results:
			done = !ok
			if ok {
				linesMu.Lock()
				if r.err != nil {
					haveErr = true
					lines[r.index] = r.err.Error()
				} else {
					lines[r.index] = "done"
				}
				linesMu.Unlock()
			}
		}
		linesMu.Lock()
		for i := range lines {
			fmt.Fprintf(&writer, "  %2d: ", c.nodes[i])
			if lines[i] != "" {
				fmt.Fprintf(&writer, "%s", lines[i])
			} else {
				fmt.Fprintf(&writer, "%s", spinner[spinnerIdx%len(spinner)])
			}
			fmt.Fprintf(&writer, "\n")
		}
		linesMu.Unlock()
		writer.Flush(os.Stdout)
		spinnerIdx++
	}

	if haveErr {
		log.Fatal("failed")
	}
}

func (c *cluster) stopLoad() {
	if c.loadGen == 0 {
		log.Fatalf("no load generator node specified for cluster: %s", c.name)
	}

	display := fmt.Sprintf("%s: stopping load", c.name)
	c.parallel(display, 1, func(i int) ([]byte, error) {
		session, err := newSSHSession("cockroach", c.host(c.loadGen))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		const cmd = `sudo kill -9 $(lsof -t -i :27183) 2>/dev/null || true`
		return session.CombinedOutput(cmd)
	})
}

func (c *cluster) parallel(display string, count int, fn func(i int) ([]byte, error)) {
	type result struct {
		index int
		out   []byte
		err   error
	}

	results := make(chan result, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := fn(i)
			results <- result{i, out, err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var writer uiWriter
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	complete := make([]bool, count)
	haveErr := false

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
		case r, ok := <-results:
			done = !ok
			if ok {
				complete[r.index] = true
			}
		}
		fmt.Fprint(&writer, display)
		var n int
		for i := range complete {
			if complete[i] {
				n++
			}
		}
		fmt.Fprintf(&writer, " %d/%d", n, len(complete))
		if !done {
			fmt.Fprintf(&writer, " %s", spinner[spinnerIdx%len(spinner)])
		}
		fmt.Fprintf(&writer, "\n")
		writer.Flush(os.Stdout)
		spinnerIdx++
	}

	if haveErr {
		log.Fatal("failed")
	}
}
