package main

import (
	"fmt"
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
	name    string
	count   int
	total   int
	loadGen int
	secure  bool
	env     string
}

func (c *cluster) host(index int) string {
	return fmt.Sprintf("cockroach-%s-%04d.crdb.io", c.name, index)
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
	args = append(args, "--background")
	if join != host {
		args = append(args, "--join="+join)
	}
	cmd := c.env + " ./cockroach start " + strings.Join(args, " ") +
		"> logs/cockroach.stdout 2> logs/cockroach.stderr"
	return session.CombinedOutput(cmd)
}

func (c *cluster) start() {
	display := fmt.Sprintf("%s: starting", c.name)
	host1 := c.host(1)
	c.parallel(display, 1, c.count, func(host string) ([]byte, error) {
		return c.startNode(host, host1)
	})
}

func (c *cluster) stopNode(host string) ([]byte, error) {
	session, err := newSSHSession("cockroach", host)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	const cmd = `
sudo pkill -9 "cockroach|java|mongo" || true ;
sudo kill -9 $(lsof -t -i :26257 -i :27183) 2>/dev/null || true ;
`
	return session.CombinedOutput(cmd)
}

func (c *cluster) stop() {
	display := fmt.Sprintf("%s: stopping", c.name)
	c.parallel(display, 1, c.total, c.stopNode)
}

func (c *cluster) wipeNode(host string) ([]byte, error) {
	session, err := newSSHSession("cockroach", host)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	const cmd = `
sudo pkill -9 "cockroach|java|mongo" || true ;
sudo kill -9 $(lsof -t -i :26257 -i :27183) 2>/dev/null || true ;
sudo find /mnt/data* -maxdepth 1 -type f -exec rm -f {} \; ;
sudo rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,mongo-data} \; ;
sudo find /home/cockroach/logs -type f -not -name supervisor.log -exec rm -f {} \; ;
`
	return session.CombinedOutput(cmd)
}

func (c *cluster) wipe() {
	display := fmt.Sprintf("%s: wiping", c.name)
	c.parallel(display, 1, c.total, c.wipeNode)
}

func (c *cluster) status() {
	fmt.Printf("%s: status\n", c.name)
	results := make([]chan string, c.total)
	for i := 0; i < c.total; i++ {
		results[i] = make(chan string, 1)
		go func(i int) {
			session, err := newSSHSession("cockroach", c.host(i+1))
			if err != nil {
				results[i] <- err.Error()
				return
			}
			defer session.Close()

			cmd := "lsof -i :26257 -i :27183 | awk '!/COMMAND/ {print $1, $2}' | sort | uniq"
			out, err := session.CombinedOutput(cmd)
			var msg string
			if err != nil {
				msg = err.Error()
			} else {
				msg = strings.TrimSpace(string(out))
				if msg == "" {
					msg = "cockroach not running"
					if i+1 == c.loadGen {
						msg = "not running"
					}
				}
			}
			results[i] <- msg
		}(i)
	}

	for i, r := range results {
		s := <-r
		fmt.Printf("  %2d: %s\n", i+1, s)
	}
}

func (c *cluster) run() {
	if c.loadGen == 0 {
		log.Fatalf("no load generator node specified for cluster: %s", c.name)
	}

	session, err := newSSHSession("cockroach", c.host(c.loadGen))
	if err != nil {
		panic(err)
	}
	defer session.Close()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		_, ok := <-ch
		if ok {
			c.stopLoad()
		}
	}()

	session.Stdout = os.Stdout
	session.Stderr = os.Stderr
	url := "postgres://root@localhost:27183/test"
	if c.secure {
		url += "?sslcert=%2Fhome%2Fcockroach%2Fcerts%2Fnode.crt&" +
			"sslkey=%2Fhome%2Fcockroach%2Fcerts%2Fnode.key&sslmode=verify-full&" +
			"sslrootcert=%2Fhome%2Fcockroach%2Fcerts%2Fca.crt"
	} else {
		url += "?sslmode=disable"
	}
	const cmd = "./kv --duration=1h --read-percent=95 --concurrency=10 --splits=10"
	fmt.Println(cmd)
	if err := session.Run(cmd + " '" + url + "'"); err != nil {
		if !isSigKill(err) {
			log.Fatal(err)
		}
	}

	signal.Stop(ch)
	close(ch)
}

const progressDone = "=======================================>"
const progressTodo = "----------------------------------------"

func formatProgress(p float64) string {
	i := int(math.Ceil(float64(len(progressDone)) * (1 - p)))
	return fmt.Sprintf("[%s%s] %.0f%%", progressDone[i:], progressTodo[:i], 100*p)
}

func (c *cluster) put(src, dest string) {
	fmt.Printf("%s: putting %s %s\n", c.name, src, dest)

	type result struct {
		index int
		err   error
	}

	var writer uiWriter
	results := make(chan result, c.total)
	lines := make([]string, c.total)
	var linesMu sync.Mutex

	var wg sync.WaitGroup
	for i := 1; i <= c.total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			session, err := newSSHSession("cockroach", c.host(i))
			if err == nil {
				defer session.Close()
				err = scp(src, dest, func(p float64) {
					linesMu.Lock()
					defer linesMu.Unlock()
					lines[i-1] = formatProgress(p)
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
					lines[r.index-1] = r.err.Error()
				} else {
					lines[r.index-1] = "done"
				}
				linesMu.Unlock()
			}
		}
		linesMu.Lock()
		for i := range lines {
			fmt.Fprintf(&writer, "  %2d: ", i+1)
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
	c.parallel(display, c.loadGen, c.loadGen, func(host string) ([]byte, error) {
		session, err := newSSHSession("cockroach", c.host(c.loadGen))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		const cmd = `sudo kill -9 $(lsof -t -i :27183) 2>/dev/null || true`
		return session.CombinedOutput(cmd)
	})
}

func (c *cluster) parallel(display string, from, to int, fn func(host string) ([]byte, error)) {
	type result struct {
		index int
		out   []byte
		err   error
	}

	results := make(chan result, 1+to-from)
	var wg sync.WaitGroup
	for i := from; i <= to; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := fn(c.host(i))
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
	complete := make([]bool, 1+to-from)
	haveErr := false

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
		case r, ok := <-results:
			done = !ok
			if ok {
				complete[r.index-from] = true
			}
		}
		fmt.Fprint(&writer, display)
		for i := range complete {
			if complete[i] {
				fmt.Fprintf(&writer, " %d", i+from)
			}
		}
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
