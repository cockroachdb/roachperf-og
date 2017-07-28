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

	"golang.org/x/crypto/ssh"
)

type cluster struct {
	name    string
	count   int
	total   int
	loadGen int
	secure  bool
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

	const env = "GOGC=200 COCKROACH_ENABLE_RPC_COMPRESSION=false"

	var args []string
	if c.secure {
		args = append(args, "--certs-dir=certs")
	} else {
		args = append(args, "--insecure")
	}
	args = append(args, "--store=path=/mnt/data1/cockroach")
	args = append(args, "--log-dir=/home/cockroach/logs")
	// args = append(args, "--logtostderr")
	args = append(args, "--background")
	if join != host {
		args = append(args, "--join="+join)
	}
	cmd := env + " ./cockroach start " + strings.Join(args, " ") +
		"> logs/cockroach.stdout 2> logs/cockroach.stderr"
	return session.CombinedOutput(cmd)
}

func (c *cluster) start() {
	fmt.Printf("%s: starting\n", c.name)
	host1 := c.host(1)
	c.parallel(1, c.count, func(host string, _ func(float64)) ([]byte, error) {
		return c.startNode(host, host1)
	})
}

func (c *cluster) stopNode(host string) ([]byte, error) {
	session, err := newSSHSession("cockroach", host)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	const cmd = `sudo pkill -9 "cockroach|java|mongo|kv" || true`
	return session.CombinedOutput(cmd)
}

func (c *cluster) stop() {
	fmt.Printf("%s: stopping\n", c.name)
	c.parallel(1, c.total, func(host string, _ func(float64)) ([]byte, error) {
		return c.stopNode(host)
	})
}

func (c *cluster) wipeNode(host string) ([]byte, error) {
	session, err := newSSHSession("cockroach", host)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	const cmd = `
sudo pkill -9 "cockroach|java|mongo|kv" || true ;
sudo find /mnt/data* -maxdepth 1 -type f -exec rm -f {} \; ;
sudo rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,mongo-data} \; ;
sudo find /home/cockroach/logs -type f -not -name supervisor.log -exec rm -f {} \; ;
`
	return session.CombinedOutput(cmd)
}

func (c *cluster) wipe() {
	if c.loadGen != 0 {
		c.stopLoad()
	}
	fmt.Printf("%s: wiping\n", c.name)
	c.parallel(1, c.total, func(host string, _ func(float64)) ([]byte, error) {
		return c.wipeNode(host)
	})
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

			proc := "cockroach"
			if i+1 == c.loadGen {
				proc = "kv"
			}
			out, err := session.CombinedOutput("pidof " + proc)
			if err != nil {
				if exit, ok := err.(*ssh.ExitError); ok && exit.Signal() == "" {
					results[i] <- proc + " not running"
				} else {
					results[i] <- err.Error()
				}
			} else {
				results[i] <- proc + " running " + strings.TrimSpace(string(out))
			}
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
	const cmd = "./kv --duration=1m --read-percent=95 --concurrency=10 --splits=10"
	fmt.Println(cmd)
	if err := session.Run(cmd + " '" + url + "'"); err != nil {
		if !isSigKill(err) {
			log.Fatal(err)
		}
	}

	signal.Stop(ch)
	close(ch)
}

func (c *cluster) put(src, dest string) {
	fmt.Printf("%s: putting %s %s\n", c.name, src, dest)
	c.parallel(1, c.total, func(host string, progress func(float64)) ([]byte, error) {
		session, err := newSSHSession("cockroach", host)
		if err != nil {
			return nil, err
		}
		defer session.Close()
		return nil, scp(src, dest, progress, session)
	})
}

func (c *cluster) stopLoad() {
	if c.loadGen == 0 {
		log.Fatalf("no load generator node specified for cluster: %s", c.name)
	}

	session, err := newSSHSession("cockroach", c.host(c.loadGen))
	if err != nil {
		panic(err)
	}
	defer session.Close()

	fmt.Printf("%s: stopping load\n", c.name)
	const cmd = `sudo pkill -9 kv || true`
	if _, err := session.CombinedOutput(cmd); err != nil {
		panic(err)
	}
}

const progressDone = "=======================================>"
const progressTodo = "----------------------------------------"

func formatProgress(p float64) string {
	i := int(math.Ceil(float64(len(progressDone)) * (1 - p)))
	return fmt.Sprintf("[%s%s] %.0f%%", progressDone[i:], progressTodo[:i], 100*p)
}

func (c *cluster) parallel(from, to int, fn func(host string, progress func(float64)) ([]byte, error)) {
	type result struct {
		host  string
		index int
		out   []byte
		err   error
	}

	var writer uiWriter
	results := make(chan result, 1+to-from)
	lines := make([]string, 1+to-from)
	var linesMu sync.Mutex

	var wg sync.WaitGroup
	for i := from; i <= to; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			host := c.host(i)
			out, err := fn(host, func(p float64) {
				linesMu.Lock()
				defer linesMu.Unlock()
				lines[i-from] = formatProgress(p)
			})
			results <- result{host, i, out, err}
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
					lines[r.index-from] = r.err.Error()
				} else {
					lines[r.index-from] = "done"
				}
				linesMu.Unlock()
			}
		}
		linesMu.Lock()
		for i := range lines {
			fmt.Fprintf(&writer, "  %2d: ", i+from)
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
