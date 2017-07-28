package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/crypto/ssh"
)

type cluster struct {
	name  string
	count int
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

	args := []string{
		"--insecure",
		"--store=path=/mnt/data1/cockroach",
		"--log-dir=/home/cockroach/logs",
		// "--logtostderr",
		"--background",
	}
	if join != host {
		args = append(args, "--join="+join)
	}
	cmd := env + " ./cockroach start " + strings.Join(args, " ") +
		"> logs/cockroach.stdout 2> logs/cockroach.stderr"
	return session.CombinedOutput(cmd)
}

func (c *cluster) start() {
	fmt.Printf("%s: starting", c.name)
	host1 := c.host(1)
	c.parallel(1, c.count, func(host string) ([]byte, error) {
		return c.startNode(host, host1)
	})
	fmt.Printf("\n")
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
	fmt.Printf("%s: stopping", c.name)
	c.parallel(1, c.count+1, c.stopNode)
	fmt.Printf("\n")
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
	c.stopLoad()
	fmt.Printf("%s: wiping", c.name)
	c.parallel(1, c.count+1, c.wipeNode)
	fmt.Printf("\n")
}

func (c *cluster) status() {
	results := make([]chan string, c.count+1)
	for i := 0; i <= c.count; i++ {
		results[i] = make(chan string, 1)
		go func(i int) {
			session, err := newSSHSession("cockroach", c.host(i+1))
			if err != nil {
				results[i] <- err.Error()
				return
			}
			defer session.Close()

			proc := "cockroach"
			if i >= c.count {
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
		fmt.Printf("%s %d: %s\n", c.name, i+1, s)
	}
}

func (c *cluster) run() {
	session, err := newSSHSession("cockroach", c.host(7))
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
	const url = "'postgres://root@localhost:27183/test?sslmode=disable'"
	const cmd = "./kv --duration=1m --read-percent=95 --concurrency=10 --splits=10"
	fmt.Println(cmd)
	if err := session.Run(cmd + " " + url); err != nil {
		if !isSigKill(err) {
			log.Fatal(err)
		}
	}

	signal.Stop(ch)
	close(ch)
}

func (c *cluster) stopLoad() {
	session, err := newSSHSession("cockroach", c.host(7))
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

func (c *cluster) parallel(from, to int, fn func(host string) ([]byte, error)) {
	type result struct {
		host  string
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
			host := c.host(i)
			out, err := fn(host)
			results <- result{host, i, out, err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	haveErr := false
	for r := range results {
		if r.err != nil {
			fmt.Printf("\n%s: %s\n", r.host, r.err)
			haveErr = true
		} else {
			fmt.Printf(" %d", r.index)
		}
	}
	if haveErr {
		panic("failed\n")
	}
}
