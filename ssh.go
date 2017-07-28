package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

var knownHosts ssh.HostKeyCallback
var knownHostsOnce sync.Once

func getKnownHosts() ssh.HostKeyCallback {
	knownHostsOnce.Do(func() {
		var err error
		knownHosts, err = knownhosts.New(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
		if err != nil {
			log.Fatal(err)
		}
	})
	return knownHosts
}

func newSSHClient(user, host string) (*ssh.Client, net.Conn, error) {
	const authSockEnv = "SSH_AUTH_SOCK"
	agentSocket := os.Getenv(authSockEnv)
	if agentSocket == "" {
		return nil, nil, fmt.Errorf("%s empty", authSockEnv)
	}
	sock, err := net.Dial("unix", agentSocket)
	if err != nil {
		return nil, nil, err
	}
	agent := agent.NewClient(sock)
	signers, err := agent.Signers()
	if err != nil {
		return nil, nil, err
	}

	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signers...)},
		HostKeyCallback: getKnownHosts(),
	}
	config.SetDefaults()

	addr := fmt.Sprintf("%s:22", host)
	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return nil, nil, err
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, addr, config)
	if err != nil {
		return nil, nil, err
	}
	return ssh.NewClient(c, chans, reqs), conn, nil
}

type sshClient struct {
	sync.Mutex
	*ssh.Client
}

var clients = make(map[string]*sshClient)
var clientsMu sync.Mutex

func newSSHSession(user, host string) (*ssh.Session, error) {
	clientsMu.Lock()
	target := fmt.Sprintf("%s@%s", user, host)
	client := clients[target]
	if client == nil {
		client = &sshClient{}
		clients[target] = client
	}
	clientsMu.Unlock()

	client.Lock()
	defer client.Unlock()
	if client.Client == nil {
		var err error
		client.Client, _, err = newSSHClient(user, host)
		if err != nil {
			return nil, err
		}
	}
	return client.NewSession()
}

func isSigKill(err error) bool {
	switch t := err.(type) {
	case *ssh.ExitError:
		return t.Signal() == string(ssh.SIGKILL)
	}
	return false
}

type progressWriter struct {
	writer   io.Writer
	done     int64
	total    int64
	progress func(float64)
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.writer.Write(b)
	if err == nil {
		p.done += int64(n)
		p.progress(float64(p.done) / float64(p.total))
	}
	return n, err
}

func scp(src, dest string, progress func(float64), session *ssh.Session) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	s, err := f.Stat()
	if err != nil {
		return err
	}
	go func() {
		w, _ := session.StdinPipe()
		defer w.Close()
		fmt.Fprintf(w, "C%#o %d %s\n", s.Mode().Perm(), s.Size(), path.Base(src))
		p := &progressWriter{w, 0, s.Size(), progress}
		io.Copy(p, f)
		fmt.Fprint(w, "\x00")
	}()
	cmd := fmt.Sprintf("rm -f %s ; scp -t %s", dest, dest)
	if err := session.Run(cmd); err != nil {
		return err
	}
	return nil
}
