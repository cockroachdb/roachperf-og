package main

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var secure = false
var env = "COCKROACH_ENABLE_RPC_COMPRESSION=false"

func listNodes(s string, total int) ([]int, error) {
	if s == "all" {
		r := make([]int, total)
		for i := range r {
			r[i] = i + 1
		}
		return r, nil
	}

	m := map[int]bool{}
	for _, p := range strings.Split(s, ",") {
		parts := strings.Split(p, "-")
		switch len(parts) {
		case 1:
			i, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}
			m[i] = true
		case 2:
			from, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}
			to, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, err
			}
			for i := from; i <= to; i++ {
				m[i] = true
			}
		default:
			return nil, fmt.Errorf("unable to parse nodes specification: %s", p)
		}
	}

	r := make([]int, 0, len(m))
	for i := range m {
		r = append(r, i)
	}
	sort.Ints(r)
	return r, nil
}

type clusterInfo struct {
	total      int
	loadGen    int
	hostFormat string
}

const defaultHostFormat = "cockroach-%s-%04d.crdb.io"

var clusters = map[string]clusterInfo{
	"denim": {7, 7, defaultHostFormat},
	"sky":   {128, 128, defaultHostFormat},
}

func isCluster(name string) bool {
	parts := strings.Split(name, ":")
	_, ok := clusters[parts[0]]
	return ok
}

func newCluster(name string) (*cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("no cluster specified")
	}
	parts := strings.Split(name, ":")
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid cluster name: %s", name)
	}
	name = parts[0]
	info, ok := clusters[name]
	if !ok {
		return nil, fmt.Errorf("unknown cluster: %s", name)
	}
	nodesSpec := "all"
	if len(parts) == 2 {
		nodesSpec = parts[1]
	}
	nodes, err := listNodes(nodesSpec, info.total)
	if err != nil {
		return nil, err
	}
	return &cluster{
		name:       name,
		nodes:      nodes,
		loadGen:    info.loadGen,
		secure:     secure,
		hostFormat: info.hostFormat,
		env:        env,
	}, nil
}

var rootCmd = &cobra.Command{
	Use:   "roachperf [command] (flags)",
	Short: "roachperf tool for manipulating test clusters",
	Long: `
roachperf is a tool for manipulating test clusters, allowing easy starting,
stopping and wiping of clusters along with running load generators.
`,
}

var startCmd = &cobra.Command{
	Use:   "start <cluster>",
	Short: "start a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no cluster specified")
		}
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.start()
		return nil
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop <cluster>",
	Short: "stop a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no cluster specified")
		}
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.stop()
		return nil
	},
}

var wipeCmd = &cobra.Command{
	Use:   "wipe <cluster>",
	Short: "wipe a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no cluster specified")
		}
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.wipe()
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status <cluster>",
	Short: "retrieve the status of a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no cluster specified")
		}
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.status()
		return nil
	},
}

var runCmd = &cobra.Command{
	Use:   "run <cluster> <command> [args]",
	Short: "run a command on every node in a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no cluster specified")
		}
		if len(args) == 1 {
			return fmt.Errorf("no command specified")
		}
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.run(args[1:])
		return nil
	},
}

var testCmd = &cobra.Command{
	Use:   "test <cluster> <name>",
	Short: "run a test on a cluster",
	Long: `
Run a test on a cluster, placing results in a timestamped directory. The test
<name> must be one of:

	` + strings.Join(allTests(), "\n\t") + `

Alternately, an interrupted test can be resumed by specifying the output
directory of a previous test. For example:

	roachperf test 2017-08-02T14_06_41.kv_0.cockroach-6151ae1

will restart the kv_0 test on denim using the cockroach binary with the build
tag 6151ae1. If the test, environment or cockroach build tag do not match,
restarting the test will fail.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			fmt.Printf("no test specified\n\n")
			return cmd.Help()
		}
		if isTest(args[0]) {
			return runTest(args[0], "")
		}
		if len(args) == 0 {
			return fmt.Errorf("no cluster specified")
		}
		clusterName := args[0]
		args = args[1:]
		if !isCluster(clusterName) {
			return fmt.Errorf("unknown cluster: %s", clusterName)
		}
		if len(args) != 1 {
			fmt.Printf("no test specified\n\n")
			return cmd.Help()
		}
		if !isTest(args[0]) {
			fmt.Printf("unknown test: %s\n\n", args[0])
			return cmd.Help()
		}
		return runTest(args[0], clusterName)
	},
}

var webCmd = &cobra.Command{
	Use:   "web <testdir> [<testdir>]",
	Short: "visualize and compare test output",
	Long: `
Visualize the output of a single test or compare the output of two tests.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return web(args)
	},
}

var dumpCmd = &cobra.Command{
	Use:   "dump <testdir> [<testdir>]",
	Short: "dump test output",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		return dump(args)
	},
}

var putCmd = &cobra.Command{
	Use:   "put <cluster> <src> [<dest>]",
	Short: "copy a local file to the nodes in a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("source file not specified")
		}
		if len(args) > 3 {
			return fmt.Errorf("too many arguments")
		}
		src := args[1]
		dest := path.Base(src)
		if len(args) == 3 {
			dest = args[2]
		}
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.put(src, dest)
		return nil
	},
}

func main() {
	rootCmd.AddCommand(
		dumpCmd,
		putCmd,
		runCmd,
		startCmd,
		statusCmd,
		stopCmd,
		testCmd,
		webCmd,
		wipeCmd,
	)

	rootCmd.PersistentFlags().BoolVar(
		&secure, "secure", false, "use a secure cluster")
	rootCmd.PersistentFlags().StringVarP(
		&env, "env", "e", env, "cockroach node environment variables")

	testCmd.PersistentFlags().DurationVarP(
		&duration, "duration", "d", 5*time.Minute, "The duration to run each test")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
