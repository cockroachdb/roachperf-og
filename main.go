// TODO:
//
// * Copy the binary into the test output directory. Automatically re-upload if
//   the version doesn't match.
//
// * Ease the creation of test metadata and then running a series of tests
//   using `roachperf <cluster> test <dir1> <dir2> ...`. Perhaps something like
//   `cockroach prepare <test> <binary>`.
//
// * Automatically detect stalled tests and restart tests upon unexpected
//   failures. Detection of stalled tests could be done by noticing zero output
//   for a period of time.
//
// * Detect crashed cockroach nodes.
//
// * Configure and run haproxy. (Assume it is already installed). This can be
//   done by running "cockroach gen haproxy" after the cluster is started.

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

var clusterName string
var clusterNodes = "all"
var secure = false
var env = "COCKROACH_ENABLE_RPC_COMPRESSION=false"
var cockroachArgs []string

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
	"sky":   {128, -1, defaultHostFormat},
}

func newCluster(name string) (*cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("no cluster specified")
	}
	info, ok := clusters[name]
	if !ok {
		return nil, fmt.Errorf("unknown cluster: %s", name)
	}
	nodes, err := listNodes(clusterNodes, info.total)
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
		args:       cockroachArgs,
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
	Use:   "start",
	Short: "start a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(clusterName)
		if err != nil {
			return err
		}
		c.start()
		return nil
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "stop a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(clusterName)
		if err != nil {
			return err
		}
		c.stop()
		return nil
	},
}

var wipeCmd = &cobra.Command{
	Use:   "wipe",
	Short: "wipe a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(clusterName)
		if err != nil {
			return err
		}
		c.wipe()
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "retrieve the status of a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(clusterName)
		if err != nil {
			return err
		}
		c.status()
		return nil
	},
}

var runCmd = &cobra.Command{
	Use:   "run <command> [args]",
	Short: "run a command on the nodes in a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no command specified")
		}
		c, err := newCluster(clusterName)
		if err != nil {
			return err
		}
		_ = c.run(os.Stdout, c.nodes, args)
		return nil
	},
}

var testCmd = &cobra.Command{
	Use:   "test <name>",
	Short: "run a test on a cluster",
	Long: `
Run a test on a cluster, placing results in a timestamped directory. The test
<name> must be one of:

	` + strings.Join(allTests(), "\n\t") + `

Alternately, an interrupted test can be resumed by specifying the output
directory of a previous test. For example:

	roachperf denim test kv_0.cockroach-6151ae1

will restart the kv_0 test on denim using the cockroach binary with the build
tag 6151ae1. If the test, environment or cockroach build tag do not match,
restarting the test will fail.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			fmt.Printf("no test specified\n\n")
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
	Use:   "put <src> [<dest>]",
	Short: "copy a local file to the nodes in a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("source file not specified")
		}
		if len(args) > 2 {
			return fmt.Errorf("too many arguments")
		}
		src := args[0]
		dest := path.Base(src)
		if len(args) == 2 {
			dest = args[1]
		}
		c, err := newCluster(clusterName)
		if err != nil {
			return err
		}
		c.put(src, dest)
		return nil
	},
}

var getCmd = &cobra.Command{
	Use:   "get <src> [<dest>]",
	Short: "copy a remote file from the nodes in a cluster",
	Long: `
Copy a remote file from the nodes in a cluster. If the file is retrieved from
multiple nodes the destination file name will be prefixed with the node number.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("source file not specified")
		}
		if len(args) > 2 {
			return fmt.Errorf("too many arguments")
		}
		src := args[0]
		dest := path.Base(src)
		if len(args) == 2 {
			dest = args[1]
		}
		c, err := newCluster(clusterName)
		if err != nil {
			return err
		}
		c.get(src, dest)
		return nil
	},
}

func sortedClusters() []string {
	var r []string
	for n := range clusters {
		r = append(r, n)
	}
	sort.Strings(r)
	return r
}

func main() {
	cobra.EnableCommandSorting = false

	for i, n := range sortedClusters() {
		var sep string
		if i+1 == len(clusters) {
			sep = "\n"
		}
		cmd := &cobra.Command{
			Use:   fmt.Sprintf("%s <command>", n),
			Short: fmt.Sprintf("perform an operation on %s%s", n, sep),
			Long: fmt.Sprintf(`

Perform an operation on %s. By default the operation is performed on all
nodes. A subset of nodes can be specified by appending :<nodes> to the cluster
name. The syntax of <nodes> is a comma separated list of specific node ids or
range of ids. For example:

  roachperf %[1]s:1-3,8-9 <command>

will perform <command> on:

  %[1]s-1
  %[1]s-2
  %[1]s-3
  %[1]s-8
  %[1]s-9
`, n),
		}
		cmd.AddCommand(
			getCmd,
			putCmd,
			runCmd,
			startCmd,
			statusCmd,
			stopCmd,
			testCmd,
			wipeCmd,
		)
		cmd.PersistentFlags().BoolVar(
			&secure, "secure", false, "use a secure cluster")
		cmd.PersistentFlags().StringSliceVarP(
			&cockroachArgs, "args", "a", nil, "cockroach node arguments")
		cmd.PersistentFlags().StringVarP(
			&env, "env", "e", env, "cockroach node environment variables")
		rootCmd.AddCommand(cmd)
	}

	rootCmd.AddCommand(dumpCmd, webCmd)

	testCmd.PersistentFlags().DurationVarP(
		&duration, "duration", "d", 5*time.Minute, "the duration to run each test")
	testCmd.PersistentFlags().StringVarP(
		&concurrency, "concurrency", "c", "1-64", "the concurrency to run each test")

	args := os.Args[1:]
	if len(args) > 0 {
		parts := strings.Split(args[0], ":")
		if len(parts) > 2 {
			fmt.Printf("invalid cluster name: %s\n\n", args[0])
			rootCmd.Help()
			os.Exit(1)
		}
		clusterName = parts[0]
		if _, ok := clusters[clusterName]; ok {
			if len(parts) == 2 {
				clusterNodes = parts[1]
			}
			args[0] = clusterName
		}
	}

	rootCmd.SetArgs(args)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
