// TODO:
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
var clusterType = "cockroach"
var secure = false
var nodeEnv = "COCKROACH_ENABLE_RPC_COMPRESSION=false"
var nodeArgs []string
var binary = "./cockroach"

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

var clusters = map[string]*cluster{}

func newCluster(name string, reserveLoadGen bool) (*cluster, error) {
	if name == "" {
		return nil, fmt.Errorf("no cluster specified")
	}
	c, ok := clusters[name]
	if !ok {
		return nil, fmt.Errorf("unknown cluster: %s", name)
	}

	switch clusterType {
	case "cockroach":
		c.impl = cockroach{}
	case "cassandra":
		c.impl = cassandra{}
	default:
		return nil, fmt.Errorf("unknown cluster type: %s", clusterType)
	}

	nodes, err := listNodes(clusterNodes, len(c.vms))
	if err != nil {
		return nil, err
	}

	c.nodes = nodes
	if reserveLoadGen {
		// TODO(marc): make loadgen node configurable. For now, we always use the
		// last ID (1-indexed).
		c.loadGen = len(c.vms)
	} else {
		c.loadGen = -1
	}
	c.secure = secure
	c.env = nodeEnv
	c.args = nodeArgs

	return c, nil
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
		c, err := newCluster(clusterName, false /* reserveLoadGen */)
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
		c, err := newCluster(clusterName, false /* reserveLoadGen */)
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
		c, err := newCluster(clusterName, false /* reserveLoadGen */)
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
		c, err := newCluster(clusterName, false /* reserveLoadGen */)
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
	RunE: func(_ *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no command specified")
		}
		c, err := newCluster(clusterName, false /* reserveLoadGen */)
		if err != nil {
			return err
		}

		cmd := strings.TrimSpace(strings.Join(args, " "))
		title := cmd
		if len(title) > 30 {
			title = title[:27] + "..."
		}

		_ = c.run(os.Stdout, c.nodes, title, cmd)
		return nil
	},
}

var testCmd = &cobra.Command{
	Use:   "test <name> [<name>]",
	Short: "run one or more tests on a cluster",
	Long: `

Run one or more tests on a cluster. The test <name> must be one of:

	` + strings.Join(allTests(), "\n\t") + `

Alternately, an interrupted test can be resumed by specifying the output
directory of a previous test. For example:

	roachperf denim test kv_0.cockroach-6151ae1

will restart the kv_0 test on denim using the cockroach binary with the build
tag 6151ae1.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			fmt.Printf("no test specified\n\n")
			return cmd.Help()
		}
		for _, arg := range args {
			if err := runTest(arg, clusterName); err != nil {
				return err
			}
		}
		return nil
	},
}

var uploadCmd = &cobra.Command{
	Use:   "upload <testdir> <backend>",
	Short: "upload test data to a backend",
	Long: `
Upload the artifacts from a test. Currently supports s3 only as a backend.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return upload(args)
	},
}

var installCmd = &cobra.Command{
	Use:   "install <software>",
	Short: "install 3rd party software",
	Long: `
Install third party software. Currently available installation options
are:

  cassandra
  mongodb
  postgres
  tools (fio, iftop, perf)
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no software specified")
		}
		c, err := newCluster(clusterName, false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		return install(c, args)
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
		c, err := newCluster(clusterName, false /* reserveLoadGen */)
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
		c, err := newCluster(clusterName, false /* reserveLoadGen */)
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

	if err := loadClusters(); err != nil {
		// We don't want to exit as we may be looking at the help message.
		fmt.Printf("problem loading clusters: %s", err)
	}

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
			installCmd,
		)
		cmd.PersistentFlags().BoolVar(
			&secure, "secure", false, "use a secure cluster")
		cmd.PersistentFlags().StringSliceVarP(
			&nodeArgs, "args", "a", nil, "node arguments")
		cmd.PersistentFlags().StringVarP(
			&nodeEnv, "env", "e", nodeEnv, "node environment variables")
		cmd.PersistentFlags().StringVarP(
			&clusterType, "type", "t", clusterType, `cluster type ("cockroach" or "cassandra")`)
		rootCmd.AddCommand(cmd)
	}

	rootCmd.AddCommand(dumpCmd, webCmd, uploadCmd)

	rootCmd.PersistentFlags().BoolVar(
		&insecureIgnoreHostKey, "insecure-ignore-host-key", true, "don't check ssh host keys")
	startCmd.PersistentFlags().StringVarP(
		&binary, "binary", "b", "./cockroach", "the remote cockroach binary used to start a server")
	testCmd.PersistentFlags().StringVarP(
		&binary, "binary", "b", "./cockroach", "the remote cockroach binary used to start a server")
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
