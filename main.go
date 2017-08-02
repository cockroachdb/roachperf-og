package main

import (
	"fmt"
	"os"
	"path"

	"github.com/spf13/cobra"
)

var clusterNodes = 6
var secure = false
var env = "COCKROACH_ENABLE_RPC_COMPRESSION=false"

type clusterInfo struct {
	total   int
	loadGen int
}

var clusterSizes = map[string]clusterInfo{
	"adriatic": {6, 0},
	"blue":     {10, 0},
	"catrina":  {3, 0},
	"cerulean": {4, 0},
	"cobalt":   {6, 0},
	"cyan":     {6, 0},
	"denim":    {7, 7},
	"indigo":   {9, 0},
	"lapis":    {4, 0},
	"navy":     {6, 0},
	"omega":    {6, 0},
}

func newCluster(args []string) (*cluster, error) {
	name := os.Getenv("CLUSTER")
	if len(args) >= 1 {
		name = args[0]
	}
	if name == "" {
		return nil, fmt.Errorf("no cluster specified")
	}
	info, ok := clusterSizes[name]
	if !ok {
		return nil, fmt.Errorf("unknown cluster: %s", name)
	}
	return &cluster{
		name:    name,
		count:   clusterNodes,
		total:   info.total,
		loadGen: info.loadGen,
		secure:  secure,
		env:     env,
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
		c, err := newCluster(args)
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
		c, err := newCluster(args)
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
		c, err := newCluster(args)
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
		c, err := newCluster(args)
		if err != nil {
			return err
		}
		c.status()
		return nil
	},
}

var testCmd = &cobra.Command{
	Use:   "test <cluster>",
	Short: "run a test on a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args)
		if err != nil {
			return err
		}
		c.wipe()
		c.start()
		c.run()
		c.stop()
		return nil
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
		c, err := newCluster(args)
		if err != nil {
			return err
		}
		c.put(src, dest)
		return nil
	},
}

func main() {
	// TODO(peter):
	//
	// Test
	// - cluster config + load generator
	// - output per directory
	// - "cockroach version"
	// - environment variables
	// - cluster settings
	// - parameterized on cluster config and load
	// - wipe cluster
	// - start cluster
	// - start load
	// - gather results
	// - wipe cluster
	//
	// Analyzer
	// - compare output from two tests
	//   - ops/sec
	//   - avg/50%/95%/99% latency
	//
	// Initial tests
	// - Read scalability
	// - Write scalability

	rootCmd.AddCommand(
		startCmd,
		stopCmd,
		wipeCmd,
		statusCmd,
		testCmd,
		putCmd,
	)

	rootCmd.PersistentFlags().IntVarP(
		&clusterNodes, "nodes", "n", clusterNodes, "number of nodes in cluster")
	rootCmd.PersistentFlags().BoolVar(
		&secure, "secure", false, "use a secure cluster")
	rootCmd.PersistentFlags().StringVarP(
		&env, "env", "e", env, "cockroach node environment variables")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
