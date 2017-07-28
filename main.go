package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var clusterName string
var clusterNodes = 6
var secure = false

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

func newCluster() *cluster {
	info := clusterSizes[clusterName]
	return &cluster{
		name:    clusterName,
		count:   clusterNodes,
		total:   info.total,
		loadGen: info.loadGen,
		secure:  secure,
	}
}

var rootCmd = &cobra.Command{
	Use:   "roachperf [command] (flags)",
	Short: "roachperf tool for manipulating test clusters",
	Long: `
roachperf is a tool for manipulating test clusters, allowing easy starting,
stopping and wiping of clusters along with running load generators.
`,
	SilenceUsage: true,
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newCluster()
		c.start()
		return nil
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "stop a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newCluster()
		c.stop()
		return nil
	},
}

var wipeCmd = &cobra.Command{
	Use:   "wipe",
	Short: "wipe a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newCluster()
		c.wipe()
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "retrieve the status of a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newCluster()
		c.status()
		return nil
	},
}

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "run a test on a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newCluster()
		c.wipe()
		c.start()
		c.run()
		c.stop()
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
	)

	clusterName = os.Getenv("CLUSTER")
	if clusterName == "" {
		clusterName = "denim"
	}
	rootCmd.PersistentFlags().StringVarP(
		&clusterName, "cluster", "c", clusterName, "cluster name")
	rootCmd.PersistentFlags().IntVarP(
		&clusterNodes, "nodes", "n", clusterNodes, "number of nodes in cluster")
	rootCmd.PersistentFlags().BoolVar(
		&secure, "secure", false, "use a secure cluster")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
