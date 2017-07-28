package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

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
		c := &cluster{"denim", 6}
		c.start()
		return nil
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "stop a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := &cluster{"denim", 6}
		c.stop()
		return nil
	},
}

var wipeCmd = &cobra.Command{
	Use:   "wipe",
	Short: "wipe a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := &cluster{"denim", 6}
		c.wipe()
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "retrieve the status of a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := &cluster{"denim", 6}
		c.status()
		return nil
	},
}

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "run a test on a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := &cluster{"denim", 6}
		c.wipe()
		c.start()
		c.run()
		c.stop()
		return nil
	},
}

func main() {
	// TODO(peter):
	// - optional secure mode
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
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
