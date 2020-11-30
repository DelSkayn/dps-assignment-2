package cmd

import (
	"ddps/chord"

	"github.com/spf13/cobra"
)

var (
	host      string
	bootstrap string
)

func init() {
	rootCmd.AddCommand(tryCmd)
	rootCmd.PersistentFlags().StringVar(&host, "host", "", "the hostname of the node")
	rootCmd.PersistentFlags().StringVar(&bootstrap, "bootstrap", "", "A address of another node in the network for bootstrapping the node")
}

var tryCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the cdfs deamon",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := chord.ConfigBuilder()
		if host != "" {
			cfg = cfg.Host(host)
		}
		if bootstrap != "" {
			cfg = cfg.BootstrapAddr(bootstrap)
		}

		if _, err := chord.Run(cfg); err != nil {
			return err
		}
		return nil
	},
}
