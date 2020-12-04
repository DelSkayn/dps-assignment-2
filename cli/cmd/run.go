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
		var err error
		if host != "" {
			cfg, err = cfg.Host(host)
			if err != nil {
				return err
			}
		}
		if bootstrap != "" {
			cfg, err = cfg.BootstrapAddr(bootstrap)
			if err != nil {
				return err
			}
		}

		chord, err := cfg.CreateChord()
		if err != nil {
			return err
		}
		chord.Run()
		return nil
	},
}
