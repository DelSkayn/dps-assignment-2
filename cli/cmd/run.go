package cmd

import (
	"ddps/chord"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(tryCmd)
}

var tryCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the cdfs deamon",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := chord.Run(chord.ConfigBuilder()); err != nil {
			return err
		}
		return nil
	},
}
