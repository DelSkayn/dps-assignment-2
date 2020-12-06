package cmd

import (
	"ddps/chord"
	"errors"

	"github.com/spf13/cobra"
)

var (
	host              string
	bootstrap         string
	bitsInKey         uint32
	numVirtualNodes   uint32
	numSuccessors     uint32
	stabilizeInterval string
	deamon            bool
)

func init() {
	tryCmd.Flags().StringVar(&bootstrap, "bootstrap", "", "A address of another node in the network for bootstrapping the node")
	tryCmd.Flags().Uint32VarP(&bitsInKey, "bits", "b", 32, "Set the amount of bits in a keys")
	tryCmd.Flags().Uint32Var(&numVirtualNodes, "virtuals", 5, "Set the amount of virtual nodes to run on this node")
	tryCmd.Flags().Uint32Var(&numSuccessors, "successors", 3, "Set the amount of extra successors to store")
	tryCmd.Flags().StringVar(&stabilizeInterval, "interval", "1s", "Set the time interval between stablizing request")
	tryCmd.Flags().BoolVarP(&deamon, "deamon", "d", false, "start the node as a deamon")
	rootCmd.AddCommand(tryCmd)
}

var tryCmd = &cobra.Command{
	Use:   "start [host]",
	Short: "Start the cdfs deamon",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("missing hostname argument")
		}
		cfg := chord.ConfigBuilder()
		var err error
		cfg, err = cfg.Host(args[0])
		if err != nil {
			return err
		}
		if bootstrap != "" {
			cfg, err = cfg.BootstrapAddr(bootstrap)
			if err != nil {
				return err
			}
		}

		cfg, err = cfg.BitsInKey(bitsInKey).
			NumSuccessors(numSuccessors).
			NumSuccessors(numSuccessors).
			StabilizeInterval(stabilizeInterval)
		if err != nil {
			return err
		}

		chord, err := cfg.CreateChord()
		if err != nil {
			return err
		}
		chord.Run()
		return nil
	},
}
