package cmd

import (
	"github.com/Conflux-Chain/go-conflux-util/config"
	"github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "confura-data-cache",
	Short: "Start confura data cache service",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func init() {
	cobra.OnInitialize(func() {
		config.MustInit("CDC")
	})

	log.BindFlags(rootCmd)
}

// Execute is the command line entrypoint.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logrus.WithError(err).Fatal("Failed to execute command")
	}
}

func fatalOnErr(err error, msg string, fields ...logrus.Fields) {
	if err == nil {
		return
	}

	if len(fields) > 0 {
		logrus.WithError(err).WithFields(fields[0]).Fatal(msg)
	} else {
		logrus.WithError(err).Fatal(msg)
	}
}
