package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "interactor",
	Version: "1.0.0",
	Short:   "clickhouse interactor",
	Long:    `this is clickhouse interactor developed in golang `,

	// Uncomment the following line if your bare application
	// has an action associated with it:

}

var cmdCreate = &cobra.Command{
	Use:     "ct",
	Version: "1.0.0",
	Short:   "create table",
	Long:    `create table and load data to clickhouse`,

	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("creating table")
	},
}

func Execute() {
	cmdCreate.AddCommand(cmdFeedsItemFilters)
	cmdCreate.AddCommand(cmdFeedsItem)
	rootCmd.AddCommand(cmdCreate)
	cobra.CheckErr(rootCmd.Execute())
}
