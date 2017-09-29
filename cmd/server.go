package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gaemma/beam"
	"github.com/gaemma/genid/beamhandler"
	"github.com/gaemma/genid/generator"
	"github.com/gaemma/logging"
	"gopkg.in/urfave/cli.v1"
)

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type Config struct {
	Listen      string
	Engine      string
	Step        int64
	Keys        []string
	RWTimeout   duration `toml:"rw_timeout"`
	IdleTimeout duration `toml:"idle_timeout"`

	Mysql struct {
		DSN       string
		TableName string
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "genid"
	app.Usage = "another id generator."
	app.HideVersion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Usage: "load config from `FILE`",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "init",
			Usage:  "Init the generator",
			Action: commandInit,
		},
		{
			Name:   "run",
			Usage:  "Run the server",
			Action: commandRun,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(10)
	}
}

func handleSignals(s *beam.Server) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			err := s.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "fail to stop server: %s.", err.Error())
			}
		}
	}()
}

func parseConfig(c *cli.Context) (config Config, err error) {
	path := c.GlobalString("c")
	if len(path) == 0 {
		err = cli.NewExitError("config path should be specified.", 10)
		return
	}
	_, err = toml.DecodeFile(path, &config)
	return config, err
}

func commandRun(c *cli.Context) error {
	config, err := parseConfig(c)
	if err != nil {
		return err
	}

	logger := logging.NewSimpleLogger()

	logger.Info("load configuration %v", config)

	if config.Engine != "mysql" {
		return cli.NewExitError("only mysql engine supported.", 10)
	}

	mysqlConfig := generator.MysqlConfig{
		Dsn:       config.Mysql.DSN,
		TableName: config.Mysql.TableName,
	}
	gen, err := generator.NewMysqlGenerator(mysqlConfig, config.Step, logger)
	if err != nil {
		return cli.NewExitError(err.Error(), 10)
	}
	defer gen.Close()
	gen.EnableKeys(config.Keys)

	serverConfig := beam.Config{
		Logger: logging.NewSimpleLogger(),
		Addr:   config.Listen,
	}
	server := beam.NewServer(beamhandler.NewHandler(gen), serverConfig)

	handleSignals(server)

	err = server.Serve()
	if err != nil {
		if err == beam.ErrServerClosed {
			logger.Info(err.Error())
		} else {
			return cli.NewExitError(err.Error(), 10)
		}
	}

	return nil
}

func commandInit(c *cli.Context) error {
	config, err := parseConfig(c)
	if err != nil {
		return err
	}

	logger := logging.NewSimpleLogger()

	logger.Info("load configuration %v", config)

	if config.Engine != "mysql" {
		return cli.NewExitError("only mysql engine supported.", 10)
	}

	mysqlConfig := generator.MysqlConfig{
		Dsn:       config.Mysql.DSN,
		TableName: config.Mysql.TableName,
	}

	err = generator.InitMysqlGenerator(mysqlConfig)
	if err != nil {
		return cli.NewExitError(err.Error(), 10)
	}
	return nil
}
