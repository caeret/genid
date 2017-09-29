package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
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
	PIDFile     string `toml:"pidfile"`
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

var logger = logging.NewSimpleLogger()

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

func handleSignals(s *beam.Server, config Config) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			logger.Info("receive signal: %s.", sig)
			err := s.Close()
			if len(config.PIDFile) > 0 {
				logger.Info("remove PIDFILE: %s.", config.PIDFile)
				err := os.Remove(config.PIDFile)
				if err != nil {
					logger.Warning("fail to remove PIDFILE: %s.", err.Error())
				}
			}
			if err != nil {
				logger.Warning("fail to stop server: %s.", err.Error())
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

	logger.Info("load configuration %v", config)

	if len(config.PIDFile) > 0 {
		pid, err := setUpPIDFile(config.PIDFile)
		if err != nil {
			return cli.NewExitError(err.Error(), 10)
		}
		logger.Info("create pidfile \"%s\" with PID \"%d\".", config.PIDFile, pid)
	}

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

	handleSignals(server, config)

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

func setUpPIDFile(PIDFile string) (PID int, err error) {
	var file *os.File
	_, err = os.Stat(PIDFile)
	if err == nil {
		// file exist
		var oldPIDData []byte
		oldPIDData, err = ioutil.ReadFile(PIDFile)
		if err != nil {
			return
		}
		var oldPID int64
		oldPID, err = strconv.ParseInt(string(oldPIDData), 10, 64)
		if err != nil {
			return
		}

		var p *os.Process
		p, err = os.FindProcess(int(oldPID))
		if err != nil {
			return
		}
		err = p.Signal(syscall.Signal(0))
		if err == nil {
			err = fmt.Errorf("progress with PID %d is running", oldPID)
			return
		}
	}
	file, err = os.Create(PIDFile)
	if err != nil {
		return
	}
	defer file.Close()
	PID = os.Getpid()
	var PIDData []byte
	PIDData = strconv.AppendInt(PIDData, int64(PID), 10)
	_, err = file.Write(PIDData)
	return
}
