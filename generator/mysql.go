package generator

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/gaemma/logging"
	_ "github.com/go-sql-driver/mysql"
)

const (
	selectSQL      = "SELECT `value` FROM %s WHERE `key` = ? FOR UPDATE"
	insertSQL      = "INSERT INTO %s (`key`, `value`, `last_mod_at`) values (?, ?, ?)"
	updateSQL      = "UPDATE %s SET `value` = ?, `last_mod_at` = ? WHERE `key` = ?"
	createTableSQL = "CREATE TABLE %s (\n" +
		"	`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
		"	`key` VARCHAR(32) NOT NULL,\n" +
		"	`value` INT UNSIGNED NOT NULL,\n" +
		"	`last_mod_at` INT UNSIGNED NOT NULL,\n" +
		"	PRIMARY KEY (`id`),\n" +
		"	UNIQUE KEY `key` (`key`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
)

// MysqlConfig contains the parameters needed by the generator.
type MysqlConfig struct {
	Dsn       string
	TableName string
}

// InitMysqlGenerator initializes the table needed.
func InitMysqlGenerator(config MysqlConfig) (err error) {
	db, err := sql.Open("mysql", config.Dsn)
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(createTableSQL, config.TableName))
	return
}

// NewMysqlGenerator creates a mysql id generator.
func NewMysqlGenerator(config MysqlConfig, step int64, logger logging.Logger) (generator Generator, err error) {
	db, err := sql.Open("mysql", config.Dsn)
	if err != nil {
		return
	}
	err = db.Ping()
	if err != nil {
		return
	}

	g := new(mysqlGenerator)
	g.sourceMap = make(map[string]*mysqlRowBasedEngine)
	g.db = db
	g.config = config
	g.skip = step
	if logger == nil {
		logger = logging.NewNopLogger()
	}
	g.logger = logger
	generator = g
	return
}

type mysqlGenerator struct {
	sync.RWMutex
	sourceMap map[string]*mysqlRowBasedEngine
	db        *sql.DB
	config    MysqlConfig
	skip      int64
	logger    logging.Logger
}

func (m *mysqlGenerator) EnableKeys(keys []string) (err error) {
	data := make(map[string]*mysqlRowBasedEngine, len(keys))
	for _, key := range keys {
		data[key], err = newMysqlRowBasedEngine(m, key, m.skip, m.logger)
		if err != nil {
			return
		}
	}
	m.Lock()
	defer m.Unlock()

	m.sourceMap = data
	return
}

func (m *mysqlGenerator) Next(key string) (id int64, err error) {
	engine, err := m.rowBasedEngine(key)
	if err != nil {
		return
	}
	return engine.next()
}

func (m *mysqlGenerator) Current(key string) (id int64, err error) {
	engine, err := m.rowBasedEngine(key)
	if err != nil {
		return
	}
	return engine.current()
}

func (m *mysqlGenerator) Close() error {
	return m.db.Close()
}

func (m *mysqlGenerator) rowBasedEngine(key string) (engine *mysqlRowBasedEngine, err error) {
	m.RLock()
	defer m.RUnlock()
	engine, exist := m.sourceMap[key]
	if !exist {
		err = ErrKeyDoesNotExist
	}
	return
}

func newMysqlRowBasedEngine(generator *mysqlGenerator, key string, skip int64, logger logging.Logger) (engine *mysqlRowBasedEngine, err error) {
	if skip <= 0 {
		err = fmt.Errorf("invalid skip: %d", skip)
		return
	}

	mysqlEngine := new(mysqlRowBasedEngine)
	mysqlEngine.generator = generator
	mysqlEngine.selectSQL = fmt.Sprintf(selectSQL, generator.config.TableName)
	mysqlEngine.insertSQL = fmt.Sprintf(insertSQL, generator.config.TableName)
	mysqlEngine.updateSQL = fmt.Sprintf(updateSQL, generator.config.TableName)
	mysqlEngine.skip = skip
	mysqlEngine.key = key
	if logger == nil {
		logger = logging.NewNopLogger()
	}
	mysqlEngine.logger = logger
	logger.Info("initialize counter for key \"%s\".", key)
	mysqlEngine.cur, mysqlEngine.max, err = mysqlEngine.increase(skip)
	if err != nil {
		return
	}

	return mysqlEngine, err
}

type mysqlRowBasedEngine struct {
	generator *mysqlGenerator
	selectSQL string
	updateSQL string
	insertSQL string
	key       string
	skip      int64
	max       int64
	cur       int64
	mutex     sync.Mutex
	logger    logging.Logger
}

func (m *mysqlRowBasedEngine) next() (id int64, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.cur == m.max {
		m.logger.Info("increase counter for key: \"%s\"", m.key)
		m.cur, m.max, err = m.increase(m.skip)
		if err != nil {
			return
		}
	}
	m.cur++
	return m.cur, nil
}

func (m *mysqlRowBasedEngine) current() (int64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.cur, nil
}

func (m *mysqlRowBasedEngine) increase(delta int64) (cur, max int64, err error) {
	defer func() {
		if err == nil {
			m.logger.Info("counter for key \"%s\" is increased from %d to %d.", m.key, cur, max)
		}
	}()
	tx, err := m.generator.db.Begin()
	defer func() {
		if err != nil {
			newErr := tx.Rollback()
			if newErr != nil {
				err = newErr
			}
		} else {
			err = tx.Commit()
		}
	}()
	if err != nil {
		return
	}
	err = m.generator.db.QueryRow(m.selectSQL, m.key).Scan(&cur)
	if err != nil {
		if err == sql.ErrNoRows {
			var res sql.Result
			max += m.skip
			res, err = m.generator.db.Exec(m.insertSQL, m.key, max, time.Now().Unix())
			var cnt int64
			cnt, err = res.RowsAffected()
			if err != nil {
				return
			}

			if cnt != 1 {
				err = fmt.Errorf("invalid effected row count: %d", cnt)
			}
		}
		return
	}

	max = cur + delta
	res, err := m.generator.db.Exec(m.updateSQL, max, time.Now().Unix(), m.key)
	if err != nil {
		return
	}

	cnt, err := res.RowsAffected()
	if err != nil {
		return
	}

	if cnt != 1 {
		err = fmt.Errorf("invalid effected row count: %d", cnt)
	}

	return
}
