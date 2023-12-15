package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dexidp/dex/connector"
	"github.com/dexidp/dex/pkg/log"
	"time"
)

// Config holds configuration options for MySQL/MariaDB logins.
type Config struct {
	Server         string `json:"server"`
	Port           int    `json:"port"`
	UsernamePrompt string `json:"usernamePrompt"`
}

type mysqlConnector struct {
	Server         string `json:"server"`
	Port           int    `json:"port"`
	UsernamePrompt string `json:"usernamePrompt"`
	logger         log.Logger
}

func (m mysqlConnector) Open(_ string, logger log.Logger) (connector.Connector, error) {
	return &mysqlConnector{
		Server:         m.Server,
		Port:           m.Port,
		UsernamePrompt: m.UsernamePrompt,
		logger:         logger,
	}, nil
}

// OpenConnector is the same as Open but returns a type with all implemented connector interfaces.
func (c *Config) OpenConnector(logger log.Logger) (interface {
	connector.Connector
	connector.PasswordConnector
}, error,
) {
	return c.openConnector(logger)
}

func (c *Config) openConnector(logger log.Logger) (*mysqlConnector, error) {
	requiredFields := []struct {
		name string
		val  string
	}{
		{"server", c.Server},
		{"port", fmt.Sprintf("%d", c.Port)},
	}

	for _, field := range requiredFields {
		if field.val == "" {
			return nil, fmt.Errorf("mysql: missing required field %q", field.name)
		}
	}

	return &mysqlConnector{
		Server:         c.Server,
		Port:           c.Port,
		UsernamePrompt: c.UsernamePrompt,
		logger:         logger,
	}, nil
}

func (m mysqlConnector) Prompt() string {
	return m.UsernamePrompt
}

func (m mysqlConnector) Login(ctx context.Context, s connector.Scopes, username, password string) (identity connector.Identity, validPassword bool, err error) {
	if password == "" {
		return connector.Identity{}, false, nil
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)", username, password, m.Server, m.Port))
	if err != nil {
		return connector.Identity{}, false, err
	}
	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	rows, err := db.Query("SELECT 1 * 5")
	if err != nil {
		return connector.Identity{}, false, err
	}
	if rows.Err() != nil {
		return connector.Identity{}, false, err
	}
	var res int
	err = rows.Scan(&res)
	if err != nil {
		return connector.Identity{}, false, err
	}

	return connector.Identity{
		UserID:            username,
		Username:          username,
		PreferredUsername: username,
		Email:             "",
		EmailVerified:     false,
		ConnectorData:     nil,
	}, true, nil
}
