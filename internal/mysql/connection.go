package mysql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"go-sync/internal/config"
)

type endpoint struct {
	host, address, user, password, database string
	port                                    uint16
}

func parseDSN(raw string) (endpoint, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "mysql" || u.User == nil || u.Hostname() == "" || u.RawQuery != "" || u.Fragment != "" {
		return endpoint{}, errors.New("mysql dsn must be mysql://user:password@host:port/database")
	}
	password, _ := u.User.Password()
	port := uint64(3306)
	if u.Port() != "" {
		port, err = strconv.ParseUint(u.Port(), 10, 16)
		if err != nil || port == 0 {
			return endpoint{}, errors.New("mysql dsn has invalid port")
		}
	}
	database, err := url.PathUnescape(strings.TrimPrefix(u.EscapedPath(), "/"))
	if err != nil || database == "" || strings.Contains(database, "/") {
		return endpoint{}, errors.New("mysql dsn must contain one database name")
	}
	host := u.Hostname()
	return endpoint{host: host, address: net.JoinHostPort(host, strconv.FormatUint(port, 10)),
		port: uint16(port), user: u.User.Username(), password: password, database: database}, nil
}

func connect(ctx context.Context, cfg config.Config) (*client.Conn, endpoint, error) {
	ep, err := parseDSN(cfg.DSN)
	if err != nil {
		return nil, ep, err
	}
	timeout, _ := time.ParseDuration(cfg.MySQL.ConnectTimeout)
	conn, err := client.ConnectWithContext(ctx, ep.address, ep.user, ep.password, ep.database, timeout)
	if err != nil {
		return nil, ep, fmt.Errorf("mysql connect: %w", err)
	}
	return conn, ep, nil
}

func quote(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
