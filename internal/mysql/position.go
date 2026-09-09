package mysql

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

func encodePosition(p gomysql.Position) string {
	return fmt.Sprintf("mysql:v1:%s:%020d", base64.RawURLEncoding.EncodeToString([]byte(p.Name)), p.Pos)
}

func decodePosition(raw string) (gomysql.Position, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 4 || parts[0] != "mysql" || parts[1] != "v1" {
		return gomysql.Position{}, errors.New("invalid mysql checkpoint")
	}
	name, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(name) == 0 {
		return gomysql.Position{}, errors.New("invalid mysql checkpoint file")
	}
	pos, err := strconv.ParseUint(parts[3], 10, 32)
	if err != nil || pos < 4 {
		return gomysql.Position{}, errors.New("invalid mysql checkpoint position")
	}
	return gomysql.Position{Name: string(name), Pos: uint32(pos)}, nil
}
