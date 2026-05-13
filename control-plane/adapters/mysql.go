package adapters

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tructxn/mirage/control-plane/session"
)

type MySQLAdapter struct {
	dsn string
	db  *sql.DB
}

func NewMySQLAdapter(dsn string) *MySQLAdapter {
	db, _ := sql.Open("mysql", dsn)
	db.SetConnMaxLifetime(time.Minute)
	db.SetMaxOpenConns(5)
	return &MySQLAdapter{dsn: dsn, db: db}
}

// PushRule inserts a mock response row into the mirage_mocks table.
// match.table identifies the target table; response is an array of row JSON objects.
func (a *MySQLAdapter) PushRule(sessionID string, rule *session.Rule) error {
	table, ok := rule.Match["table"].(string)
	if !ok || table == "" {
		return fmt.Errorf("mysql rule missing match.table")
	}
	_, err := a.db.Exec(
		`INSERT INTO mirage_mocks (session_id, rule_id, target_table, response_json) VALUES (?, ?, ?, ?)`,
		sessionID, rule.ID, table, fmt.Sprintf("%v", rule.Response),
	)
	return err
}

func (a *MySQLAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	_, err := a.db.Exec(`DELETE FROM mirage_mocks WHERE session_id=? AND rule_id=?`, sessionID, rule.ID)
	return err
}

func (a *MySQLAdapter) Reset(sessionID string, rules []session.Rule) error {
	_, err := a.db.Exec(`DELETE FROM mirage_mocks WHERE session_id=?`, sessionID)
	return err
}

func (a *MySQLAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil // Phase 4
}

func (a *MySQLAdapter) Healthy() bool {
	return a.db.Ping() == nil
}
