package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	"ltv-monthly/pkg/models"

	_ "github.com/go-sql-driver/mysql"
)

const orderEventTypeID = 6 // "Commande"

/*
Open() : DSN en variable d'env possible, typé MySQL, loc=UTC
*/
func Open(dsn string) (*sql.DB, string, error) {
	mysqlDSN, err := toMySQLDSN(dsn)
	if err != nil {
		return nil, "", err
	}
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, "", err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db, mysqlDSN, nil
}

func toMySQLDSN(dsn string) (string, error) {
	if strings.HasPrefix(dsn, "mariadb://") || strings.HasPrefix(dsn, "mysql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse dsn: %w", err)
		}
		user := ""
		pass := ""
		if u.User != nil {
			user = u.User.Username()
			pw, _ := u.User.Password()
			pass = pw
		}
		host := u.Host
		db := strings.TrimPrefix(u.Path, "/")
		if user == "" || host == "" || db == "" {
			return "", fmt.Errorf("dsn incomplet (user/host/db)")
		}
		// loc=UTC → cohérence avec Observation UTC
		return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=UTC&interpolateParams=true",
			user, pass, host, db), nil
	}
	return dsn, nil
}

func LoadOrderEvents(ctx context.Context, db *sql.DB, table string, obsBefore time.Time) ([]models.RawEvent, error) {
	if !regexp.MustCompile(`^[A-Za-z0-9_]+$`).MatchString(table) {
		return nil, fmt.Errorf("table invalide")
	}
	const layout = "2006-01-02 15:04:05"
	pObs := obsBefore.Format(layout)
	q := fmt.Sprintf(`
		SELECT
			ced.CustomerID,
			ced.EventDate,
			COALESCE(ced.Quantity, 1) AS qty,
			CAST(JSON_EXTRACT(ced.Digest, '$.price.originalUnitPrice') AS DECIMAL(18,6)) AS unit_price
		FROM %s ced
		WHERE ced.EventTypeID = ?
		  AND ced.EventDate < ?
	`, table)

	rows, err := db.QueryContext(ctx, q, orderEventTypeID, pObs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.RawEvent, 0, 1024) // todo to explain why 1024
	var n int
	for rows.Next() {
		var ev models.RawEvent
		if err := rows.Scan(&ev.CustomerID, &ev.EventDate, &ev.Quantity, &ev.UnitPrice); err != nil {
			return nil, err
		}
		out = append(out, ev)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	log.Printf("[INFO] [LOAD] events: %d", n)
	return out, nil
}

func LoadCohortCustomers(
	ctx context.Context,
	db *sql.DB,
	table string,
	cohortStart, cohortEnd time.Time,
) ([]models.CohortCustomer, error) {

	if !regexp.MustCompile(`^[A-Za-z0-9_]+$`).MatchString(table) {
		return nil, fmt.Errorf("table invalide")
	}

	const layout = "2006-01-02 15:04:05"
	cStart := cohortStart.Format(layout)
	cEnd := cohortEnd.Format(layout)

	q := fmt.Sprintf(`
		SELECT ced.CustomerID,
			MIN(ced.EventDate) AS firstDt
		FROM %s ced
		WHERE ced.EventTypeID = ? AND ced.EventDate < ?
		GROUP BY ced.CustomerID
		HAVING MIN(ced.EventDate) >= ?
	`, table)

	rows, err := db.QueryContext(ctx, q, orderEventTypeID, cEnd, cStart)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.CohortCustomer, 0, 1024)
	count := 0
	for rows.Next() {
		var r models.CohortCustomer
		if err := rows.Scan(&r.CustomerID, &r.FirstOrderDT); err != nil {
			return nil, err
		}
		out = append(out, r)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	log.Printf("[INFO] [LOAD] cohort customers %s..%s: %d",
		cStart, cEnd, count)

	return out, nil
}

func LoadOrderEventsWithCustomersID(ctx context.Context, db *sql.DB, table string, customersID []models.CohortCustomer, obsBefore time.Time) ([]models.RawEvent, error) {
	if !regexp.MustCompile(`^[A-Za-z0-9_]+$`).MatchString(table) {
		return nil, fmt.Errorf("table invalide")
	}
	const layout = "2006-01-02 15:04:05"
	pObs := obsBefore.Format(layout)

	if len(customersID) == 0 {
		return []models.RawEvent{}, nil
	}
	ids := make([]any, 0, len(customersID))
	for _, c := range customersID {
		ids = append(ids, c.CustomerID)
	}
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1] // drop trailing comma

	q := fmt.Sprintf(`
		SELECT
			ced.CustomerID,
			ced.EventDate,
			COALESCE(ced.Quantity, 1) AS qty,
			CAST(JSON_EXTRACT(ced.Digest, '$.price.originalUnitPrice') AS DECIMAL(18,6)) AS unit_price
		FROM %s ced
		WHERE ced.EventTypeID = ?
		  AND ced.CustomerID IN (%s)
		  AND ced.EventDate < ?
	`, table, placeholders)
	args := make([]any, 0, 1+len(ids)+1)
	args = append(args, orderEventTypeID)
	args = append(args, ids...)
	args = append(args, pObs)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.RawEvent, 0, 1024) // todo to explain why 1024
	var n int
	for rows.Next() {
		var ev models.RawEvent
		if err := rows.Scan(&ev.CustomerID, &ev.EventDate, &ev.Quantity, &ev.UnitPrice); err != nil {
			return nil, err
		}
		out = append(out, ev)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	log.Printf("[INFO] [LOAD] events: %d", n)
	return out, nil
}
