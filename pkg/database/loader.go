package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"ltv-monthly/pkg/models"

	_ "github.com/go-sql-driver/mysql"
)

const orderEventTypeID = 6 // "Purchase"

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

	// Configuration du pool de connexions.
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db, mysqlDSN, nil
}

// toMySQLDSN convertit une URL de type mariadb:// ou mysql:// en une chaîne DSN standard
// pour le driver Go. Il force l'utilisation de `loc=UTC` pour la cohérence des dates.
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

// LoadOrderEvents charge tous les événements de commande avant la date d'observation.
// Cette fonction est utilisée dans la première version (Run) qui charge tout en mémoire.
func LoadOrderEvents(ctx context.Context, db *sql.DB, obsBefore time.Time, cfg models.Config) ([]models.RawEventData, error) {
	const table = "CustomerEventData"

	const layout = "2006-01-02 15:04:05"
	pObs := obsBefore.Format(layout)
	q := fmt.Sprintf(`
		SELECT
			ced.EventID,
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

	out := make([]models.RawEventData, 0, 1024)
	var n int
	for rows.Next() {
		var ev models.RawEventData
		if err := rows.Scan(&ev.EventID, &ev.CustomerID, &ev.EventDate, &ev.Quantity, &ev.UnitPrice); err != nil {
			return nil, err
		}
		out = append(out, ev)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if cfg.Verbose {
		log.Printf("[INFO] [LOAD] events: %d", n)
	}
	return out, nil
}

// LoadOrderEvents charge tous les événements de commande avant la date d'observation.
// Cette fonction est utilisée dans la première version (Run) qui charge tout en mémoire.
func LoadOrdersInsertDate(ctx context.Context, db *sql.DB, eventsData []models.RawEventData, obsBefore time.Time, cfg models.Config) ([]models.RawEventsInsertDate, error) {
	const table = "CustomerEvent"
	const chunkSize = 1000

	const layout = "2006-01-02 15:04:05"
	pObs := obsBefore.Format(layout)

	if len(eventsData) == 0 {
		return []models.RawEventsInsertDate{}, nil
	}

	idsSet := make(map[uint64]struct{}, len(eventsData))
	for _, e := range eventsData {
		idsSet[e.EventID] = struct{}{}
	}
	ids := make([]uint64, 0, len(idsSet))
	for id := range idsSet {
		ids = append(ids, id)
	}

	out := make([]models.RawEventsInsertDate, 0, len(ids)) // capacité approximative
	// 2) Parcours par lots
	for start := 0; start < len(ids); start += chunkSize {
		end := start + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]

		// placeholders "?, ?, ?, ..."
		ph := strings.TrimRight(strings.Repeat("?,", len(batch)), ",")

		q := fmt.Sprintf(`
            SELECT ce.EventID, ce.InsertDate
            FROM %s ce
            WHERE ce.EventID IN (%s)
              AND ce.InsertDate < ?
        `, table, ph)

		// 3) Args: les IDs du lot + la borne de date
		args := make([]any, 0, len(batch)+1)
		for _, id := range batch {
			args = append(args, int64(id))
		}
		args = append(args, pObs)

		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		func() {
			defer rows.Close()
			for rows.Next() {
				var ev models.RawEventsInsertDate
				if err := rows.Scan(&ev.EventID, &ev.InsertDate); err != nil {
					out = nil
				}
				out = append(out, ev)
			}
		}()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if cfg.Verbose {
			log.Printf("[INFO] [LOAD] chunk %d..%d/%d (batch=%d)", start, end, len(ids), len(batch))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	if cfg.Verbose {
		log.Printf("[INFO] [LOAD] events Insert Dates total: %d", len(out))
	}
	return out, nil
}

// LoadCohortCustomers récupère les clients dont la première commande se situe dans l'intervalle de temps spécifié, les identifiant ainsi comme membres des cohortes de cette période.
func LoadCohortCustomers(ctx context.Context, db *sql.DB, cohortStart, cohortEnd time.Time, cfg models.Config) ([]models.CohortCustomer, error) {
	const table = "CustomerEventData"

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

	if cfg.Verbose {
		log.Printf("[INFO] [LOAD] cohort customers %s..%s: %d", cStart, cEnd, count)
	}
	return out, nil
}

// LoadOrderEventsWithCustomersID charge tous les événements de commande pour une liste spécifique de clients, avant la date d'observation.
func LoadOrderEventsWithCustomersID(ctx context.Context, db *sql.DB, customersID []models.CohortCustomer, obsBefore time.Time, cfg models.Config) ([]models.RawEventData, error) {
	const table = "CustomerEventData"

	const layout = "2006-01-02 15:04:05"
	pObs := obsBefore.Format(layout)

	if len(customersID) == 0 {
		return []models.RawEventData{}, nil
	}
	ids := make([]any, 0, len(customersID))
	for _, c := range customersID {
		ids = append(ids, c.CustomerID)
	}

	customersIDs := strings.Repeat("?,", len(ids))
	customersIDs = customersIDs[:len(customersIDs)-1]

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
	`, table, customersIDs)
	args := make([]any, 0, len(ids)+2)
	args = append(args, orderEventTypeID)
	args = append(args, ids...)
	args = append(args, pObs)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.RawEventData, 0, 1024)
	var n int
	for rows.Next() {
		var ev models.RawEventData
		if err := rows.Scan(&ev.CustomerID, &ev.EventDate, &ev.Quantity, &ev.UnitPrice); err != nil {
			return nil, err
		}
		out = append(out, ev)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if cfg.Verbose {
		log.Printf("[INFO] [LOAD] events by CustomerID: %d", n)
	}
	return out, nil
}
