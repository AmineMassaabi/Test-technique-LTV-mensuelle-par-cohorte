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

	_ "github.com/go-sql-driver/mysql"
)

const orderEventTypeID = 6 // "Commande"

// Open DSN mariadb:// ou mysql:// → DSN driver MySQL avec loc=UTC
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
		// loc=UTC pour cohérence avec nos bornes UTC
		return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=UTC&interpolateParams=true",
			user, pass, host, db), nil
	}
	return dsn, nil
}

func ComputeCohortAvgFromDigest(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	cohortStart, cohortEnd, periodStart, periodEnd time.Time,
) (sql.NullFloat64, int, int, int, error) {

	if !regexp.MustCompile(`^[A-Za-z0-9_]+$`).MatchString(tableName) {
		return sql.NullFloat64{}, 0, 0, 0, fmt.Errorf("table invalide")
	}

	// Bornes DATETIME en UTC
	const layout = "2006-01-02 15:04:05"
	cStart := cohortStart.UTC().Format(layout)
	cEnd := cohortEnd.UTC().Format(layout)
	pStart := periodStart.UTC().Format(layout)
	pEnd := periodEnd.UTC().Format(layout)

	// Sous-requête: clients dont la 1re commande est dans la fenêtre de cohorte
	subCohort := fmt.Sprintf(`
		SELECT ced.CustomerID
		FROM %s ced
		WHERE ced.EventTypeID = ?
		GROUP BY ced.CustomerID
		HAVING MIN(ced.EventDate) >= ? AND MIN(ced.EventDate) < ?
	`, tableName)

	// 1) Nombre de clients de cohorte
	countCohortQ := fmt.Sprintf(`SELECT COUNT(*) FROM (%s) x`, subCohort)
	var cohortClients int
	if err := db.QueryRowContext(ctx, countCohortQ, orderEventTypeID, cStart, cEnd).Scan(&cohortClients); err != nil {
		return sql.NullFloat64{}, 0, 0, 0, err
	}
	if cohortClients == 0 {
		// Rien à calculer ; remonter compteurs à 0
		return sql.NullFloat64{Valid: false}, 0, 0, 0, nil
	}

	// 2) Statistiques d'événements + somme des montants
	// price_val = CAST(JSON_UNQUOTE(JSON_EXTRACT(ced.Digest, '$.price.originalUnitPrice')) AS DECIMAL(18,6))
	statsQ := fmt.Sprintf(`
	SELECT
		CAST(COUNT(*) AS UNSIGNED) AS events_read,
		CAST(SUM(
			CASE
				WHEN CAST(JSON_UNQUOTE(JSON_EXTRACT(ced.Digest, '$.price.originalUnitPrice')) AS DECIMAL(18,6)) > 0
				THEN 1 ELSE 0
			END
		) AS UNSIGNED) AS events_with_price,
		SUM(
			IFNULL(CAST(JSON_UNQUOTE(JSON_EXTRACT(ced.Digest, '$.price.originalUnitPrice')) AS DECIMAL(18,6)), 0)
			* COALESCE(ced.Quantity, 1)
		) AS gross_total
		FROM %s ced
		WHERE ced.EventTypeID = ?
		  AND ced.EventDate >= ? AND ced.EventDate < ?
		  AND ced.CustomerID IN (%s)
	`, tableName, subCohort)

	var eventsRead int
	var eventsWithPrice int
	var grossTotal sql.NullFloat64

	args := []any{
		// principale (période d'observation)
		orderEventTypeID, pStart, pEnd,
		// sous-requête cohorte
		orderEventTypeID, cStart, cEnd,
	}
	log.Printf(cStart, cEnd, pStart, pEnd)

	if err := db.QueryRowContext(ctx, statsQ, args...).Scan(&eventsRead, &eventsWithPrice, &grossTotal); err != nil {
		return sql.NullFloat64{}, cohortClients, 0, 0, err
	}

	if !grossTotal.Valid {
		return sql.NullFloat64{Valid: false}, cohortClients, eventsRead, eventsWithPrice, nil
	}
	ltv := sql.NullFloat64{
		Float64: grossTotal.Float64 / float64(cohortClients),
		Valid:   true,
	}

	// Logs de debug (optionnels)
	log.Printf("[DEBUG] Cohort clients=%d | events=%d | priced=%d | gross=%.6f | ltv=%.6f",
		cohortClients, eventsRead, eventsWithPrice, grossTotal.Float64, ltv.Float64,
	)

	return ltv, cohortClients, eventsRead, eventsWithPrice, nil
}
