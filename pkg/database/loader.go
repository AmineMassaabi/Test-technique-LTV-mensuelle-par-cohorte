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

// Open DSN mariadb:// ou mysql:// → format MySQL driver
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
		return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=Local&interpolateParams=true",
			user, pass, host, db), nil
	}
	return dsn, nil
}

// ComputeCohortAvgFromDigest (DATETIME-safe)
// Montant = Digest.price.originalUnitPrice × Quantity
func ComputeCohortAvgFromDigest(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	cohortStart, cohortEnd, periodStart, periodEnd time.Time,
) (sql.NullFloat64, error) {

	if !regexp.MustCompile(`^[A-Za-z0-9_]+$`).MatchString(tableName) {
		return sql.NullFloat64{}, fmt.Errorf("table invalide")
	}

	// Always work in UTC and format as MySQL DATETIME strings
	const layout = "2006-01-02 15:04:05"
	cStart := cohortStart.UTC().Format(layout)
	cEnd := cohortEnd.UTC().Format(layout)
	pStart := periodStart.UTC().Format(layout)
	pEnd := periodEnd.UTC().Format(layout)

	// 1) Sous-requête cohorte : première commande dans [cohortStart, cohortEnd)
	subCohort := fmt.Sprintf(`
		SELECT ced.CustomerID
		FROM %s ced
		WHERE ced.EventTypeID = ?
		GROUP BY ced.CustomerID
		HAVING MIN(ced.EventDate) >= ? AND MIN(ced.EventDate) < ?
	`, tableName)

	// 2) Événements commande des clients de la cohorte dans [periodStart, periodEnd)
	q := fmt.Sprintf(`
		SELECT
			ced.CustomerID,
			COALESCE(ced.Quantity, 1) AS quantity,
			ced.Digest AS digestJSON
		FROM %s ced
		WHERE ced.EventTypeID = ?
		  AND ced.EventDate >= ? AND ced.EventDate < ?
		  AND ced.CustomerID IN (%s)
	`, tableName, subCohort)

	// Args alignés avec les "?" :
	args := []any{
		// période d'observation (requête principale)
		orderEventTypeID, pStart, pEnd,
		// fenêtre de cohorte (sous-requête)
		orderEventTypeID, cStart, cEnd,
	}

	// ---- logs bornes ----
	log.Printf("[DEBUG] Boundaries UTC: cohort=[%s ; %s) / period=[%s ; %s)", cStart, cEnd, pStart, pEnd)

	// Compter les clients de cohorte (debug)
	countCohortQ := fmt.Sprintf(`SELECT COUNT(*) FROM (%s) x`, subCohort)
	var cohortClients int
	if err := db.QueryRowContext(ctx, countCohortQ, orderEventTypeID, cStart, cEnd).Scan(&cohortClients); err == nil {
		log.Printf("[DEBUG] Clients de cohorte trouvés: %d", cohortClients)
	} else {
		log.Printf("[DEBUG] Cohort count error: %v", err)
	}

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return sql.NullFloat64{}, err
	}
	defer rows.Close()

	countEvents := 0
	parsedOK := 0
	sumByClient := map[uint64]float64{}

	for rows.Next() {
		countEvents++
		var (
			customerID uint64
			qty        sql.NullInt64
			digestJSON sql.NullString
		)
		if err := rows.Scan(&customerID, &qty, &digestJSON); err != nil {
			return sql.NullFloat64{}, err
		}

		ev := models.EventData{
			ClientCustomerID: customerID,
			Quantity:         1,
			DigestJSON:       digestJSON,
		}
		if qty.Valid && qty.Int64 > 0 {
			ev.Quantity = int(qty.Int64)
		}

		unit, err := ev.UnitPrice() // Digest.price.originalUnitPrice
		if err != nil {
			log.Printf("[DEBUG] JSON parse error client=%d err=%v raw=%s", customerID, err, digestJSON.String)
			continue
		}
		if unit <= 0 {
			log.Printf("[DEBUG] unit price <= 0 client=%d raw=%s", customerID, digestJSON.String)
			continue
		}
		parsedOK++
		sumByClient[customerID] += unit * float64(ev.Quantity)
	}
	if err := rows.Err(); err != nil {
		return sql.NullFloat64{}, err
	}

	log.Printf("[DEBUG] Events lus=%d, events avec prix OK=%d, clients agrégés=%d",
		countEvents, parsedOK, len(sumByClient),
	)

	if len(sumByClient) == 0 {
		return sql.NullFloat64{Valid: false}, nil
	}
	total := 0.0
	for _, v := range sumByClient {
		total += v
	}
	return sql.NullFloat64{Float64: total / float64(len(sumByClient)), Valid: true}, nil
}
