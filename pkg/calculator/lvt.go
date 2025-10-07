package calculator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"ltv-monthly/pkg/database"
	"ltv-monthly/pkg/models"
	"time"
)

// RunRamOptimized est une version optimisée du calcul de LTV.
func RunRamOptimized(ctx context.Context, db *sql.DB, cfg models.Config) ([]models.CohortResult, error) {

	// Date Validation
	start, err := parseMonth(cfg.StartMonthInclusive)
	if err != nil {
		return nil, fmt.Errorf("start_month: %w", err)
	}
	end, err := parseMonth(cfg.EndMonthInclusive)
	if err != nil {
		return nil, fmt.Errorf("end_month: %w", err)
	}
	if end.Before(start) {
		return nil, fmt.Errorf("end_date < start_date")
	}

	months := monthsBetweenInclusive(start, end)
	rangeEnd := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)

	// 2. [OPTIMISATION] Charge uniquement les clients dont la première commande
	// se situe dans la plage de dates des cohortes. Le calcul du MIN(EventDate)
	// est délégué à la base de données, ce qui est beaucoup plus performant.
	allCohortCustomers, err := database.LoadCohortCustomers(ctx, db, start, rangeEnd, cfg)
	if err != nil {
		return nil, fmt.Errorf("load cohort customers: %w", err)
	}
	if len(allCohortCustomers) == 0 {
		// nothing to compute; still return rows with zeros for each month
		results := make([]models.CohortResult, 0, len(months))
		for _, m := range months {
			results = append(results, models.CohortResult{
				MonthYear:     fmt.Sprintf("%02d/%04d", int(m.Month()), m.Year()),
				LTVAvg:        0,
				CohortClients: 0,
				EventsRead:    0,
			})
		}
		return results, nil
	}

	// Prépare les structures de données pour les étapes suivantes.
	customersIDs := make([]models.CohortCustomer, 0, len(allCohortCustomers))
	firstByCustomer := make(map[uint64]time.Time, len(allCohortCustomers))
	for _, cc := range allCohortCustomers {
		customersIDs = append(customersIDs, models.CohortCustomer{CustomerID: cc.CustomerID})
		firstByCustomer[cc.CustomerID] = cc.FirstOrderDT
	}

	if cfg.Verbose {
		log.Printf("[INFO] [STEP] Load events for %d customers (< %s)",
			len(customersIDs), cfg.Observation.UTC().Format(time.RFC3339))
	}

	// 3. [OPTIMISATION] Charge les événements de commande UNIQUEMENT pour les clients identifiés précédemment.
	events, err := database.LoadOrderEventsWithCustomersID(ctx, db, customersIDs, cfg.Observation, cfg)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}

	if cfg.Verbose {
		log.Printf("[INFO] [STEP] Aggregate purchases per customer (UnitPrice*Quantity)")
	}

	// 4. Agrège le revenu total pour chaque client (sur le jeu de données réduit).
	sumByCustomer := make(map[uint64]float64, len(customersIDs))
	eventsCountByCustomer := make(map[uint64]int, len(customersIDs))

	for _, ev := range events {
		if ev.UnitPrice > 0 && ev.Quantity > 0 {
			sumByCustomer[ev.CustomerID] += ev.UnitPrice * float64(ev.Quantity)
			eventsCountByCustomer[ev.CustomerID]++
		}
	}

	// 5. Itère sur chaque mois pour construire les cohortes et calculer la LTV.
	results := make([]models.CohortResult, 0, len(months))
	for _, m := range months {
		cohortStart := time.Date(m.Year(), m.Month(), 1, 0, 0, 0, 0, time.UTC)
		cohortEnd := cohortStart.AddDate(0, 1, 0)

		cohortClients := 0
		totalRevenue := 0.0
		totalEvents := 0

		for _, cc := range allCohortCustomers {
			first := firstByCustomer[cc.CustomerID]
			if !first.Before(cohortStart) && first.Before(cohortEnd) {
				cohortClients++
				totalRevenue += sumByCustomer[cc.CustomerID]
				totalEvents += eventsCountByCustomer[cc.CustomerID]
			}
		}

		ltv := 0.0
		if cohortClients > 0 {
			ltv = totalRevenue / float64(cohortClients)
		}

		results = append(results, models.CohortResult{
			MonthYear:     fmt.Sprintf("%02d/%04d", int(m.Month()), m.Year()),
			LTVAvg:        ltv,
			CohortClients: cohortClients,
			EventsRead:    totalEvents, // priced events used in revenue
		})

		if cfg.Verbose {
			log.Printf("[INFO] %s -> LTV=%.6f | clients=%d | events=%d",
				fmt.Sprintf("%02d/%04d", int(m.Month()), m.Year()),
				ltv, cohortClients, totalEvents)
		}
	}

	return results, nil
}


// runCore factorise Run et RunWithInsertDateFromCustomerEvent
func runCore(ctx context.Context, db *sql.DB, cfg models.Config, useInsertDate bool) ([]models.CohortResult, error) {
	// 0) validation
	start, err := parseMonth(cfg.StartMonthInclusive)
	if err != nil {
		return nil, fmt.Errorf("start_month: %w", err)
	}
	end, err := parseMonth(cfg.EndMonthInclusive)
	if err != nil {
		return nil, fmt.Errorf("end_month: %w", err)
	}
	if end.Before(start) {
		return nil, fmt.Errorf("end_date < start_date")
	}
	months := monthsBetweenInclusive(start, end)

	if cfg.Verbose {
		log.Printf("[INFO] [STEP] Load events < Observation=%s", cfg.Observation.Format(time.RFC3339))
	}

	// 1) chargement des events
	events, err := database.LoadOrderEvents(ctx, db, cfg.Observation, cfg)
	if err != nil {
		return nil, err
	}

	// 1b) si demandé, override EventDate par InsertDate
	if useInsertDate {
		ins, err := database.LoadOrdersInsertDate(ctx, db, events, cfg.Observation, cfg)
		if err != nil {
			return nil, err
		}
		idx := make(map[uint64]time.Time, len(ins))
		for _, x := range ins {
			// si doublons, on retient l'InsertDate la plus ancienne
			if cur, ok := idx[x.EventID]; !ok || x.InsertDate.Before(cur) {
				idx[x.EventID] = x.InsertDate
			}
		}
		for i := range events {
			if d, ok := idx[events[i].EventID]; ok {
				events[i].EventDate = d
			}
		}
	}

	// 2) agrégation en 1 seul passage (pas besoin de customersSet + passes séparées)
	minFirst := make(map[uint64]time.Time, 1024)
	sumByCustomer := make(map[uint64]float64, 1024)
	eventsByCustomer := make(map[uint64]int, 1024)

	eventsRead := len(events)
	eventsWithPrice := 0

	for _, ev := range events {
		// min première date
		if t0, ok := minFirst[ev.CustomerID]; !ok || ev.EventDate.Before(t0) || t0.IsZero() {
			minFirst[ev.CustomerID] = ev.EventDate
		}
		// revenus + nombre d'événements "pricing"
		if ev.UnitPrice > 0 && ev.Quantity > 0 {
			sumByCustomer[ev.CustomerID] += ev.UnitPrice * float64(ev.Quantity)
			eventsByCustomer[ev.CustomerID]++
			eventsWithPrice++
		}
	}

	if cfg.Verbose {
		log.Printf("[INFO] [STEP] aggregated: events=%d, eventsWithPrice=%d, customers=%d",
			eventsRead, eventsWithPrice, len(minFirst))
		log.Printf("[INFO] [STEP] project to cohorts per month")
	}

	// 3) projection en cohortes
	type bucket struct {
		clients int
		total   float64
		events  int
	}
	byMonth := make(map[string]bucket, len(months))

	for cid, first := range minFirst {
		if first.IsZero() {
			continue
		}
		key := formatMonth(time.Date(first.Year(), first.Month(), 1, 0, 0, 0, 0, time.UTC))
		b := byMonth[key]
		b.clients++
		b.total += sumByCustomer[cid]
		b.events += eventsByCustomer[cid]
		byMonth[key] = b
	}

	// 4) construction des résultats dans l’ordre des mois demandés
	results := make([]models.CohortResult, 0, len(months))
	for _, m := range months {
		cohortStart := time.Date(m.Year(), m.Month(), 1, 0, 0, 0, 0, time.UTC)
		key := formatMonth(cohortStart)
		b := byMonth[key]

		ltv := 0.0
		if b.clients > 0 {
			ltv = b.total / float64(b.clients)
		}

		results = append(results, models.CohortResult{
			MonthYear:     key,
			LTVAvg:        ltv,
			CohortClients: b.clients,
			EventsRead:    b.events,
		})

		if cfg.Verbose {
			log.Printf("[INFO] %s -> LTV=%.6f | clients=%d | events=%d", key, ltv, b.clients, b.events)
		}
	}

	return results, nil
}
func Run(ctx context.Context, db *sql.DB, cfg models.Config) ([]models.CohortResult, error) {
	return runCore(ctx, db, cfg, false)
}

func RunWithInsertDateFromCustomerEvent(ctx context.Context, db *sql.DB, cfg models.Config) ([]models.CohortResult, error) {
	return runCore(ctx, db, cfg, true)
}


// parseMonth("MMYYYY") -> 1er jour du mois UTC
func parseMonth(mmyyyy string) (time.Time, error) {
	var month, year int
	_, err := fmt.Sscanf(mmyyyy, "%2d%4d", &month, &year)

	if len(mmyyyy) != 6 || err != nil {
		return time.Time{}, fmt.Errorf("format attendu MMYYYY (ex: 012025)")
	}
	if month < 1 || month > 12 {
		return time.Time{}, fmt.Errorf("mois invalide")
	}
	return time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC), nil
}

func monthsBetweenInclusive(start, end time.Time) []time.Time {
	cur := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	last := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	var out []time.Time
	for !cur.After(last) {
		out = append(out, cur)
		cur = cur.AddDate(0, 1, 0)
	}
	return out
}

func formatMonth(t time.Time) string {
	return fmt.Sprintf("%02d/%04d", int(t.Month()), t.Year())
}
