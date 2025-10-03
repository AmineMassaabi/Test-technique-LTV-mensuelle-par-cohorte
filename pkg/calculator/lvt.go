package calculator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"ltv-monthly/pkg/database"
	"ltv-monthly/pkg/models"
	"time"

	"github.com/schollz/progressbar/v3"
)

const tableName = "CustomerEventData"

func Run(ctx context.Context, db *sql.DB, cfg models.Config) ([]models.CohortResult, error) {

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
	bar := progressbar.NewOptions(
		len(months),
		progressbar.OptionSetDescription("Calculate M"),
		progressbar.OptionThrottle(time.Millisecond), // <-- MAÎTRE DE LA FRÉQUENCE D’AFFICHAGE
	)

	if cfg.Verbose {
		log.Printf("[INFO] [STEP] Load events < Observation=%s", cfg.Observation.Format(time.RFC3339))
	}
	events, err := database.LoadOrderEvents(ctx, db, tableName, cfg.Observation)
	if err != nil {
		return nil, err
	}

	eventsRead := len(events)

	customersSet := make(map[uint64]struct{}, eventsRead)
	if cfg.Verbose {
		log.Printf("[INFO] [STEP] customers (distinct from events): %d", len(customersSet))
	}
	for _, ev := range events {
		customersSet[ev.CustomerID] = struct{}{}
	}

	if cfg.Verbose {
		log.Printf("[INFO] [STEP] Compute cohort (min order date by customer)")
	}
	minFirst := make(map[uint64]time.Time, len(customersSet))
	for cid := range customersSet {
		minFirst[cid] = time.Time{}
	}
	for _, ev := range events {
		if t0, ok := minFirst[ev.CustomerID]; ok {
			if t0.IsZero() || ev.EventDate.Before(t0) {
				minFirst[ev.CustomerID] = ev.EventDate
			}
		}
	}

	if cfg.Verbose {
		log.Printf("[INFO] [STEP] Aggregate purchases per customer (UnitPrice*Quantity)")
	}
	sumByCustomer := make(map[uint64]float64, len(customersSet))
	eventsByCustomer := make(map[uint64]int, len(customersSet))
	eventsWithPrice := 0
	for _, ev := range events {
		if ev.UnitPrice > 0 && ev.Quantity > 0 {
			sumByCustomer[ev.CustomerID] += ev.UnitPrice * float64(ev.Quantity)
			eventsByCustomer[ev.CustomerID]++
			eventsWithPrice++
		}
	}

	results := make([]models.CohortResult, 0, len(months))
	for _, m := range months {
		bar.Describe(fmt.Sprintf("cohort %02d/%04d", int(m.Month()), m.Year()))
		_ = bar.Add(1)
		cohortStart := time.Date(m.Year(), m.Month(), 1, 0, 0, 0, 0, time.UTC)
		cohortEnd := cohortStart.AddDate(0, 1, 0)

		cohortClients := 0
		totalEvents := 0
		total := 0.0
		for cid, first := range minFirst {
			if first.IsZero() {
				continue
			}
			if !first.Before(cohortStart) && first.Before(cohortEnd) {
				cohortClients++
				total += sumByCustomer[cid]
				totalEvents += eventsByCustomer[cid]
			}
		}

		ltv := 0.0
		if cohortClients > 0 {
			ltv = total / float64(cohortClients)
		}

		results = append(results, models.CohortResult{
			MonthYear:     formatMonth(cohortStart),
			LTVAvg:        ltv,
			CohortClients: cohortClients,
			EventsRead:    totalEvents,
		})

		if cfg.Verbose {
			log.Printf("[INFO] %s -> LTV=%.6f | clients=%d | events=%d",
				formatMonth(cohortStart), ltv, cohortClients, totalEvents)
		}
	}

	return results, nil
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

func Run1(ctx context.Context, db *sql.DB, cfg models.Config) ([]models.CohortResult, error) {

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
	monthBar := progressbar.NewOptions(
		len(months),
		progressbar.OptionSetDescription("Calculate M"),
		progressbar.OptionThrottle(time.Millisecond), // <-- MAÎTRE DE LA FRÉQUENCE D’AFFICHAGE
	)
	rangeEnd := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)

	allCohortCustomers, err := database.LoadCohortCustomers(ctx, db, tableName, start, rangeEnd)
	if err != nil {
		return nil, fmt.Errorf("load cohort customers: %w", err)
	}
	if len(allCohortCustomers) == 0 {
		// nothing to compute; still return rows with zeros for each month
		results := make([]models.CohortResult, 0, len(months))
		for _, m := range months {
			monthBar.Describe(fmt.Sprintf("cohort %02d/%04d", int(m.Month()), m.Year()))
			results = append(results, models.CohortResult{
				MonthYear:     fmt.Sprintf("%02d/%04d", int(m.Month()), m.Year()),
				LTVAvg:        0,
				CohortClients: 0,
				EventsRead:    0,
			})
			_ = monthBar.Add(1)
		}
		return results, nil
	}

	// Build a compact []CustomerIDOnly for the events loader
	customersIDs := make([]models.CohortCustomer, 0, len(allCohortCustomers))
	firstByCustomer := make(map[uint64]time.Time, len(allCohortCustomers))
	for _, cc := range allCohortCustomers {
		customersIDs = append(customersIDs, models.CohortCustomer{CustomerID: cc.CustomerID})
		firstByCustomer[cc.CustomerID] = cc.FirstOrderDT
	}

	// ---- LOAD ONCE: all events for those customers < Observation
	if cfg.Verbose {
		log.Printf("[INFO] [STEP] Load events for %d customers (< %s)",
			len(customersIDs), cfg.Observation.UTC().Format(time.RFC3339))
	}
	events, err := database.LoadOrderEventsWithCustomersID(ctx, db, tableName, customersIDs, cfg.Observation)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}

	// ---- COMPUTE (in-memory): per-customer totals up to Observation
	if cfg.Verbose {
		log.Printf("[INFO] [STEP] Aggregate purchases per customer (UnitPrice*Quantity)")
	}
	sumByCustomer := make(map[uint64]float64, len(customersIDs))
	eventsCountByCustomer := make(map[uint64]int, len(customersIDs))

	evBar := progressbar.NewOptions(
		len(events),
		progressbar.OptionSetDescription("aggregate events"),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionClearOnFinish(),
	)

	for i, ev := range events {
		if ev.UnitPrice > 0 && ev.Quantity > 0 {
			sumByCustomer[ev.CustomerID] += ev.UnitPrice * float64(ev.Quantity)
			eventsCountByCustomer[ev.CustomerID]++
		}
		if i%1000 == 0 {
			evBar.Describe(fmt.Sprintf("aggregate events (%d/%d)", i, len(events)))
		}
		_ = evBar.Add(1)
	}

	// ---- EXPORT per month: use firstByCustomer to assign cohort month
	results := make([]models.CohortResult, 0, len(months))
	for _, m := range months {
		cohortStart := time.Date(m.Year(), m.Month(), 1, 0, 0, 0, 0, time.UTC)
		cohortEnd := cohortStart.AddDate(0, 1, 0)

		evBar.Describe(fmt.Sprintf("cohort %02d/%04d", int(m.Month()), m.Year()))

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

		_ = evBar.Add(1)
		if cfg.Verbose {
			log.Printf("[INFO] %s -> LTV=%.6f | clients=%d | events=%d",
				fmt.Sprintf("%02d/%04d", int(m.Month()), m.Year()),
				ltv, cohortClients, totalEvents)
		}
	}

	return results, nil
}

