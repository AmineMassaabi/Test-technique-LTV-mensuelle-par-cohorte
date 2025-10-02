package calculator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"ltv-monthly/pkg/database"
	"ltv-monthly/pkg/models"

	"github.com/schollz/progressbar/v3"
)


const tableName = "CustomerEventData"

func Run(ctx context.Context, db *sql.DB, cfg models.Config) ([]models.CohortResult, error) {
	start, err := parseMonth(cfg.StartMonthInclusive)
	if err != nil {
		return nil, fmt.Errorf("start_month: %w", err)
	}
	end, err := parseMonth(cfg.EndMonthInclusive)
	if err != nil {
		return nil, fmt.Errorf("end_month: %w", err)
	}
	if end.Before(start) {
		return nil, fmt.Errorf("end_month < start_month")
	}

	months := monthsBetweenInclusive(start, end)
	bar := progressbar.Default(int64(len(months)))

	results := make([]models.CohortResult, 0, len(months))
	for _, m := range months {
		cohortStart := time.Date(m.Year(), m.Month(), 1, 0, 0, 0, 0, time.UTC)
		cohortEnd := cohortStart.AddDate(0, 1, 0)
		periodStart := cohortStart
		periodEnd := time.Date(cfg.Observation.Year(), cfg.Observation.Month(), 1, 0, 0, 0, 0, time.UTC)

		avg, cohortClients, eventsRead, eventsWithPrice, err :=
			database.ComputeCohortAvgFromDigest(ctx, db, tableName, cohortStart, cohortEnd, periodStart, periodEnd)
		if err != nil {
			return nil, fmt.Errorf("compute %s: %w", formatMonth(cohortStart), err)
		}

		ltv := 0.0
		if avg.Valid {
			ltv = avg.Float64
		}
		results = append(results, models.CohortResult{
			MonthYear:       formatMonth(cohortStart),
			LTVAvg:          ltv,
			CohortClients:   cohortClients,
			EventsRead:      eventsRead,
			EventsWithPrice: eventsWithPrice,
		})

		_ = bar.Add(1)
		if cfg.Verbose {
			log.Printf("[INFO] %s -> LTV=%.6f | clients=%d events=%d priced=%d",
				formatMonth(cohortStart), ltv, cohortClients, eventsRead, eventsWithPrice)
		}
	}
	return results, nil
}

// parseMonth("MMYYYY") -> 1er jour du mois UTC
func parseMonth(mmyyyy string) (time.Time, error) {
	if len(mmyyyy) != 6 {
		return time.Time{}, fmt.Errorf("format attendu MMYYYY (ex: 012025)")
	}
	month := int(mmyyyy[0]-'0')*10 + int(mmyyyy[1]-'0')
	year := int(mmyyyy[2]-'0')*1000 + int(mmyyyy[3]-'0')*100 + int(mmyyyy[4]-'0')*10 + int(mmyyyy[5]-'0')
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
