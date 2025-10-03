package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"ltv-monthly/pkg/calculator"
	"ltv-monthly/pkg/database"
	"ltv-monthly/pkg/models"
)

func main() {
	// Logs : timestamp + niveau
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	totalStart := time.Now()

	// Flags
	dsn := flag.String("dsn", os.Getenv("LTV_MONTHLY_DSN"), "DSN MariaDB/MySQL (ex: mariadb://user:pwd@host:3306/db)")
	startMonth := flag.String("start_month", "", "Mois de début (MMYYYY)")
	endMonth := flag.String("end_month", "", "Mois de fin (MMYYYY)")
	verbose := flag.Bool("v", true, "Mode verbeux")
	flag.Parse()

	if *dsn == "" || *startMonth == "" || *endMonth == "" {
		log.Fatalf("Usage: ltv-monthly --dsn ... --start_month MMYYYY --end_month MMYYYY")
	}

	// Observation = 1er jour du mois courant (UTC)
	now := time.Now().UTC()
	obs := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	// LOAD: connexion
	db, dsnUsed, err := database.Open(*dsn)
	if err != nil {
		log.Fatalf("[ERROR] open db: %v", err)
	}
	defer db.Close()
	if *verbose {
		log.Printf("[INFO] connected dsn=%s", dsnUsed)
	}

	// COMPUTE → RUN
	ctx := context.Background()
	results, err := calculator.Run(ctx, db, models.Config{
		StartMonthInclusive: *startMonth,
		EndMonthInclusive:   *endMonth,
		Observation:         obs,
		Verbose:             *verbose,
	})
	if err != nil {
		log.Fatalf("[ERROR] compute: %v", err)
	}

	fmt.Println(" month | ltv_avg | cohort_clients | events")
	for _, r := range results {
		fmt.Printf("%s | %.15f | %d | %d\n", r.MonthYear, r.LTVAvg, r.CohortClients, r.EventsRead)
	}
	log.Printf("[INFO] total elapsed: %s", time.Since(totalStart))

}
