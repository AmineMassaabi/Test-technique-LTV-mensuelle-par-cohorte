package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"ltv-monthly/pkg/models"
	"ltv-monthly/pkg/calculator"
	"ltv-monthly/pkg/database"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Flags simplifiés
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

	// Connexion DB
	db, dsnUsed, err := database.Open(*dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if *verbose {
		log.Printf("[INFO] connected dsn=%s", dsnUsed)
	}

	// Calcul
	ctx := context.Background()
	results, err := calculator.Run(ctx, db, models.Config{
		StartMonthInclusive: *startMonth,
		EndMonthInclusive:   *endMonth,
		Observation:         obs,
		Verbose:             *verbose,
	})
	if err != nil {
		log.Fatalf("compute: %v", err)
	}

	// Sortie enrichie : MM/YYYY ; LTV ; cohort_clients ; events ; priced
	for _, r := range results {
		fmt.Printf("%s ; %.15f ; cohort_clients=%d ; events=%d ; priced=%d\n",
			r.MonthYear, r.LTVAvg, r.CohortClients, r.EventsRead, r.EventsWithPrice)
	}
}
