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
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Flags CLI très simples
	dsn := flag.String("dsn", os.Getenv("LTV_MONTHLY_DSN"), "DSN MariaDB/MySQL (ex: mariadb://user:pwd@host:3306/datafy)")
	startMonth := flag.String("start_month", "", "Mois de début (MMYYYY)")
	endMonth := flag.String("end_month", "", "Mois de fin (MMYYYY)")
	verbose := flag.Bool("v", true, "Mode verbeux")
	flag.Parse()

	if *dsn == "" || *startMonth == "" || *endMonth == "" {
		log.Fatalf("Usage: ltv-monthly --dsn ... --start_month MMYYYY --end_month MMYYYY")
	}

	// Date d'observation = 1er jour du mois courant (00:00)
	now := time.Now()
	obs := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())

	// Connexion DB
	db, dsnUsed, err := database.Open(*dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if *verbose {
		log.Printf("[INFO] connected dsn=%s", dsnUsed)
	}

	// Calcul (table fixée: CustomerEventData)
	ctx := context.Background()
	results, err := calculator.Run(ctx, db, calculator.Config{
		StartMonthInclusive: *startMonth,
		EndMonthInclusive:   *endMonth,
		Observation:         obs,
		Verbose:             *verbose,
	})
	if err != nil {
		log.Fatalf("compute: %v", err)
	}

	// Sortie demandée: "MM/YYYY ; valeur"
	for _, r := range results {
		fmt.Printf("%s ; %.15f\n", r.MonthYear, r.LTVAvg)
	}
}
