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
	// Configure les logs pour inclure le timestamp avec les microsecondes.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	totalStart := time.Now()

	// Flags
	// -dsn: Data Source Name pour la connexion à la base de données.
	// -start_month: Mois de début pour l'analyse (format MMYYYY).
	// -end_month: Mois de fin pour l'analyse (format MMYYYY).
	// -v:(Optional, default=true) Active le mode verbeux pour des logs détaillés.
	// -rro:(Optional, default=false) Pour calculer la LVT Moyenne par la fonction RunRamOptimized.
	// -runWithInsertDateFromCustomerEvent:(Optional, default=false) Pour calculer la LVT Moyenne à l'aide de la colonne CustomerEvent.InsertDate.
	// -show_calculation_details:(Optional, default=false) afficher les details de calcul dans le stdout.

	dsn := flag.String("dsn", os.Getenv("LTV_MONTHLY_DSN"), "DSN MariaDB/MySQL (ex: mariadb://user:pwd@host:3306/db)")
	startMonth := flag.String("start_month", "", "Mois de début (MMYYYY)")
	endMonth := flag.String("end_month", "", "Mois de fin (MMYYYY)")
	verbose := flag.Bool("v", true, "Mode verbeux")
	runRamOptimized := flag.Bool("rro", false, "Run RAM Optimized function")
	runWithInsertDateFromCustomerEvent := flag.Bool("run_with_insertDate", false, "Run with insertDate from CustomerEvent")
	showCalculationDetails := flag.Bool("show_calculation_details", false, "show calculation details")
	flag.Parse()

	if *dsn == "" || *startMonth == "" || *endMonth == "" {
		log.Fatalf("Usage: ltv-monthly --dsn ... --start_month MMYYYY --end_month MMYYYY")
	}

	// Observation = 1er jour du mois courant (UTC)
	now := time.Now().UTC()
	obs := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	// Établit la connexion à la base de données.
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
	runner := calculator.Run
	mode := "normal"

	if *runWithInsertDateFromCustomerEvent {
		runner = calculator.RunWithInsertDateFromCustomerEvent
		mode = "withInsertDate"
	} else if *runRamOptimized {
		runner = calculator.RunRamOptimized
		mode = "ramOptimized"
	}

	if *verbose {
		log.Printf("[INFO] Calculation mode: %s", mode)
	}

	results, errRunning := runner(ctx, db, models.Config{
		StartMonthInclusive: *startMonth,
		EndMonthInclusive:   *endMonth,
		Observation:         obs,
		Verbose:             *verbose,
	})
	if errRunning != nil {
		log.Fatalf("[ERROR] compute: %v", err)
	}
	header := " month ; ltv_avg_gross_on_period"
	if *showCalculationDetails {
		header = " month ; ltv_avg_gross_on_period ; cohort_clients ; events"
	}
	fmt.Println(header)
	for _, r := range results {
		if *showCalculationDetails {
			fmt.Printf("%s ; %.15f ; %d ; %d\n", r.MonthYear, r.LTVAvg, r.CohortClients, r.EventsRead)
		} else {
			fmt.Printf("%s ; %.15f\n", r.MonthYear, r.LTVAvg)
		}

	}
	if *verbose {
		log.Printf("[INFO] total elapsed: %s", time.Since(totalStart))
	}

}
