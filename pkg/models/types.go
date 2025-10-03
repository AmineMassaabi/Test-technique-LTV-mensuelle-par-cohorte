package models

import (
	"time"
)

/*
LOAD → types simples pour charger en 2 requêtes :
- RawEvent : une ligne brute (client, datetime, qty, unit price)
- CohortCustomer  : ligne pour DISTINCT/GROUP BY customer
*/

type RawEvent struct {
	CustomerID uint64
	EventDate  time.Time
	Quantity   int
	UnitPrice  float64 // 0 si absent/indisponible
}

type CohortCustomer  struct {
	CustomerID uint64
	FirstOrderDT time.Time
}

/*
COMPUTE → structure de résultat exportée par mois de cohorte
*/

type CohortResult struct {
	MonthYear       string
	LTVAvg          float64
	CohortClients   int
	EventsRead      int
}

/*
CONFIG → paramètres globaux
*/

type Config struct {
	StartMonthInclusive string    // "MMYYYY"
	EndMonthInclusive   string    // "MMYYYY"
	Observation         time.Time // borne haute (ex: 1er jour du mois courant) – en UTC
	Verbose             bool
}
