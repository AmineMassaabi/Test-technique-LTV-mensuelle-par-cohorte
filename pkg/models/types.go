package models

import (
	"database/sql"
	"time"
)

type EventData struct {
	ClientCustomerID uint64
	ExternalEventID  string
	ContentSystemID  string
	EventDate        time.Time
	Quantity         int
	DigestJSON       sql.NullString
}


// Résultat d'une cohorte + compteurs
type CohortResult struct {
	MonthYear       string
	LTVAvg          float64
	CohortClients   int
	EventsRead      int
	EventsWithPrice int
}


type Config struct {
	StartMonthInclusive string // "MMYYYY"
	EndMonthInclusive   string // "MMYYYY"
	Observation         time.Time
	Verbose             bool
}
