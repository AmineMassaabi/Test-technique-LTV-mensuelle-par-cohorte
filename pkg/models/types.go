package models

import (
	"database/sql"
	"encoding/json"
	"time"
)

// EventData reste disponible si tu veux tester/parsers localement plus tard.
type EventData struct {
	ClientCustomerID uint64
	ExternalEventID  string
	ContentSystemID  string
	EventDate        time.Time
	Quantity         int
	DigestJSON       sql.NullString
}

type Digest struct {
	Price struct {
		OriginalUnitPrice float64 `json:"originalUnitPrice"`
		Currency          string  `json:"currency,omitempty"`
	} `json:"price"`
}

// Utilitaire (non utilisé par la voie SQL-first, mais utile pour tests futurs)
func (e *EventData) UnitPrice() (float64, error) {
	if !e.DigestJSON.Valid || e.DigestJSON.String == "" {
		return 0, nil
	}
	var d Digest
	if err := json.Unmarshal([]byte(e.DigestJSON.String), &d); err != nil {
		return 0, err
	}
	return d.Price.OriginalUnitPrice, nil
}

// Résultat d'une cohorte + compteurs
type CohortResult struct {
	MonthYear       string
	LTVAvg          float64
	CohortClients   int
	EventsRead      int
	EventsWithPrice int
}

// Borne d'un mois [Start, End)
type CohortWindow struct {
	Start time.Time
	End   time.Time
}


type Config struct {
	StartMonthInclusive string // "MMYYYY"
	EndMonthInclusive   string // "MMYYYY"
	Observation         time.Time
	Verbose             bool
}
