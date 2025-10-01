package models

import (
	"database/sql"
	"encoding/json"
	"time"
)

// EventData = ligne d'événement (CustomerEventData)
type EventData struct {
	ClientCustomerID uint64
	ExternalEventID  string
	ContentSystemID  string
	EventDate        time.Time
	Quantity         int
	DigestJSON       sql.NullString // JSON brut ; on lit price.originalUnitPrice
}

// Digest = structure minimale pour extraire le prix unitaire original
type Digest struct {
	Price struct {
		OriginalUnitPrice float64 `json:"originalUnitPrice"`
		Currency          string  `json:"currency,omitempty"`
	} `json:"price"`
}

// UnitPrice retourne Digest.price.originalUnitPrice (0 si absent/JSON invalide)
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

// CohortResult = "MM/YYYY ; ltv"
type CohortResult struct {
	MonthYear string
	LTVAvg    float64
}

// CohortWindow = [Start, End)
type CohortWindow struct {
	Start time.Time
	End   time.Time
}
