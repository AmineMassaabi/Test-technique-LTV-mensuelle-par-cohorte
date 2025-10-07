package models

import (
	"time"
)

/*
LOAD → types simples pour charger les données brutes de la base de données.
*/

// RawEventData représente un événement de commande brut tel qu'il est lu depuis la base de données.
type RawEventData struct {
	EventID    uint64
	CustomerID uint64
	EventDate  time.Time
	Quantity   int
	UnitPrice  float64
}

// RawEventsInsertDate représente un événement de commande avec sa date d'insertion tel qu'il est lu depuis la base de données.
type RawEventsInsertDate struct {
	EventID    uint64
	InsertDate  time.Time
}

// CohortCustomer représente un client avec la date de sa première commande, utilisée pour l'associer à une cohorte.
type CohortCustomer struct {
	CustomerID   uint64
	FirstOrderDT time.Time
}

/*
COMPUTE → structure de résultat exportée par mois de cohorte
*/
// CohortResult contient les métriques calculées pour une cohorte mensuelle.
type CohortResult struct {
	MonthYear     string  // Mois de la cohorte (format "MM/YYYY").
	LTVAvg        float64 // Lifetime Value moyenne des clients de la cohorte.
	CohortClients int     // Nombre total de clients dans la cohorte.
	EventsRead    int     // Nombre total d'événements de commande pour cette cohorte.
}

/*
CONFIG → paramètres globaux
*/
// Config contient les paramètres de configuration passés à la fonction de calcul.
type Config struct {
	StartMonthInclusive string    // "MMYYYY"
	EndMonthInclusive   string    // "MMYYYY"
	Observation         time.Time // borne haute (ex: 1er jour du mois courant) – en UTC
	Verbose             bool      // Flag pour activer les logs détaillés.
}
