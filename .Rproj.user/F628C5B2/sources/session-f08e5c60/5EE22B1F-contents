## Part D: Delete Database
## Maggie Chua
## Summer 2 2025

# Deletes DB tables if they exist
dbExecute(dbcon, "
  DROP TABLE IF EXISTS Airline, Aircraft, Airport, Reporter,
  IncidentType, Flight, Incident, incidents_stage
")
dbListTables(dbcon)

# Disconnect from DB
status <- dbDisconnect(dbcon)

# Clear all objects
rm(list = ls())
