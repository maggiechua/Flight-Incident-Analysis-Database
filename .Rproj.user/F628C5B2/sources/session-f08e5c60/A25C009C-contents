## Part F: Test Data Loading Process
## Maggie Chua
## Summer 2 2025

# Load Data from CSV File
url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/incidents-v2.csv"
csvData <- read.csv(url, header = TRUE, stringsAsFactors = FALSE)
csv_df <- subset(csvData, select = -c(X))

# Tests
## row counts of initial loaded data frames
test_num_rows <- function() {
  tryCatch({
    db_stagingTable <- nrow(dbGetQuery(dbcon, "
      SELECT * FROM incidents_stage
    "))
    csv_table <- nrow(csv_df)
    
    if (db_stagingTable == csv_table) {
      message("PASS: equivalent number of rows: ", csv_table)
    } else {
      message("FAIL: number of rows NOT equivalent. CSV: ",
              csv_table, " DB: ", db_stagingTable)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
}

## first and last dates in CSV
test_dates_match <- function() {
  tryCatch({
    dbFirstDate <- dbGetQuery(dbcon, "
      SELECT * FROM incidents_stage
      ORDER BY isId ASC
      LIMIT 1
    ")
    
    csvFirstRow <- csvData[1, ]
    csvFirstDate <- csvFirstRow$date
    csvFirstDateFormatted <- formatDate(csvFirstDate)
    
    if (dbFirstDate$date == csvFirstDateFormatted) {
      message("PASS: equivalent first row date values")
    } else {
      message("FAIL: first row date values NOT equivalent")
    }
    
    dbLastDate <- dbGetQuery(dbcon, "
      SELECT * FROM incidents_stage
      ORDER BY isId DESC
      LIMIT 1
    ")
    
    csvLastRow <- csvData[nrow(csvData), ]
    csvLastDate <- csvLastRow$date
    csvLastDateFormatted <- formatDate(csvLastDate)
    
    if (dbLastDate$date == csvLastDateFormatted) {
      message("PASS: equivalent last row date values")
    } else {
      message("FAIL: last date values NOT equivalent")
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
}

test_unq_values <- function() {
  ## number of unique airlines
  tryCatch({
    db_unqAirlines <- dbGetQuery(dbcon, "
      SELECT COUNT(alId) 
      FROM Airline
    ")
    csv_unqAirlines <- length(levels(factor(df$airline)))
    
    if (db_unqAirlines == csv_unqAirlines) {
      message("PASS: equivalent number of unique airlines: ", csv_unqAirlines)
    } else {
      message("FAIL: number of unique airlines NOT equivalent. CSV: ",
              csv_unqAirlines, " DB: ", db_unqAirlines)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
  
  ## number of unique aircraft
  tryCatch({
    db_unqAircrafts <- dbGetQuery(dbcon, "
      SELECT COUNT(acId) 
      FROM Aircraft
    ")
    csv_unqAircrafts <- length(levels(factor(df$aircraft)))
    
    if (db_unqAircrafts == csv_unqAircrafts) {
      message("PASS: equivalent number of unique aircrafts: ", 
              csv_unqAircrafts)
    } else {
      message("FAIL: number of unique aircrafts NOT equivalent. CSV: ",
              csv_unqAircrafts, " DB: ", db_unqAircrafts)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
  
  ## number of unique incidentTypes
  tryCatch({
    db_unqIncidentTypes <- dbGetQuery(dbcon, "
      SELECT COUNT(itId) 
      FROM IncidentType
    ")
    csv_unqIncidentTypes <- length(levels(factor(df$incidentType)))
    
    if (db_unqIncidentTypes == csv_unqIncidentTypes) {
      message("PASS: equivalent number of unique incident types: ", 
              csv_unqIncidentTypes)
    } else {
      message("FAIL: number of unique incident types NOT equivalent. CSV: ",
              csv_unqIncidentTypes, " DB: ", db_unqIncidentTypes)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
  
  ## number of unique reporters
  tryCatch({
    db_unqReporters <- dbGetQuery(dbcon, "
      SELECT COUNT(rId) 
      FROM Reporter
    ")
    csv_unqReporters <- length(levels(factor(df$reportedBy)))
    
    if (db_unqReporters == csv_unqReporters) {
      message("PASS: equivalent number of unique reporters: ", 
              csv_unqReporters)
    } else {
      message("FAIL: number of unique reporters NOT equivalent. CSV: ",
              csv_unqReporters, " DB: ", db_unqReporters)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
  
  ## number of unique departure airlines
  tryCatch({
    db_unqDepAirports <- nrow(dbGetQuery(dbcon, "
      SELECT * FROM Airport
    "))
    csv_unqDepAirports <- length(levels(factor(df$depAirport)))
    
    if (db_unqDepAirports == csv_unqDepAirports) {
      message("PASS: equivalent number of unique departure airlines: ", 
              csv_unqDepAirports)
    } else {
      message("FAIL: number of unique departure airlines NOT equivalent. CSV: ",
              csv_unqDepAirports, " DB: ", db_unqDepAirports)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
  
  ## number of unique severity labels
  tryCatch({
    db_unqSeverityLabels <- nrow(dbGetQuery(dbcon, "
      SELECT DISTINCT severity
      FROM incidents_stage
    "))
    csv_unqSeverityLabels <- length(levels(factor(df$severity)))
    
    if (db_unqSeverityLabels == csv_unqSeverityLabels) {
      message("PASS: equivalent number of unique severity labels: ", 
              csv_unqSeverityLabels)
    } else {
      message("FAIL: number of unique severity labels NOT equivalent. CSV: ",
              csv_unqSeverityLabels, " DB: ", db_unqSeverityLabels)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
  
  ## number of unique flights
  tryCatch({
    db_unqFlights <- dbGetQuery(dbcon, "
      SELECT COUNT(fId) 
      FROM Flight
    ")
    csv_flightDF <- data.frame(
      csvData$date,
      csvData$airline,
      csvData$flightNumber,
      csvData$dep.airport,
      csvData$delay,
      csvData$aircraft
    )
    csv_duplicate_rows <- duplicated(csv_flightDF)
    csv_unqFlights <- nrow(unique(csv_flightDF))
    
    if (db_unqFlights == csv_unqFlights) {
      message("PASS: equivalent number of unique flights: ", csv_unqFlights)
    } else {
      message("FAIL: number of unique flights NOT equivalent. CSV: ",
              csv_unqFlights, " DB: ", db_unqFlights)
    }
    
    ## TO-DO?: insert into both another incident value to test edge case
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
  
  ## number of unique incidents
  tryCatch({
    db_unqIncidents <- nrow(dbGetQuery(dbcon, "
      SELECT *
      FROM Incident
    "))
    csv_unqIncidents <- length(levels(factor(csv_df$iid)))
    
    if (db_unqIncidents == csv_unqIncidents) {
      message("PASS: equivalent number of unique incidents: ", 
              csv_unqIncidents)
    } else {
      message("FAIL: number of unique incidents NOT equivalent. CSV: ",
              csv_unqIncidents, " DB: ", db_unqIncidents)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
}

## avg flight delay
test_avg_flight_delay <- function() {
  tryCatch({
    db_delay_avg <- sprintf("%.2f", dbGetQuery(dbcon, "
      SELECT AVG(delay) 
      FROM incidents_stage
    "))
    csv_avg_delay <- sprintf("%.2f", mean(df$delay))
    db_delay_avg
    csv_avg_delay
    if (db_delay_avg == csv_avg_delay) {
      message("PASS: equivalent delay averages (in minutes): ", 
              csv_avg_delay)
    } else {
      message("FAIL: delay averages (in minutes) NOT equivalent. CSV: ",
              csv_avg_delay, " DB: ", db_delay_avg)
    }
  }, error = function(e) {
    message("FAIL: failed to retrieve data from query")
  })
}

