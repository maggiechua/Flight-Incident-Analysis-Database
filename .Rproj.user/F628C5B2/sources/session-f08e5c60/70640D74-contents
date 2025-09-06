## Part E: Populate Database
## Maggie Chua
## Summer 2 2025

# Reusable Functions to Populate DB Tables
rowBatchNum <- 500
startRow <- 1
endRow <- rowBatchNum

lookupTableCols <- c("airline", "aircraft", "depAirport", "incidentType",
                     "reportedBy")
lookupTableNames <- c('Airline', 'Aircraft', 'Airport', 'IncidentType',
                      'Reporter')

## Function to determine number of packages of data that need
## to be sent to the db
determineNumPackages <- function(rows) {
  numPackages <- length(rows) %/% rowBatchNum
  if (length(rows) %% rowBatchNum != 0) {
    numPackages <- numPackages + 1
  }
  return(numPackages)
}

## Function to bulk insert data into a given table using provided
## data and query header
bulkInsertData <- function(rows, table_name, table_cols) {
  numPackages <- determineNumPackages(rows)
  
  for (p in 1:numPackages) {
    if (p == numPackages && numPackages != 1) {
      endRow <- length(rows)
    } else {
      endRow <- p * rowBatchNum
    }
    
    if (p > 1) {
      startRow <- (p - 1) * rowBatchNum + 1
    } 
    
    rowVals <- rows[startRow:endRow]
    package <-  glue_sql_collapse(rowVals, sep = ", ")
    
    bulkInsertQuery <- glue_sql("
      INSERT INTO {table_name} ({table_cols}) VALUES {package}
    ", table_name = DBI::SQL(table_name), table_cols = DBI::SQL(table_cols),
                                package = DBI::SQL(package), .con = dbcon)
    DBI::dbExecute(dbcon, bulkInsertQuery)
  }
}

## Function to populate lookup tables with reusable unique 
## column values
populateLookupTables <- function() {
  for (c in 1:length(lookupTableCols)) {
    lookupCol <- lookupTableCols[c]
    factorCol <- factor(df[[lookupCol]])
    factorLevels <- levels(factorCol)
    numUnqLevels <- length(factorLevels)
    
    package <- list()
    for (i in 1:numUnqLevels) {
      item <- factorLevels[i]
      vals <- glue::glue_sql(
        "({item})", .con = dbcon
      )
      package <- append(package, list(vals))
    }
    package <- glue_sql_collapse(package, sep = ", ")
    
    lookupTableLabel <- lookupTableNames[c]
    queryHeader <- sprintf("%s (name)",
                           lookupTableLabel)
    lookupQuery <- glue_sql("
      INSERT INTO {queryHeader} VALUES {vals_sql}
    ", queryHeader = DBI::SQL(queryHeader), vals_sql = DBI::SQL(package), 
                            .con = dbcon)
    DBI::dbExecute(dbcon, lookupQuery)
  }
}

## Helper Function to Format Date Values and Cast Them as Type Date
formatDate <- function(date) {
  dateSplit <- strsplit(date, "[.]")
  day <- dateSplit[[1]][1]
  month <- dateSplit[[1]][2]
  year <- dateSplit[[1]][3]
  formattedDate <-sprintf("%04d-%02d-%02d", as.numeric(year), 
                          as.numeric(month), as.numeric(day))
  return(as.Date(formattedDate))
}

# Load Data from CSV File
url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/incidents-v2.csv"
csvData <- read.csv(url, header = TRUE, stringsAsFactors = FALSE)
csvData <- subset(csvData, select = -c(X))

## Extract CSV row data into a list of tuples
rowsData <- list()
for (r in 1:nrow(csvData)) {
  row <- csvData[r,]
  flightDate <- formatDate(row$date)
  row_iid <- row$iid
  isId <- sub("i", "", row_iid)
  rowVals <- glue::glue_sql(
    "({isId}, {row$iid}, {flightDate}, {row$airline}, {row$flightNumber},
      {row$dep.airport}, {row$incidentType}, {row$severity},
      {row$delay}, {row$num.injuries}, {row$reported.by},
      {row$aircraft})
    ", .con = dbcon)
  rowsData <- append(rowsData, list(rowVals))
}

## Create staging table to store all CSV Data
## Sets missing data as NULL as R has a function anyNA() that can only check 
## for NA values, especially in the case where we want to remove invalid data 
## from the dataset prior to loading
dbExecute(dbcon, "
  CREATE TABLE IF NOT EXISTS incidents_stage (
    isId INTEGER PRIMARY KEY,
    iid varchar(16) DEFAULT NULL,
    date DATE DEFAULT NULL,
    airline varchar(2) DEFAULT NULL,
    flightNumber INTEGER DEFAULT NULL,
    depAirport varchar(3) DEFAULT NULL,
    incidentType varchar(36) DEFAULT NULL,
    severity varchar(16) DEFAULT NULL,
    delay INTEGER DEFAULT NULL,
    numInjuries INTEGER DEFAULT NULL,
    reportedBy varchar(36) DEFAULT NULL,
    aircraft varchar(36)
  )
")

# Populate Staging Table
attributes <- paste0("isId, iid, date, airline, flightNumber, depAirport, ",
"incidentType, severity, delay, numInjuries, reportedBy, aircraft")
bulkInsertData(rowsData, "incidents_stage", attributes)

# Populate Tables with Appropriate Data
df <- dbGetQuery(dbcon, "SELECT * FROM incidents_stage")
df
## Populate Lookup Tables
populateLookupTables()

airline_df <- dbGetQuery(dbcon, "SELECT * FROM Airline")
aircraft_df <- dbGetQuery(dbcon, "SELECT * FROM Aircraft")
airport_df <- dbGetQuery(dbcon, "SELECT * FROM Airport")
incidentType_df <- dbGetQuery(dbcon, "SELECT * FROM IncidentType")
reporter_df <- dbGetQuery(dbcon, "SELECT * FROM Reporter")

## Populate Flight Table
flightAttributes <- paste0("alId, acId, apId, flightNum, flightDate, delay")

## Limitation: If I had more time, I would probably also check for
## duplicates to prevent duplicate Flight entries from being uploaded
checkForFlightDuplicates <- dbGetQuery(dbcon, "
  SELECT
    date,
    flightNumber
    airline,
    aircraft,
    depAirport,
    delay,
    COUNT(*) AS DuplicateCount 
  FROM incidents_stage s
  GROUP BY date, flightNumber, airline, aircraft, depAirport, delay
  HAVING COUNT(*) > 1
")

flightJoinTable <- dbGetQuery(dbcon, "
  SELECT *
  FROM incidents_stage s
  LEFT JOIN Airline a 
    ON s.airline=a.name
  LEFT JOIN Aircraft c
    ON s.aircraft=c.name
  LEFT JOIN Airport p
    ON s.depAirport=p.name
")

flightDF <- data.frame(
  alId = flightJoinTable$alId,
  acId = flightJoinTable$acId,
  apId = flightJoinTable$apId,
  flightNum = flightJoinTable$flightNumber,
  flightDate = flightJoinTable$date,
  delay = flightJoinTable$delay
)

flightData <- list()
for (r in 1:nrow(flightDF)) {
  row <- flightDF[r,]
  # checks if necessary row values are NULL
  # prevents invalid rows from being uploaded
  necessaryAttributes <- c(row$alId, row$acId, row$apId, row$flightNum,
                           row$flightDate)
  if (anyNA(necessaryAttributes) == FALSE) {
    rowVals <- glue::glue_sql(
      "({row$alId}, {row$acId}, {row$apId}, {row$flightNum}, 
      {row$flightDate}, {row$delay})
    ", .con = dbcon)
    flightData <- append(flightData, list(rowVals))
  }
}

bulkInsertData(flightData, "Flight", flightAttributes)

flights_df <- dbGetQuery(dbcon, "SELECT * FROM Flight")

## Populate Incident Table
incidentAttributes <- paste0("iid, fId, itId, rId, severity, injuries")

incidentJoinTable <- dbGetQuery(dbcon, "
  SELECT * FROM incidents_stage s
  LEFT JOIN IncidentType i
    ON s.incidentType=i.name
  LEFT JOIN Reporter r
    ON s.reportedBy=r.name
  LEFT JOIN Airport a
    ON s.depAirport=a.name
  LEFT JOIN Flight f
    ON s.flightNumber=f.flightNum AND s.date=f.flightDate
    AND s.delay=f.delay AND f.apId=a.apId
")

incidentDF <- data.frame(
  inId = incidentJoinTable$isId,
  iid = incidentJoinTable$iid,
  fId = incidentJoinTable$fId,
  itId = incidentJoinTable$itId,
  rId = incidentJoinTable$rId,
  severity = incidentJoinTable$severity,
  injuries = incidentJoinTable$numInjuries
)

incidentData <- list()
for (r in 1:nrow(incidentDF)) {
  row <- incidentDF[r,]
  # checks if necessary row values are NULL
  # prevents invalid rows from being uploaded
  necessaryAttributes <- c(row$iid, row$fid, row$itId, row$rId)
  if (anyNA(necessaryAttributes) == FALSE) {
    rowVals <- glue::glue_sql(
      "({row$iid}, {row$fId}, {row$itId}, {row$rId},
      {row$severity}, {row$injuries})
      ", .con = dbcon)
    incidentData <- append(incidentData, list(rowVals))
  }
}
bulkInsertData(incidentData, "Incident", incidentAttributes)

incidents_df <- dbGetQuery(dbcon, "SELECT * FROM Incident")