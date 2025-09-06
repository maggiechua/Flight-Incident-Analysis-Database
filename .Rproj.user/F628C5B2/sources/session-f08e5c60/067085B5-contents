## Part H: Add Business Logic
## Maggie Chua
## Summer 2 2025

# Drop Stored Procedures 
dbExecute(dbcon, "DROP PROCEDURE IF EXISTS storeIncident")
dbExecute(dbcon, "DROP PROCEDURE IF EXISTS storeNewIncident")

# Stored Proc 1: storeIncident
dbExecute(dbcon, "
  CREATE PROCEDURE storeIncident ( IN
    iid varchar(16),
    flightDate DATE,
    airline varchar(2),
    flightNumber INT,
    depAirport varchar(3),
    incidentType varchar(36),
    severity varchar(36),
    delay INT,
    numInjuries INT,
    reportedBy varchar(36),
    aircraft varchar(16)
  )
  BEGIN
    DECLARE alId_val INT;
    DECLARE acId_val INT;
    DECLARE apId_val INT;
    DECLARE itId_val INT;
    DECLARE rId_val INT;
    DECLARE fId_val INT;
    
    DECLARE alName varchar(2);
    DECLARE acName varchar(16);
    DECLARE apName varchar(3);
    
    -- Retrieve corresponding Id values from lookup tables
    SELECT alId, name INTO alId_val, alName 
    FROM Airline WHERE name = airline;
    
    SELECT acId, name INTO acId_val, acName
    FROM Aircraft WHERE name = aircraft;
    
    SELECT apId, name INTO apId_val, apName
    FROM Airport WHERE name = depAirport;
    
    SELECT itId INTO itId_val FROM IncidentType WHERE name = incidentType;
    SELECT rId INTO rId_val FROM Reporter WHERE name = reportedBy;
    
    -- Check to see if flight already exists in Flight table
    BEGIN
      IF EXISTS (SELECT * FROM Flight f 
        WHERE f.flightNum = flightNumber AND
              f.flightDate = flightDate AND
              f.delay = delay AND
              alName = airline AND
              acName = aircraft AND
              apName = depAirport
              ) THEN
          SELECT fId INTO fId_val
          FROM Flight f
          WHERE f.flightNum = flightNumber AND
                f.flightDate = flightDate AND
                f.delay = delay AND
                alName = airline AND
                acName = aircraft AND
                apName = depAirport;
      END IF;
    END;
    
    BEGIN
      DECLARE EXIT HANDLER FOR SQLEXCEPTION
      BEGIN
        ROLLBACK;
      END;
      
      START TRANSACTION;
      -- If flight does not exist, insert a new flight into Flight table
      BEGIN
        IF fId_val = NULL THEN
          INSERT INTO Flight (alId, acId, apId, flightNum, flightDate, delay)
          VALUES (alId_val, acId_val, apId_val, flightNumber, flightDate, delay);
        END IF;
      END;
      
      COMMIT;
    END;
    
    BEGIN
      DECLARE EXIT HANDLER FOR SQLEXCEPTION
      BEGIN
        ROLLBACK;
      END;
      
      START TRANSACTION;
      -- Retrieve newly inserted flight Id 
      SELECT fId INTO fId_val FROM Flight f
        WHERE f.flightNum = flightNumber AND
              f.flightDate = date AND
              f.delay = delay AND
              f.alId = alId_val AND
              f.acId = acId_val AND
              f.apId = apId_val;
      
      -- Use retrieved values to insert new incident
      INSERT INTO Incident (iid, fId, itId, rId, severity, injuries)
      VALUES (iid, fId_val, itId_val, rId_val, severity, injuries);
      
      COMMIT;
    END;
  END;
")

# Stored Proc 2: storeNewIncident
dbExecute(dbcon, "
  CREATE PROCEDURE storeNewIncident ( IN
    iid varchar(16),
    flightDate DATE,
    airline varchar(2),
    flightNumber INT,
    depAirport varchar(3),
    incidentType varchar(36),
    severity varchar(36),
    delay INT,
    numInjuries INT,
    reportedBy varchar(36),
    aircraft varchar(16)
  )
  BEGIN
    DECLARE alId_val INT;
    DECLARE acId_val INT;
    DECLARE apId_val INT;
    DECLARE itId_val INT;
    DECLARE rId_val INT;
    DECLARE fId_val INT;
    
    DECLARE alName varchar(2);
    DECLARE acName varchar(16);
    DECLARE apName varchar(3);
    
    -- Check if airport, aircraft, and airline parameters exist in DB
    -- If not, insert their values into their corresponding lookup table
    BEGIN
      IF EXISTS (SELECT * FROM Airport  
        WHERE name = depAirport) THEN
          SELECT apId INTO apId_val
          FROM Airport
          WHERE name = depAirport;
      END IF;
    END;
    
    BEGIN
      DECLARE EXIT HANDLER FOR SQLEXCEPTION
      BEGIN
        ROLLBACK;
      END;
      
      START TRANSACTION;
      -- If airport does not exist, insert a new airport into Airport table
      BEGIN
        IF apId_val = NULL THEN
          INSERT INTO Airport (name)
          VALUES (depAirport);
        END IF;
      END;
      
      COMMIT;
    END;
    
    BEGIN
      IF EXISTS (SELECT * FROM Aircraft  
        WHERE name = aircraft) THEN
          SELECT acId INTO acId_val
          FROM Aircraft
          WHERE name = aircraft;
      END IF;
    END;
    
    BEGIN
      DECLARE EXIT HANDLER FOR SQLEXCEPTION
      BEGIN
        ROLLBACK;
      END;
      
      START TRANSACTION;
      -- If aircraft does not exist, insert a new aircraft into Aircraft table
      BEGIN
        IF acId_val = NULL THEN
          INSERT INTO Aircraft (name)
          VALUES (aircraft);
        END IF;
      END;
      
      COMMIT;
    END;
    
    BEGIN
      IF EXISTS (SELECT * FROM Airline 
        WHERE name = airline) THEN
          SELECT apId INTO alId_val
          FROM Airline
          WHERE name = airline;
      END IF;
    END;
    
    BEGIN
      DECLARE EXIT HANDLER FOR SQLEXCEPTION
      BEGIN
        ROLLBACK;
      END;
      
      START TRANSACTION;
      -- If airline does not exist, insert a new airline into Airline table
      BEGIN
        IF alId_val = NULL THEN
          INSERT INTO Airline (name)
          VALUES (airline);
        END IF;
      END;
      
      COMMIT;
    END;
    
    -- Retrieve corresponding Id values from lookup tables
    SELECT alId, name INTO alId_val, alName 
    FROM Airline WHERE name = airline;
    
    SELECT acId, name INTO acId_val, acName
    FROM Aircraft WHERE name = aircraft;
    
    SELECT apId, name INTO apId_val, apName
    FROM Airport WHERE name = depAirport;
    
    SELECT itId INTO itId_val FROM IncidentType WHERE name = incidentType;
    SELECT rId INTO rId_val FROM Reporter WHERE name = reportedBy;
    
    -- Check to see if flight already exists in Flight table
    BEGIN
      IF EXISTS (SELECT * FROM Flight f 
        WHERE f.flightNum = flightNumber AND
              f.flightDate = flightDate AND
              f.delay = delay AND
              alName = airline AND
              acName = aircraft AND
              apName = depAirport
              ) THEN
          SELECT fId INTO fId_val
          FROM Flight f
          WHERE f.flightNum = flightNumber AND
                f.flightDate = flightDate AND
                f.delay = delay AND
                alName = airline AND
                acName = aircraft AND
                apName = depAirport;
      END IF;
    END;
    
    BEGIN
      DECLARE EXIT HANDLER FOR SQLEXCEPTION
      BEGIN
        ROLLBACK;
      END;
      
      START TRANSACTION;
      -- If flight does not exist, insert a new flight into Flight table
      BEGIN
        IF fId_val = NULL THEN
          INSERT INTO Flight (alId, acId, apId, flightNum, flightDate, delay)
          VALUES (alId_val, acId_val, apId_val, flightNumber, flightDate, delay);
        END IF;
      END;
      
      COMMIT;
    END;
    
    BEGIN
      DECLARE EXIT HANDLER FOR SQLEXCEPTION
      BEGIN
        ROLLBACK;
      END;
      
      START TRANSACTION;
      -- Retrieve newly inserted flight Id 
      SELECT fId INTO fId_val FROM Flight f
        WHERE f.flightNum = flightNumber AND
              f.flightDate = date AND
              f.delay = delay AND
              f.alId = alId_val AND
              f.acId = acId_val AND
              f.apId = apId_val;
      
      -- Use retrieved values to insert new incident
      INSERT INTO Incident (iid, fId, itId, rId, severity, injuries)
      VALUES (iid, fId_val, itId_val, rId_val, severity, injuries);
      
      COMMIT;
    END;
  END;
")

# Test Stored Procedures
test_sp_storeIncident <- function() {
  # query parameters
  in_iid <- "i19043"
  in_date <- as.Date("2025-08-17")
  in_al <- "UA"
  in_flightNum <- 9876
  in_ap <- "JFK"
  in_it <- "crew"
  in_severity <- "minor"
  in_delay <- 25
  in_injuries <- 0
  in_reportedBy <- "crew"
  in_ac <- "737-800"
  
  query <- glue::glue_sql("CALL storeIncident(
      {in_iid}, {in_date}, {in_al}, {in_flightNum},
      {in_ap}, {in_it}, {in_severity}, {in_delay},
      {in_injuries}, {in_reportedBy}, {in_ac}
    )", .con=dbcon)

  # CASE: successfully insert a new Incident
  try({
    before_insert <- dbGetQuery(dbcon, "SELECT * FROM Incident")
    dbSendQuery(dbcon, query)
    after_insert <- dbGetQuery(dbcon, "SELECT * FROM Incident")
    
    after_insert[nrow(after_insert),]
    
    if (nrow(before_insert) + 1 == nrow(after_insert)) {
      message("PASS: Incident succesfully inserted")
    }
  }, error = function(e) {
      message("FAIL: Unable to execute stored procedure to completion")
  })
  
  # CASE: fail to insert a duplicate Incident
  try({
    before_insert <- dbGetQuery(dbcon, "SELECT * FROM Flight")
    dbSendQuery(dbcon, query)
    after_insert <- dbGetQuery(dbcon, "SELECT * FROM Flight")
    
    if (nrow(before_insert) == nrow(after_insert)) {
      message("PASS: No duplicate Incident inserted")
    }
  }, error = function(e) {
      message("FAIL: Unable to executed stored procedure to completion")
  })
}

test_sp_storeNewIncident <- function() {
  # query parameters
  in_iid <- "i19044"
  in_date <- as.Date("2025-08-17")
  in_al <- "MM"
  in_flightNum <- 9999
  in_ap <- "MMM"
  in_it <- "crew"
  in_severity <- "minor"
  in_delay <- 25
  in_injuries <- 0
  in_reportedBy <- "crew"
  in_ac <- "M737-900"
  
  query <- glue::glue_sql("CALL storeNewIncident(
      {in_iid}, {in_date}, {in_al}, {in_flightNum},
      {in_ap}, {in_it}, {in_severity}, {in_delay},
      {in_injuries}, {in_reportedBy}, {in_ac}
    )", .con=dbcon)
  
  # CASE: successfully insert a new Incident
  try({
    before_insert <- dbGetQuery(dbcon, "SELECT * FROM Incident")
    dbSendQuery(dbcon, query)
    after_insert <- dbGetQuery(dbcon, "SELECT * FROM Incident")
    
    if (nrow(before_insert) + 1 == nrow(after_insert)) {
      message("PASS: Incident succesfully inserted")
    }
  }, error = function(e) {
     message("FAIL: Unable to executed stored procedure to completion")
  })
  
  # CASE: fail to insert a duplicate Incident
  try({
    before_insert <- dbGetQuery(dbcon, "SELECT * FROM Incident")
    dbSendQuery(dbcon, query)
    after_insert <- dbGetQuery(dbcon, "SELECT * FROM Incident")
    
    if (nrow(before_insert) == nrow(after_insert)) {
      message("PASS: No duplicate Incident inserted")
    }
  }, error = function(e) {
      message("FAIL: Unable to executed stored procedure to completion")
  })
}
