## Part C: Realize Database
## Maggie Chua
## Summer 2 2025

# Connecting to DB
library(RMySQL)
library(DBI)
library(glue)

## define settings
db_host <- ""
db_port <- ""
db_name <- ""
db_user <- ""
db_pwd <- ""

## embedded SSL certificate
db_cert <- ""

## connect securely to remote MySQL server and database
dbcon <- dbConnect(RMySQL::MySQL(), 
                        user = db_user, 
                        password = db_pwd,
                        dbname = db_name, 
                        host = db_host, 
                        port = db_port,
                        sslmode = "require",
                        sslcert = db_cert)

# Realize DB
## Create DB Tables
dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Airline (
  alId INTEGER NOT NULL AUTO_INCREMENT,
  name varchar(2) NOT NULL,
  PRIMARY KEY (alId)
)
")

dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Aircraft (
  acId INTEGER NOT NULL AUTO_INCREMENT,
  name varchar(16) NOT NULL,
  PRIMARY KEY (acId)
)
")

dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Airport (
  apId INTEGER NOT NULL AUTO_INCREMENT,
  name varchar(3) NOT NULL,
  PRIMARY KEY (apId)
)
")

dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Reporter (
  rId INTEGER NOT NULL AUTO_INCREMENT,
  name varchar(16) NOT NULL,
  PRIMARY KEY (rId)
)
")

dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS IncidentType (
  itId INTEGER NOT NULL AUTO_INCREMENT,
  name varchar(16) NOT NULL,
  PRIMARY KEY (itId)
)
")

dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Flight (
  fId INTEGER NOT NULL AUTO_INCREMENT,
  alId INTEGER NOT NULL,
  acId INTEGER NOT NULL,
  apId INTEGER NOT NULL,
  flightNum INTEGER NOT NULL,
  flightDate DATE NOT NULL,
  delay INTEGER NOT NULL,
  PRIMARY KEY (fId),
  FOREIGN KEY (alId) REFERENCES Airline(alId),
  FOREIGN KEY (acId) REFERENCES Aircraft(acId)
)
")

dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Incident (
  inId INTEGER NOT NULL AUTO_INCREMENT,
  iid VARCHAR(16) NOT NULL,
  fId INTEGER NOT NULL,
  itId INTEGER NOT NULL,
  rId INTEGER NOT NULL,
  severity varchar(16) NOT NULL,
    CHECK (severity IN ('minor', 'major', 'critical')),
  injuries INTEGER NOT NULL,
    CHECK (injuries IN (0, 1, NULL)),
  PRIMARY KEY (inId),
  FOREIGN KEY (fId) REFERENCES Flight(fId),
  FOREIGN KEY (itId) REFERENCES IncidentType(itId),
  FOREIGN KEY (rId) REFERENCES Reporter(rId)
)
")

## Set auto-increments to ensure data integrity and prevent collisions
dbExecute(dbcon, "
  ALTER TABLE IncidentType AUTO_INCREMENT=100
")

dbExecute(dbcon, "
  ALTER TABLE Airline AUTO_INCREMENT=200
")

dbExecute(dbcon, "
  ALTER TABLE Reporter AUTO_INCREMENT=300
")

dbExecute(dbcon, "
  ALTER TABLE Aircraft AUTO_INCREMENT=400
")

dbExecute(dbcon, "
  ALTER TABLE Airport AUTO_INCREMENT=500
")

dbExecute(dbcon, "
  ALTER TABLE Incident AUTO_INCREMENT=1000
")

dbExecute(dbcon, "
  ALTER TABLE Flight AUTO_INCREMENT=50000
")

dbListTables(dbcon)

