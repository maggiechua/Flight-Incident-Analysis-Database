# Flight Incident Analysis Database
3NF-compliant relational database system for analyzing flight incident patterns across a decade of aviation data. Features automated data processing pipeline, performance-optimized storage, and comprehensive statistical analysis using R and SQL.

## Features
- **3NF-compliant database design** with optimized table relationships and indexing
- **Automated data cleaning and validation** pipeline handling 18,000+ flight incident records
- **Performance-optimized bulk insertion** reducing load times to under 2 minutes
- **Comprehensive data analysis** with statistical insights and visualizations
- **SQL stored procedures** with error handling and data integrity safeguards
- **Interactive R Markdown reports** documenting analysis methodology and findings 

## Technologies Used
- R (data processing and analysis)
- R Studio (development environment)
- SQL (database queries and stored procedures)
- MySQL (database engine via Aiven)
- Aiven (cloud database hosting)
- R Markdown (analysis documentation and reporting)

## Limitations
- Cloud database instance no longer active (discontinued to avoid ongoing costs)
- Dataset limited to synthetic flight incident data over 10-year period
- Designed for analytical workloads rather than real-time transaction processing
- R-based data processing pipeline may not scale efficiently for datasets beyond 100k+ records
- Does not account for multi-user design and concurrent uploads to the database

## Known Issues
- Stored procedures compile successfully but do not properly execute insert/update/delete operations
- Data validation logic in stored procedures needs refinement for edge cases

## How to Run
1. Clone the repository:
```
git clone https://github.com/maggiechua/Flight-Incident-Analysis-Database.git
```
2. Database setup:
- Setup a MySQL database (local or cloud service like Aiven, AWS RDS, etc.)
- Open `createDB.R` and populate the database connection variables (lines 11-15)
  - `db_host`: Your database host address
  - `db_port`: Database port
  - `db_name`: Your database name
  - `db_user`: Database username
  - `db_password`: Database password 
- Add your database connection string by setting it as the value of `db_cert` in `createDB.R` (line 18)
- Run `createDB.R` to initialize tables and schema
3. Load and analyze data:
- Execute `loadDB.R` to import and clean the dataset
- Navigate to `IncidentReport.Rmd` and run code chunks for analysis

## How to Test
The testing workflow is embedded within the R scripts and can be executed in sequence:

**Testing Scripts:**
- `createDB.R`: Creates secure database connection and initializes tables
- `loadDB.R`: Cleans and validates dataset, loads 500-record batches
- `testDBLoading.R`: Validates data consistency and loading accuracy
- `configBusinessLogic.R`: Contains stored procedures with validation tests

_Note: Scripts must be executed in logical sequence with active database connection_

## Database Structure
This database follows Third Normal Form (3NF) principles to eliminate data redundancy and ensure referential integrity.

**Key Tables:**
- **Incident:** primary incident records with unique identifiers
- **Flight:** flight records with unique identifiers, including aircraft, airline, and airport information
- **Airline:** lookup table for airline name by unique identifier
- **Airport:** lookup table for airport name by unique identifier
- **Aircraft:** lookup table for aircraft name by unique identifier
- **Reporter:** lookup table for reporter type by unique identifier
- **IncidentType:** lookup table for incident type by unique identifier

**Design Features:**
- Foreign key relationships maintain data consistency
- Lookup tables for categorical data (aircraft/airline types, airports, incident categories)
- Optimized indexing on frequently queried columns
- Normalized structure eliminates duplicate data storage

**ERD:**
<img width="1284" height="722" alt="image" src="https://github.com/user-attachments/assets/5535ca76-f46a-4239-bfb5-4a1c1616b00b" />

## Analysis Workflow 
**Data Processing:**
- Data import and processing pipeline with validation and null handling
- Standardized categorical variables using lookup table mappings
- Quality assurance checks for data completeness and accuracy

**Statistical Analysis:**
- Incident frequency analysis by airline and average delay
- Incident frequency analysis by month and incident type
- Trend analysis identifying patterns in aviation safety over the decade

**Reporting:**
- Interactive R Markdown notebooks with embedded visualizations
- SQL queries for analysis and data exploration
- Exportable summary statistics and key findings
