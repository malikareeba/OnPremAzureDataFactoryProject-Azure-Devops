# OnPremAzureDataFactoryProject-Azure-Devops
Airline Data Engineering Pipeline | Azure Data Factory & PySpark
An end-to-end Azure data engineering project that migrates, ingests, transforms and models airline booking data into analytics-ready datasets. The project demonstrates on-prem to cloud migration, API ingestion, dynamic and incremental pipelines, orchestration, Medallion Architecture, and CI/CD with Azure DevOps.
Table of Contents
•	Project Overview
•	Architecture
•	Technologies Used
•	Data Sources
•	Data Model
•	Key Features
•	Pipeline Workflow
•	Project Structure
•	Getting Started
•	Incremental Loading
•	CI/CD and Version Control
•	Learning Outcomes
•	Future Improvements
Project Overview
The objective is to build a scalable pipeline that consolidates airline data from multiple source types (on-premises files, a REST API and Azure SQL Database) into a structured Azure data lake.
The pipeline supports full and incremental loads, dynamic parameterised ingestion, PySpark-based transformations, automated alerting and trigger-based scheduling. The Gold layer exposes business views for analysing bookings, revenue and passenger behaviour.
Architecture
The project follows the Medallion Architecture pattern.
1. Source Layer
•	On-premises files: airline, flight and passenger data (CSV).
•	REST API: airport reference data (JSON).
•	Azure SQL Database: FactBookings transactional data.
2. Bronze Layer: Raw Ingestion
•	Azure Data Factory copies source data into Azure Data Lake Storage as-is.
•	Dynamic pipelines with dynamic mapping handle multiple datasets with one reusable pattern.
•	Incremental loading brings in only new booking records.
3. Silver Layer: Transformation
•	PySpark transformations via ADF Mapping Data Flows.
•	Cleaning, type casting, deduplication and standardisation.
•	Output stored in Delta Lake format.
4. Gold Layer: Business Views
•	Star schema of dimension and fact tables.
•	Business-ready views for reporting and analytics.
Technologies Used
Technology	Purpose
Azure Data Factory	Ingestion, orchestration, mapping data flows, triggers
Azure Data Lake Storage Gen2	Bronze, Silver and Gold storage
Azure SQL Database	Source for transactional booking data
Azure Logic Apps	Pipeline success/failure email notifications
PySpark	Distributed data transformations
Delta Lake	Reliable storage and incremental processing
Azure DevOps	Repos, pull requests, CI/CD workflow
GitHub	Version control and documentation
SQL / Python / JSON / CSV	Source data, scripting and modelling
Data Sources
Source	Format	Ingestion Method	Records
Airlines	CSV (on-prem)	On-prem to Azure migration	10
Flights	CSV (on-prem)	On-prem to Azure migration	10
Passengers	CSV (on-prem)	On-prem to Azure migration	10
Airports	JSON	REST API ingestion	10
FactBookings	Azure SQL Database	Incremental load	1,000
Dataset at a Glance
Metric	Value
Source tables	5
Source types	3 (on-prem files, REST API, Azure SQL DB)
Medallion layers	3 (Bronze, Silver, Gold)
Booking records (FactBookings)	1,000
FactBookings columns	10
Booking date range	1 Jan 2025 to 30 Jun 2025 (6 months)
Airlines	10 across 10 countries/regions
Airports	10 across 10 cities
Flights	10
Passengers	10 in the dimension table
Flight duration range	about 60 to 720 minutes
Gold model	4 dimensions + 1 fact
Key Metrics (Gold Layer)
Run these against your Gold tables and paste the results into this table and your CV.
Metric	Value
Total ticket revenue	<fill in>
Average ticket price	<fill in>
Check-in rate	<fill in> %
Average flight duration (mins)	<fill in>
Top airline by bookings	<fill in>
Busiest route (origin to destination)	<fill in>
Pipeline count / data flows	<fill in>
End-to-end run time	<fill in>
-- Headline metrics
SELECT COUNT(*)                                          AS total_bookings,
       SUM(ticket_cost)                                  AS total_revenue,
       AVG(ticket_cost)                                  AS avg_ticket_price,
       AVG(flight_duration_mins)                         AS avg_duration_mins,
       100.0 * SUM(CASE WHEN checkin_status = 'Yes' THEN 1 ELSE 0 END) / COUNT(*) AS checkin_rate_pct
FROM FactBookings;

-- Revenue and bookings by airline
SELECT a.airline_name, COUNT(*) AS bookings, SUM(f.ticket_cost) AS revenue
FROM FactBookings f
JOIN DimAirline a ON f.airline_id = a.airline_id
GROUP BY a.airline_name
ORDER BY revenue DESC;

-- Monthly booking trend
SELECT FORMAT(booking_date, 'yyyy-MM') AS month, COUNT(*) AS bookings, SUM(ticket_cost) AS revenue
FROM FactBookings
GROUP BY FORMAT(booking_date, 'yyyy-MM')
ORDER BY month;
Data Model
A star schema with four dimension tables and one fact table.
Table	Description
DimAirline	Airline ID, name and country
DimAirport	Airport ID, name, city and country
DimFlight	Flight ID, flight number, departure and arrival times
DimPassenger	Passenger ID, name, gender, age and country
FactBookings	Booking ID, passenger, flight, airline, origin and destination airports, booking date, ticket cost, flight duration (mins), check-in status
Key Features
On-Prem to Azure Migration
•	Copies on-premises files into Azure using ADF (self-hosted integration runtime).
•	Lands raw data in the Bronze layer.
Dynamic ADF Pipelines
•	Parameterised datasets and linked services.
•	Dynamic column mapping so one pipeline serves multiple tables.
•	ForEach looping over a metadata/config list.
API Ingestion
•	Pulls airport reference data from a REST API and lands the JSON in the lake.
Incremental Data Loading
•	Watermark-based incremental load for FactBookings from Azure SQL Database.
•	Avoids full reloads and duplicate records.
Orchestration and Alerting
•	Pipeline chaining with control-flow activities (dependencies, conditions).
•	Azure Logic Apps send email notifications on success or failure.
PySpark Transformations
•	ADF Mapping Data Flows for cleaning, joins and derived columns.
•	Output written as Delta tables.
Gold Layer Business Views
•	Aggregated views for revenue by airline, bookings by route, average flight duration and check-in rate.
Triggers
•	Schedule and event-based triggers to automate pipeline runs.
CI/CD
•	ADF integrated with GitHub and Azure DevOps.
•	Feature branch and pull request workflow for safe deployment.
Pipeline Workflow
1.	Migrate on-premises airline, flight and passenger files to the Bronze layer.
2.	Ingest airport data from the REST API.
3.	Set up Azure SQL Database and load FactBookings.
4.	Run the initial load, then incremental loads using a watermark.
5.	Orchestrate all ingestion pipelines in a master pipeline.
6.	Transform Bronze data into Silver using PySpark Mapping Data Flows.
7.	Build Gold dimension and fact tables and business views.
8.	Attach triggers to automate runs and Logic Apps for alerts.
9.	Commit changes via feature branches and merge through pull requests.
Project Structure
Suggested structure (adjust to match your repository):
airline-data-engineering/
│
├── adf/
│   ├── pipeline/
│   ├── dataset/
│   ├── linkedService/
│   ├── dataflow/
│   └── trigger/
│
├── sql/
│   ├── create_factbookings.sql
│   └── insert_factbookings.sql
│
├── data/
│   ├── airlines.csv
│   ├── flights.csv
│   ├── passengers.csv
│   └── airports.json
│
├── notebooks/
├── docs/
│   └── architecture.png
│
├── README.md
└── .gitignore
Getting Started
Prerequisites
•	An Azure account (free tier is sufficient).
•	Azure Data Factory, ADLS Gen2 and Azure SQL Database resources.
•	Azure Logic Apps for notifications.
•	A GitHub account and an Azure DevOps organisation.
Setup
1.	Clone the repository: git clone https://github.com/YOUR-USERNAME/YOUR-REPOSITORY.git
2.	Create the Azure resources (resource group, storage account, SQL Database, Data Factory).
3.	Run the SQL scripts in sql/ to create and populate FactBookings.
4.	Configure linked services for the on-prem source, API, ADLS and Azure SQL.
5.	Import the pipelines, datasets and data flows from adf/.
6.	Run the initial load, then the incremental pipeline.
7.	Validate output in the Silver and Gold layers.
Note: Never commit credentials or secrets. Use Azure Key Vault or ADF-managed connections.
Incremental Loading
•	Initial load: loads the full FactBookings table.
•	Incremental load: uses a watermark (e.g. booking_date or booking_id) to pull only new records, then updates the watermark after a successful run.
Validate that repeated runs do not create duplicates.
CI/CD and Version Control
•	ADF connected to GitHub for source control.
•	Azure DevOps used for repos and the pull request workflow.
•	Development happens on feature branches, merged into main via pull requests, then published to the live ADF instance.
Learning Outcomes
•	Migrating on-premises data to Azure using Azure Data Factory.
•	Building dynamic, parameterised pipelines with dynamic mapping.
•	Ingesting data from REST APIs.
•	Implementing incremental loading with watermarks.
•	Orchestrating pipelines with triggers and Logic Apps alerts.
•	Transforming data with PySpark Mapping Data Flows and Delta Lake.
•	Applying Medallion Architecture and star-schema modelling.
•	Using Git integration, feature branches and pull requests in a data engineering workflow.
Future Improvements
•	Add automated data quality checks and validation reports.
•	Add referential integrity checks between FactBookings and the dimension tables.
•	Add pipeline monitoring dashboards (Azure Monitor / Log Analytics).
•	Automate CI/CD deployment with Azure DevOps release pipelines.
•	Connect Power BI to the Gold layer for reporting.
•	Document actual row counts and run times for each layer.
