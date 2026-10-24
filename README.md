# Airline Data Engineering Pipeline | Azure Data Factory & PySpark

An end-to-end Azure Data Engineering project demonstrating on-premises data migration, REST API ingestion, incremental data loading, data transformation, Medallion Architecture, dimensional modelling, and CI/CD workflows using Azure Data Factory and Azure DevOps.

The pipeline integrates airline, flight, passenger, airport, and booking data from multiple sources into Azure Data Lake Storage Gen2, transforming raw data into analytics-ready datasets for business reporting.

## Table of Contents

- Project Overview
- Architecture
- Technologies Used
- Data Sources
- Medallion Architecture
- Data Model
- Key Features
- Pipeline Workflow
- Project Structure
- Getting Started
- Incremental Loading
- Business Metrics
- CI/CD and Version Control
- Learning Outcomes
- Future Improvements
- Security Considerations
- Author

## Project Overview

The objective of this project is to build a scalable, automated data pipeline that consolidates airline booking data from on-premises files, a REST API, and Azure SQL Database into Azure Data Lake Storage Gen2.

Azure Data Factory manages data ingestion and orchestration, while transformation processes prepare the data for downstream analytics. The Gold layer supports business analysis of ticket revenue, booking trends, airline performance, flight routes, and passenger behaviour.

The project demonstrates the following data engineering concepts:

- On-premises to cloud data migration.
- Metadata-driven and parameterised ingestion pipelines.
- REST API integration.
- Full and incremental data loading.
- Watermark-based change tracking.
- Medallion Architecture: Bronze, Silver, and Gold.
- Data cleansing, standardisation, and deduplication.
- Star-schema dimensional modelling.
- Pipeline orchestration, scheduling, and notifications.
- Git-based collaboration and CI/CD practices.

## Architecture

The solution follows the Medallion Architecture pattern, separating raw ingestion, data transformation, and business-ready datasets.

### High-Level Architecture

```
                      DATA SOURCES
                           |
          +----------------+----------------+
          |                |                |
    On-Premises Files   REST API       Azure SQL DB
        CSV Files        JSON            Bookings
          |                |                |
          +----------------+----------------+
                           |
                  Azure Data Factory
               Ingestion & Orchestration
                           |
                           v
                  BRONZE LAYER
               Raw Data Ingestion
              Azure Data Lake Gen2
                           |
                           v
                  SILVER LAYER
             Cleansing & Transformation
                PySpark / Data Flows
                  Delta Lake Format
                           |
                           v
                   GOLD LAYER
              Fact & Dimension Tables
                 Business Views
                           |
                           v
                Analytics & Reporting

             Supporting Services
        Azure Logic Apps | Key Vault
        Triggers | Azure DevOps | GitHub
```

### Architecture Components

1. **Source Layer:** On-premises CSV files, REST API JSON data, and transactional booking records from Azure SQL Database.
2. **Bronze Layer:** Raw source data ingested into Azure Data Lake Storage Gen2.
3. **Silver Layer:** Cleansed, standardised, deduplicated, and transformed datasets.
4. **Gold Layer:** Analytics-ready fact and dimension tables supporting business reporting.
5. **Orchestration Layer:** Azure Data Factory pipelines, triggers, and control-flow activities.
6. **Monitoring and Notifications:** Azure Logic Apps for pipeline success and failure alerts.
7. **Development and Deployment:** GitHub, Azure DevOps, feature branches, pull requests, and CI/CD workflows.

## Technologies Used

| Technology                            | Purpose                                                                 |
| ------------------------------------- | ----------------------------------------------------------------------- |
| Azure Data Factory                    | Data ingestion, pipeline orchestration, scheduling, and data movement   |
| Azure Data Lake Storage Gen2          | Centralised storage for Bronze, Silver, and Gold datasets               |
| Azure SQL Database                    | Source system for transactional booking data                            |
| Azure Data Factory Mapping Data Flows | Data cleansing, transformations, joins, and derived columns             |
| PySpark                               | Distributed data processing and transformation logic, where implemented |
| Delta Lake                            | Reliable analytical storage and incremental processing                  |
| Self-hosted Integration Runtime       | Connectivity to on-premises data sources                                |
| Azure Integration Runtime             | Cloud-based data movement and connectivity                              |
| Azure Logic Apps                      | Pipeline success and failure notifications                              |
| Azure Key Vault                       | Secure storage of secrets and credentials                               |
| Azure DevOps                          | Repository management and CI/CD workflows                               |
| GitHub and Git                        | Source control, version history, and collaboration                      |
| SQL, Python, JSON, CSV                | Data ingestion, transformation, and modelling                           |

## Data Sources

The pipeline integrates five logical datasets from three source types.

| Dataset      | Format    | Source             | Ingestion Method             | Expected Records |
| ------------ | --------- | ------------------ | ---------------------------- | ---------------- |
| Airlines     | CSV       | On-premises        | ADF file ingestion           | 10               |
| Flights      | CSV       | On-premises        | ADF file ingestion           | 10               |
| Passengers   | CSV       | On-premises        | ADF file ingestion           | 10               |
| Airports     | JSON      | REST API           | ADF API ingestion            | 10               |
| FactBookings | SQL table | Azure SQL Database | Full and incremental loading | 1,000            |

### Dataset at a Glance

| Metric                   | Value                         |
| ------------------------ | ----------------------------- |
| Source datasets          | 5                             |
| Source types             | 3                             |
| Medallion layers         | 3                             |
| Expected booking records | 1,000                         |
| Booking date range       | 1 January 2025 – 30 June 2025 |
| Airlines                 | 10                            |
| Airports                 | 10                            |
| Flights                  | 10                            |
| Passengers               | 10                            |
| Gold data model          | 4 dimensions and 1 fact table |

*Record counts represent the intended sample dataset and should be updated if the source data changes.*

## Medallion Architecture

### 1. Bronze Layer — Raw Data Ingestion

The Bronze layer stores ingested source data in its original or minimally processed form.

Key activities:

- Migrates on-premises CSV files into Azure Data Lake Storage Gen2.
- Ingests airport reference data from a REST API.
- Copies transactional booking data from Azure SQL Database.
- Uses reusable, parameterised ADF pipelines to process multiple datasets.
- Preserves raw data for traceability and potential reprocessing.

### 2. Silver Layer — Data Cleansing and Transformation

The Silver layer prepares source data for downstream analytics.

Key activities:

- Handles null values and inconsistent records.
- Applies appropriate data types.
- Standardises column names and data formats.
- Removes duplicate records where appropriate.
- Applies business rules and joins related datasets.
- Stores transformed outputs in Delta Lake format, where configured.

### 3. Gold Layer — Analytics-Ready Data

The Gold layer contains structured datasets designed for reporting and business analysis.

Key activities:

- Builds a star-schema data model.
- Organises data into dimension and fact tables.
- Supports revenue and booking analysis.
- Enables airline and route performance comparisons.
- Provides datasets for Power BI and other reporting tools.

## Data Model

The analytical model consists of four dimension tables and one fact table.

| Table        | Description                                                                                              |
| ------------ | -------------------------------------------------------------------------------------------------------- |
| DimAirline   | Airline identifiers, names, and countries                                                                |
| DimAirport   | Airport identifiers, names, cities, and countries                                                        |
| DimFlight    | Flight identifiers, flight numbers, departure times, and arrival times                                   |
| DimPassenger | Passenger identifiers, names, gender, age, and country                                                   |
| FactBookings | Booking transactions, dimension keys, booking dates, ticket costs, flight durations, and check-in status |

### FactBookings — Key Attributes

- Booking ID
- Passenger ID
- Flight ID
- Airline ID
- Origin Airport ID
- Destination Airport ID
- Booking Date
- Ticket Cost
- Flight Duration in Minutes
- Check-in Status

The fact table connects booking transactions to the relevant dimensions, enabling analysis across airlines, airports, passengers, and flights.

## Key Features

### On-Premises to Azure Migration

- Uses Azure Data Factory to transfer local source files into cloud storage.
- Uses a self-hosted Integration Runtime when connectivity to an on-premises environment is required.
- Lands raw data in the Bronze layer.

### Dynamic and Parameterised Pipelines

- Uses pipeline parameters and reusable datasets.
- Supports metadata-driven ingestion patterns.
- Uses ForEach activities to process multiple datasets.
- Applies dynamic column mapping where source and target schemas permit.

### REST API Ingestion

- Retrieves airport reference data from a REST API endpoint.
- Ingests JSON data into the data lake.
- Demonstrates integration with non-relational data sources.

### Incremental Data Loading

- Performs an initial full load of booking records.
- Uses a watermark to identify records eligible for subsequent ingestion.
- Reduces unnecessary source reads and repeated data movement.
- Supports duplicate prevention through appropriate target-side logic.

### Pipeline Orchestration and Notifications

- Coordinates ingestion and transformation pipelines through a master pipeline.
- Uses activity dependencies and conditional execution.
- Supports scheduled and event-based triggers where configured.
- Integrates Azure Logic Apps for success and failure notifications.

### Data Transformation and Modelling

- Cleans and standardises source datasets.
- Applies transformations, joins, and derived columns.
- Produces Silver and Gold datasets.
- Structures analytical data using a star schema.

### CI/CD and Version Control

- Maintains version-controlled pipeline definitions and project scripts.
- Supports feature branches and pull requests.
- Separates development changes from deployment to the live environment.
- Provides a foundation for automated validation and deployment through Azure DevOps.

## Pipeline Workflow

The end-to-end pipeline follows these steps:

1. **Prepare Source Data:** Create the sample CSV files and configure the booking source in Azure SQL Database.
2. **Configure Connectivity:** Set up the required linked services and Integration Runtime.
3. **Ingest On-Premises Files:** Copy airline, flight, and passenger data into the Bronze layer.
4. **Ingest API Data:** Retrieve airport reference data and store the JSON response in the data lake.
5. **Load Booking Transactions:** Perform the initial full load from Azure SQL Database.
6. **Implement Incremental Loading:** Use a watermark to identify new or changed source records according to the implemented logic.
7. **Transform the Data:** Clean, standardise, deduplicate, and join datasets in the Silver layer.
8. **Build the Gold Model:** Populate dimension and fact tables and prepare business-ready datasets.
9. **Orchestrate Execution:** Coordinate pipelines, configure triggers, and integrate success or failure notifications.
10. **Validate the Outputs:** Check row counts, data quality, relationships, and analytical query results.
11. **Manage Changes:** Commit changes to version control and use pull requests and deployment workflows.

## Project Structure

The following is a suggested repository structure. Adjust directory names to match the actual contents of your repository.

```
OnPremAzureDataFactoryProject-Azure-Devops/
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
│   ├── insert_factbookings.sql
│   └── gold_queries.sql
│
├── data/
│   ├── airlines.csv
│   ├── flights.csv
│   ├── passengers.csv
│   └── airports.json
│
├── notebooks/
│
├── docs/
│   └── architecture.png
│
├── README.md
└── .gitignore
```

## Getting Started

### Prerequisites

Before running the project, ensure you have:

- An Azure subscription with permission to create and configure the required resources.
- Azure Data Factory.
- Azure Data Lake Storage Gen2.
- Azure SQL Database.
- A self-hosted Integration Runtime and access to a source machine, if migrating from a local environment.
- Azure Logic Apps, if implementing email notifications.
- Azure DevOps and/or GitHub for source control.
- The sample datasets and SQL scripts required by the project.

Some Azure services incur charges. Review pricing and resource configurations before deployment.

### Step 1 — Clone the Repository

Replace the placeholders with your actual GitHub username and repository name.

```
git clone https://github.com/YOUR-USERNAME/YOUR-REPOSITORY.git
cd OnPremAzureDataFactoryProject-Azure-Devops
```

### Step 2 — Create Azure Resources

Create and configure the resources required for the pipeline:

- Azure Resource Group
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure SQL Database
- Integration Runtime
- Azure Logic App, if required

Configure the necessary permissions and network connectivity between the services.

### Step 3 — Prepare the Source Data

Place the sample CSV and JSON files in their appropriate source locations.

If you are using an on-premises source, configure the self-hosted Integration Runtime and verify that Azure Data Factory can access the source files.

### Step 4 — Set Up Azure SQL Database

Run the SQL scripts in the `sql/` directory to create and populate the booking table.

Validate the source records using:

```
SELECT COUNT(*) AS total_bookings
FROM dbo.FactBookings;
```

Confirm that the source table contains the expected records before running the pipeline.

### Step 5 — Configure ADF Connectivity

Configure the linked services and datasets for:

- Source files
- REST API
- Azure SQL Database
- Azure Data Lake Storage Gen2

Use managed identities where supported and appropriate. Store required secrets in Azure Key Vault rather than hardcoding them in pipeline definitions.

### Step 6 — Configure and Run the Pipelines

Import or recreate the required pipelines, datasets, mapping data flows, and triggers.

Run the ingestion pipelines and verify that the expected data lands in the Bronze layer.

### Step 7 — Run Transformations

Execute the transformation pipelines to populate the Silver and Gold layers.

Validate schema consistency, record counts, data types, duplicate handling, and relationships between the fact and dimension tables.

### Step 8 — Validate the Results

Run analytical queries against the appropriate Gold tables or views.

Record actual row counts, pipeline execution times, and business metrics to document the results of your implementation.

## Incremental Loading

The booking ingestion process supports an initial full load followed by incremental loads.

### Initial Load

The initial run loads the required historical booking records from Azure SQL Database into the target data lake or processing layer.

### Incremental Load

A watermark identifies the boundary between previously processed data and records eligible for the next load.

A typical process is:

1. Read the last successfully stored watermark.
2. Retrieve the current upper watermark from the source.
3. Extract eligible records between the lower and upper boundaries.
4. Write the extracted records to the target.
5. Validate the load and handle duplicates or updates according to the target design.
6. Update the stored watermark only after the load succeeds.

### Important Considerations

- Use a reliable watermark column, such as a monotonically increasing booking ID or a source-maintained modification timestamp.
- A booking date watermark may miss late-arriving records or changes to existing records.
- Use consistent boundary conditions to avoid missing records between runs.
- Use a staging and MERGE strategy, or an equivalent idempotent write pattern, when updates and duplicate prevention are required.
- Ensure failed runs do not advance the watermark prematurely.
- Test repeated runs to confirm that the pipeline behaves as expected.

The exact behaviour depends on the watermark column, extraction query, and target write strategy implemented in Azure Data Factory.

## Business Metrics

The Gold layer is designed to support analytical metrics such as:

| Metric                  | Description                                                |
| ----------------------- | ---------------------------------------------------------- |
| Total Bookings          | Number of booking transactions                             |
| Total Ticket Revenue    | Sum of ticket costs                                        |
| Average Ticket Price    | Average ticket cost per booking                            |
| Check-in Rate           | Percentage of bookings with a successful check-in status   |
| Average Flight Duration | Average flight duration in minutes                         |
| Top Airline by Bookings | Airline with the highest number of bookings                |
| Busiest Route           | Origin-to-destination route with the highest booking count |
| Monthly Booking Trend   | Monthly booking volume and revenue                         |
| Pipeline Execution Time | Duration of pipeline runs                                  |

### Example SQL Queries

The following examples assume that `FactBookings` exposes the required booking measures and attributes. Adjust the table names and join logic to match your actual schema.

### 1. Headline Metrics

```
SELECT
    COUNT(*) AS total_bookings,
    SUM(ticket_cost) AS total_revenue,
    AVG(CAST(ticket_cost AS DECIMAL(18, 2)))
        AS avg_ticket_price,
    AVG(CAST(flight_duration_mins AS DECIMAL(18, 2)))
        AS avg_duration_mins,
    100.0 * SUM(
        CASE
            WHEN checkin_status = 'Yes' THEN 1
            ELSE 0
        END
    ) / NULLIF(COUNT(*), 0) AS checkin_rate_pct
FROM dbo.FactBookings;
```

### 2. Bookings and Revenue by Airline

```
SELECT
    a.airline_name,
    COUNT(*) AS bookings,
    SUM(f.ticket_cost) AS revenue
FROM dbo.FactBookings AS f
JOIN dbo.DimAirline AS a
    ON f.airline_id = a.airline_id
GROUP BY a.airline_name
ORDER BY revenue DESC;
```

### 3. Monthly Booking Trend

```
SELECT
    CONVERT(CHAR(7), booking_date, 120) AS booking_month,
    COUNT(*) AS bookings,
    SUM(ticket_cost) AS revenue
FROM dbo.FactBookings
GROUP BY CONVERT(CHAR(7), booking_date, 120)
ORDER BY booking_month;
```

### 4. Busiest Routes

```
SELECT TOP 10
    origin_airport_id,
    destination_airport_id,
    COUNT(*) AS total_bookings
FROM dbo.FactBookings
GROUP BY
    origin_airport_id,
    destination_airport_id
ORDER BY total_bookings DESC;
```

### Project Results

Populate this table with actual results after running the pipeline and analytical queries.

| Metric                             | Actual Result  |
| ---------------------------------- | -------------- |
| Total Ticket Revenue               | To be measured |
| Average Ticket Price               | To be measured |
| Check-in Rate                      | To be measured |
| Average Flight Duration            | To be measured |
| Top Airline by Bookings            | To be measured |
| Busiest Route                      | To be measured |
| Number of Pipelines and Data Flows | To be verified |
| End-to-End Pipeline Runtime        | To be measured |

*Do not report estimated results as actual pipeline outcomes.*

## CI/CD and Version Control

The project uses Git-based version control to support collaboration, change tracking, and controlled deployment.

### Development Workflow

1. Develop changes in a feature branch.
2. Commit and push changes to the configured repository.
3. Open a pull request for review.
4. Validate changes before merging into the main branch.
5. Publish or deploy approved ADF artifacts to the target environment.
6. Verify the deployed pipelines, datasets, and linked services.

Azure Data Factory Git integration and Azure DevOps deployment pipelines can be used to implement this workflow.

### Repository Configuration

GitHub and Azure DevOps are separate platforms. If both are used, document which repository is the source of truth and how changes are synchronised.

ADF Git integration, branch policies, and automated release pipelines must be configured explicitly. They are not enabled automatically by storing files in a repository.

## Learning Outcomes

This project demonstrates practical experience with:

- Migrating on-premises data into Azure.
- Building cloud-based data ingestion pipelines.
- Connecting to REST APIs and relational databases.
- Developing reusable and parameterised ADF pipelines.
- Implementing watermark-based incremental ingestion.
- Applying data quality, cleansing, and deduplication techniques.
- Working with the Bronze, Silver, and Gold Medallion Architecture.
- Designing star-schema analytical models.
- Orchestrating data workflows and handling pipeline failures.
- Configuring pipeline scheduling and notifications.
- Using Git, pull requests, and CI/CD concepts in data engineering.

## Future Improvements

Potential enhancements include:

- Implement automated data quality checks and validation reports.
- Add referential integrity checks between fact and dimension tables.
- Implement robust upserts and late-arriving data handling.
- Add retry policies and more detailed error logging.
- Integrate Azure Monitor and Log Analytics for pipeline observability.
- Automate deployments through Azure DevOps YAML pipelines.
- Introduce automated tests for transformation logic and SQL models.
- Connect Power BI to the Gold layer.
- Document actual execution times, row counts, and cost considerations.
- Add an architecture diagram and sample pipeline screenshots.

## Security Considerations

- Never commit passwords, access keys, tokens, connection strings, or other secrets.
- Use Azure Key Vault and managed identities where appropriate.
- Apply least-privilege access to storage, databases, and pipeline resources.
- Keep environment-specific configuration separate from pipeline logic.
- Exclude local configuration files and sensitive exports from version control.

## Author

**Your Name**

- GitHub: [YOUR-GITHUB-USERNAME](https://github.com/YOUR-GITHUB-USERNAME)
- LinkedIn: [YOUR-LINKEDIN-PROFILE](https://www.linkedin.com/)

---

*This project is intended for learning and demonstration. Update the architecture, implementation details, repository structure, and performance metrics to reflect the resources, pipelines, and results actually deployed in your environment.*

Before publishing: Replace the GitHub and author placeholders, verify the directory structure against your actual repository, and update the metrics once you have run your queries. Also, make sure the README describes PySpark and ADF Mapping Data Flows accurately for the implementation you have built.
