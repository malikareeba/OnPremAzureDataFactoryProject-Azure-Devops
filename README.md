# On-Premises to Azure Data Engineering Project | Azure Data Factory & Azure DevOps

An end-to-end data engineering project that migrates, ingests, transforms, and models airline booking data using Azure Data Factory and Azure cloud services.

The project demonstrates on-premises data migration, REST API ingestion, dynamic and parameterised pipelines, incremental data loading, Medallion Architecture, data orchestration, business data serving, and Git-based collaboration.

The entire data pipeline is built around **Azure Data Factory**, using Mapping Data Flows to transform data from the Bronze Layer to the Silver Layer and to prepare business-ready data in the Gold Layer.

## Table of Contents

- Project Overview
- Project Objectives
- Architecture
- Project Screenshots
- Technologies Used
- Data Sources
- Medallion Architecture
- Data Model
- Key Features
- End-to-End Pipeline Workflow
- Project Structure
- Getting Started
- Incremental Data Loading
- Business Metrics and SQL Queries
- Git Integration and CI/CD Workflow
- Challenges and Learning Outcomes
- Future Improvements
- Security Considerations
- Author

## Project Overview

The objective of this project was to build an end-to-end Azure data engineering solution that consolidates airline data from multiple sources into a centralised Azure data lake.

The pipeline integrates on-premises CSV files, airport reference data from a REST API, and transactional booking data from Azure SQL Database. Azure Data Factory orchestrates the ingestion, transformation, and serving processes to produce analytics-ready datasets.

The project follows the **Medallion Architecture**:

**Bronze → Silver → Gold**

A key design decision was to use Azure Data Factory Mapping Data Flows for data transformation and serving instead of relying on Databricks as a separate transformation platform.

This project brings together data ingestion, transformation, orchestration, incremental processing, and source control in a single end-to-end workflow.

## Project Objectives

- Migrate on-premises data into Azure.
- Ingest reference data from a REST API.
- Configure Azure SQL Database as a transactional data source.
- Build reusable and dynamic Azure Data Factory pipelines.
- Implement incremental loading using watermark logic.
- Transform data through Bronze, Silver, and Gold layers.
- Build separate Data Flow layers for transformation and data serving.
- Orchestrate pipeline execution using Azure Data Factory.
- Integrate Azure Logic Apps for pipeline notifications.
- Configure triggers for automated execution.
- Use Git integration, feature branches, and pull requests.
- Apply version control and deployment workflow concepts using GitHub and Azure DevOps.

## Architecture

The project uses Azure Data Factory as the central orchestration and data integration service.

### High-Level Architecture

```
                     DATA SOURCES
                          |
          +---------------+---------------+
          |               |               |
     On-Prem Files     REST API      Azure SQL DB
       CSV Files        JSON          FactBookings
          |               |               |
          +---------------+---------------+
                          |
                          v
                 AZURE DATA FACTORY
              Ingestion & Orchestration
                          |
                          v
                   BRONZE LAYER
                Raw Data in ADLS Gen2
                          |
                          v
                   SILVER LAYER
              ADF Mapping Data Flows
             Data Transformation Layer
                          |
                          v
                    GOLD LAYER
              ADF Mapping Data Flows
                Data Serving Layer
                          |
                          v
               Business-Ready Datasets
                Reporting & Analytics

             Supporting Components
          Azure Logic Apps | Triggers
          GitHub | Azure DevOps | Git
```

### Architecture Components

1. **Source Systems:** On-premises CSV files, REST API JSON data, and booking transactions stored in Azure SQL Database.
2. **Azure Data Factory:** Manages data movement, pipeline orchestration, parameters, dependencies, and execution.
3. **Bronze Layer:** Stores ingested source data in Azure Data Lake Storage Gen2.
4. **Silver Layer:** Uses Azure Data Factory Mapping Data Flows to transform and prepare Bronze data.
5. **Gold Layer:** Uses a separate Data Flow layer to prepare business-ready datasets for reporting and analytics.
6. **Azure Logic Apps:** Supports pipeline success and failure notifications.
7. **Triggers:** Automates pipeline execution according to the configured trigger conditions.
8. **Git Integration:** Supports source control, feature branches, pull requests, and deployment workflows.

## Project Screenshots

The screenshots below show the Azure resources, Data Factory pipelines and Mapping Data Flows, storage output, and Logic Apps notification workflow used in this project.

### 1. Pipeline Notification Email

![Email notification showing the pipeline run details](<images/DAY 65.1.jpg>)

### 2. Parent Pipeline Execution

![Azure Data Factory parent pipeline execution and activity status](<images/DAY 65.2.jpg>)

### 3. Logic Apps Email Notification Workflow

![Azure Logic Apps HTTP request trigger and Send email action](<images/day 65.3.jpg>)

### 4. Data Flow Preview

![Azure Data Factory Mapping Data Flow preview showing airport records](<images/day 65.4.jpg>)

### 5. Silver Layer Storage Output

![Silver container in Azure Data Lake Storage with dimension and fact folders](<images/day 65.5.jpg>)

### 6. Azure Project Resources

![Azure resource group resources including Data Factory, SQL, Logic App, and storage](<images/day 65.6.jpg>)

### 7. Data Transformation Flow

![Mapping Data Flow with joins, aggregations, filtering, and sink output](<images/day 65.7.jpg>)

### 8. Silver Layer Mapping Data Flows

![Azure Data Factory transformation flows for airline, flight, passenger, booking, and airport data](<images/day 65.8.jpg>)

## Technologies Used

| Technology                            | Purpose                                                                    |
| ------------------------------------- | -------------------------------------------------------------------------- |
| Azure Data Factory                    | Data ingestion, orchestration, pipelines, triggers, and Mapping Data Flows |
| Azure Data Lake Storage Gen2          | Storage for Bronze, Silver, and Gold datasets                              |
| Azure SQL Database                    | Transactional booking data source                                          |
| Azure Data Factory Mapping Data Flows | Visual data transformation, cleansing, joins, and data serving             |
| Self-hosted Integration Runtime       | Connectivity to on-premises data sources                                   |
| Azure Integration Runtime             | Cloud-based data movement and connectivity                                 |
| REST API                              | Airport reference data ingestion                                           |
| Azure Logic Apps                      | Pipeline success and failure notifications                                 |
| GitHub                                | Source control and repository management                                   |
| Azure DevOps                          | Development collaboration and deployment workflow                          |
| Git                                   | Branching, commits, and version control                                    |
| SQL, CSV, JSON                        | Source data, database operations, and data exchange                        |

**Transformation approach:** The Silver and Gold layers are implemented using Azure Data Factory Mapping Data Flows. Databricks is not used as a separate transformation platform in this project.

## Data Sources

The pipeline integrates five logical datasets from three source types.

| Dataset      | Format    | Source             | Ingestion Method             | Expected Records |
| ------------ | --------- | ------------------ | ---------------------------- | ---------------- |
| Airlines     | CSV       | On-premises        | Azure Data Factory           | 10               |
| Flights      | CSV       | On-premises        | Azure Data Factory           | 10               |
| Passengers   | CSV       | On-premises        | Azure Data Factory           | 10               |
| Airports     | JSON      | REST API           | API ingestion pipeline       | 10               |
| FactBookings | SQL table | Azure SQL Database | Full and incremental loading | 1,000            |

### Dataset Summary

| Metric                   | Value                               |
| ------------------------ | ----------------------------------- |
| Logical source datasets  | 5                                   |
| Source types             | 3                                   |
| Medallion layers         | 3                                   |
| Expected booking records | 1,000                               |
| Booking date range       | 1 January 2025 – 30 June 2025       |
| Airlines                 | 10                                  |
| Airports                 | 10                                  |
| Flights                  | 10                                  |
| Passengers               | 10                                  |
| Gold model               | 4 dimension tables and 1 fact table |

*These figures describe the sample dataset used for the project. Actual record counts may vary depending on the data loaded.*

## Medallion Architecture

### 1. Bronze Layer — Raw Data Ingestion

The Bronze Layer stores source data ingested from the different source systems.

Key activities:

- Migrating on-premises CSV files into Azure Data Lake Storage Gen2.
- Ingesting airport reference data from a REST API.
- Extracting booking data from Azure SQL Database.
- Using reusable and parameterised Azure Data Factory pipelines.
- Applying dynamic mapping where appropriate.
- Maintaining a raw data layer for downstream processing.



### 2. Silver Layer — Data Transformation

The Silver Layer transforms and prepares data from the Bronze Layer using Azure Data Factory Mapping Data Flows.

Key activities:

- Data cleansing and standardisation.
- Data type conversion.
- Column mapping and schema alignment.
- Duplicate handling.
- Applying transformation rules.
- Preparing datasets for downstream modelling and analytics.

This layer demonstrates how Azure Data Factory Mapping Data Flows can be used for transformation without requiring a separate Databricks environment.

### 3. Gold Layer — Data Serving

The Gold Layer is implemented through a separate Azure Data Factory Mapping Data Flow layer focused on preparing business-ready data.

Key activities:

- Preparing dimension and fact datasets.
- Structuring data for analytical queries.
- Supporting revenue and booking analysis.
- Enabling airline and route performance analysis.
- Preparing data for downstream reporting and analytics.

The Gold Layer represents the data-serving stage of the architecture, separating analytical outputs from raw and intermediate datasets.

## Data Model

The analytical model consists of four dimension tables and one fact table.

| Table        | Description                                                                                                                                                 |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| DimAirline   | Airline identifiers, names, and countries                                                                                                                   |
| DimAirport   | Airport identifiers, names, cities, and countries                                                                                                           |
| DimFlight    | Flight identifiers, flight numbers, departure times, and arrival times                                                                                      |
| DimPassenger | Passenger identifiers, names, gender, age, and country                                                                                                      |
| FactBookings | Booking transactions, passenger and flight identifiers, airline and airport identifiers, booking dates, ticket costs, flight durations, and check-in status |

### FactBookings Attributes

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

The fact table connects booking transactions to the relevant dimensions, allowing bookings and revenue to be analysed across airlines, passengers, flights, and airport routes.

## Key Features

### On-Premises to Azure Migration

- Migrates local CSV files into Azure using Azure Data Factory.
- Uses a self-hosted Integration Runtime where required for on-premises connectivity.
- Lands raw source data in the Bronze Layer.

### Dynamic Azure Data Factory Pipelines

- Uses parameterised pipelines and reusable datasets.
- Supports processing multiple datasets through a common ingestion pattern.
- Uses dynamic column mapping where applicable.
- Reduces the need to build a completely separate ingestion pipeline for every dataset.

### REST API Ingestion

- Integrates airport reference data from a REST API.
- Retrieves JSON data for ingestion into Azure.
- Demonstrates integration of API-based and file-based data sources.

### Azure SQL Database Integration

- Uses Azure SQL Database as the source for transactional booking records.
- Supports initial full loading and subsequent incremental ingestion.
- Provides booking data for downstream transformation and modelling.

### Incremental Data Loading

- Performs an initial full load of booking data.
- Uses watermark logic to identify records eligible for incremental ingestion.
- Reduces unnecessary repeated data movement.
- Supports duplicate prevention when combined with appropriate target-side logic.

### Data Transformation Using Mapping Data Flows

- Implements the Silver Layer transformation process in Azure Data Factory.
- Implements a separate Gold Layer Data Flow for data serving.
- Applies data preparation and transformation logic before analytical consumption.
- Keeps the transformation workflow within Azure Data Factory.

### Orchestration and Notifications

- Uses Azure Data Factory to coordinate pipeline execution.
- Configures activity dependencies and pipeline control flow.
- Integrates Azure Logic Apps for success and failure notifications.
- Uses triggers to automate execution according to the configured schedule or events.

### Git Integration and Development Workflow

- Integrates Azure Data Factory with GitHub for source control.
- Uses feature branches to manage development changes.
- Uses pull requests to support controlled changes and collaboration.
- Sets up Azure DevOps as part of the project development and deployment workflow.

## End-to-End Pipeline Workflow

The pipeline follows these steps:

1. **Source Preparation:** Prepare airline, flight, and passenger CSV files and configure the booking data in Azure SQL Database.
2. **Connectivity Configuration:** Configure linked services, datasets, and the required Integration Runtime.
3. **On-Premises Migration:** Copy source CSV files into the Bronze Layer.
4. **API Ingestion:** Retrieve airport reference data from the REST API.
5. **Booking Ingestion:** Load transactional booking records from Azure SQL Database.
6. **Incremental Processing:** Use watermark logic to identify records for subsequent loads.
7. **Silver Transformation:** Transform and prepare Bronze data using Azure Data Factory Mapping Data Flows.
8. **Gold Data Serving:** Use the Gold Data Flow layer to prepare business-ready datasets.
9. **Orchestration:** Coordinate pipeline execution through Azure Data Factory.
10. **Notifications:** Integrate Azure Logic Apps for pipeline success and failure notifications.
11. **Automation:** Configure triggers to automate pipeline execution.
12. **Version Control:** Manage changes using Git integration, feature branches, and pull requests.
13. **Validation:** Validate the resulting datasets and run analytical queries against the Gold outputs.

## Project Structure

The following is a suggested repository structure. Update it to reflect the actual files exported or committed to your repository.

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
├── docs/
│   └── architecture.png
│
├── README.md
└── .gitignore
```

## Getting Started

### Prerequisites

The following resources and tools are required to recreate the project:

- An Azure subscription.
- Azure Data Factory.
- Azure Data Lake Storage Gen2.
- Azure SQL Database.
- Self-hosted Integration Runtime, if required for on-premises connectivity.
- Azure Logic Apps, if implementing pipeline notifications.
- GitHub account.
- Azure DevOps organisation.
- Source CSV files, API configuration, and SQL scripts.

Some Azure services incur charges. Review the applicable pricing before deploying resources.

### Step 1 — Clone the Repository

Replace the placeholders with your actual repository URL.

```
git clone https://github.com/YOUR-USERNAME/YOUR-REPOSITORY.git
cd OnPremAzureDataFactoryProject-Azure-Devops
```

### Step 2 — Create the Azure Resources

Create and configure the Azure resources required by the pipeline:

- Resource Group
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure SQL Database
- Integration Runtime
- Azure Logic App, if required

Configure the required access permissions and connectivity between the services.

### Step 3 — Prepare the Source Data

Prepare the airline, flight, and passenger CSV files and the airport API source.

If the CSV files are stored on-premises, configure the self-hosted Integration Runtime and verify connectivity from Azure Data Factory to the source environment.

### Step 4 — Configure Azure SQL Database

Run the relevant SQL scripts to create and populate the booking table.

Validate the source data using a query such as:

```
SELECT COUNT(*) AS total_bookings
FROM dbo.FactBookings;
```

Confirm the table contains the expected records before starting the ingestion pipeline.

### Step 5 — Configure Azure Data Factory

Configure the linked services, datasets, pipelines, and Mapping Data Flows required for:

- On-premises CSV ingestion.
- REST API ingestion.
- Azure SQL Database ingestion.
- Azure Data Lake Storage Gen2.
- Silver Layer transformations.
- Gold Layer data serving.

Import the exported ADF JSON definitions if they are included in the repository. Otherwise, recreate the components using the documented configuration.

### Step 6 — Run the Initial Ingestion

Execute the ingestion pipelines and confirm that the source data is available in the Bronze Layer.

Validate the file formats, table schemas, and record counts.

### Step 7 — Run Silver and Gold Data Flows

Execute the Silver Layer Mapping Data Flows to transform the Bronze data.

Run the Gold Layer Data Flow to prepare the business-ready datasets.

Validate the resulting tables or files before running analytical queries.

### Step 8 — Configure Orchestration and Automation

Configure the master pipeline, activity dependencies, triggers, and Logic Apps notifications according to your implementation.

Test successful runs and failure scenarios to verify that pipeline execution and notifications behave as expected.

## Incremental Data Loading

The booking ingestion process supports an initial full load followed by incremental loads using watermark logic.

### Initial Load

The initial load extracts the required historical booking records from Azure SQL Database and writes them to the target ingestion layer.

### Incremental Load

A watermark tracks the boundary between previously processed data and records eligible for subsequent ingestion.

The general process is:

1. Read the last successfully stored watermark.
2. Retrieve the current upper watermark from the source.
3. Extract eligible records between the lower and upper boundaries.
4. Write the extracted records to the target.
5. Validate the load and handle duplicates or updates according to the target design.
6. Update the stored watermark only after the load succeeds.

### Important Considerations

- Use a reliable watermark column, such as a monotonically increasing booking ID or a source-maintained modification timestamp.
- A booking date watermark may miss late-arriving records or changes to existing records.
- Apply consistent boundary conditions to prevent missing records between runs.
- Use a staging and MERGE strategy, or an equivalent idempotent write pattern, when updates and duplicate prevention are required.
- Avoid advancing the watermark if the ingestion process fails.
- Test repeated executions to validate incremental loading behaviour.

The precise implementation depends on the watermark column, extraction query, and target write strategy configured in Azure Data Factory.

## Business Metrics and SQL Queries

The Gold Layer can support business reporting and analytical queries related to ticket revenue, bookings, airline performance, check-in rates, and flight duration.

### Example Metrics

| Metric                  | Description                                                |
| ----------------------- | ---------------------------------------------------------- |
| Total Bookings          | Total number of booking records                            |
| Total Ticket Revenue    | Sum of ticket costs                                        |
| Average Ticket Price    | Average cost per booking                                   |
| Check-in Rate           | Percentage of bookings with a successful check-in status   |
| Average Flight Duration | Average flight duration in minutes                         |
| Top Airline             | Airline with the highest number of bookings                |
| Busiest Route           | Origin-to-destination route with the highest booking count |
| Monthly Booking Trend   | Booking volume and revenue by month                        |

### 1. Headline Metrics

The following example assumes the required booking attributes are available in `FactBookings`.

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

### 2. Revenue and Bookings by Airline

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

Populate this table with actual values after running the pipeline and analytical queries.

| Metric                       | Result         |
| ---------------------------- | -------------- |
| Total Ticket Revenue         | To be measured |
| Average Ticket Price         | To be measured |
| Check-in Rate                | To be measured |
| Average Flight Duration      | To be measured |
| Top Airline by Bookings      | To be measured |
| Busiest Route                | To be measured |
| Number of Pipelines          | To be verified |
| Number of Mapping Data Flows | To be verified |
| End-to-End Pipeline Runtime  | To be measured |

*Actual results should be recorded after validating the pipeline outputs.*

## Git Integration and CI/CD Workflow

Git integration was an important part of the project, supporting source control, development collaboration, and a structured approach to managing Azure Data Factory changes.

### GitHub Integration

Azure Data Factory Git integration was used to manage pipeline development and version control.

The workflow supports:

- Tracking pipeline and Data Flow changes.
- Working with feature branches.
- Committing and reviewing changes.
- Using pull requests to manage changes.
- Maintaining a controlled development workflow.

### Azure DevOps

Azure DevOps was incorporated into the project's collaboration and deployment workflow.

The intended development process is:

1. Create a feature branch for a new change.
2. Develop and test the changes in Azure Data Factory.
3. Commit and push the changes to the configured repository.
4. Create a pull request for review.
5. Merge approved changes into the main collaboration branch.
6. Publish or deploy the approved ADF artifacts to the target environment.
7. Validate the deployed pipelines and data flows.

### Deployment Considerations

GitHub and Azure DevOps are separate platforms. Where both are used, the repository arrangement and synchronisation process should be documented clearly.

ADF Git integration and pull requests support source control and collaboration, while deployment to a live environment requires the appropriate publishing and deployment configuration.

Automated CI/CD deployments can be added using Azure DevOps pipelines.

## Challenges and Learning Outcomes

This project provided practical experience in building an end-to-end data engineering workflow using Azure services.

### Technical Learning Outcomes

- Migrating data from an on-premises environment into Azure.
- Integrating file-based, API-based, and relational data sources.
- Building dynamic and parameterised Azure Data Factory pipelines.
- Implementing dynamic column mapping.
- Configuring incremental data loading with watermarks.
- Using Mapping Data Flows for Silver Layer transformations.
- Building a separate Data Flow layer for Gold data serving.
- Implementing Medallion Architecture.
- Designing fact and dimension tables for analytical workloads.
- Orchestrating pipeline execution with Azure Data Factory.
- Integrating Azure Logic Apps and triggers.
- Managing development changes through Git integration and pull requests.
- Understanding how ingestion, transformation, orchestration, and data serving work together in an end-to-end pipeline.

### Key Architectural Learning

One of the main decisions in this project was using Azure Data Factory Mapping Data Flows instead of Databricks for the Silver Layer transformation process.

This provided practical experience with Azure Data Factory's native transformation capabilities and demonstrated how data engineering architectures can be designed around different services depending on project requirements.


## Security Considerations

- Never commit passwords, access keys, tokens, or connection strings.
- Use Azure Key Vault and managed identities where appropriate.
- Apply least-privilege access to storage, databases, and pipeline resources.
- Keep environment-specific configuration separate from pipeline logic.
- Exclude sensitive files and credentials from version control.

## Author

**Areeba Malik**
- LinkedIn: [LINKEDIN-PROFILE](https://www.linkedin.com/in/areeba-malik234/)
