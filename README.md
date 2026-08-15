# Cognistream: Developer Flow-State & Cognitive Load Analytics

Cognistream is a real-time data analytics project that analyzes developer activity events to understand activity patterns, interruptions, and focus-related behavior.

The project uses Python to generate activity events, Apache Kafka for real-time streaming, PostgreSQL for data storage and SQL analytics, and Power BI for interactive visualization.

## Project Objective

* Analyze developer activity patterns
* Measure activity hours and events
* Analyze interruption rates
* Compare different activity types
* Generate developer-level analytics
* Visualize insights through an interactive Power BI dashboard

## Data Pipeline

```text
Python Activity Simulator
          ↓
      Apache Kafka
          ↓
   Kafka Consumer
          ↓
      PostgreSQL
          ↓
     SQL Analytics
          ↓
       Power BI
          ↓
 Interactive Dashboard
```

## Technologies Used

* **Python** – Activity event simulation and data processing
* **Apache Kafka** – Real-time event streaming
* **PostgreSQL** – Activity data storage
* **SQL** – Data analysis and metric calculation
* **Power BI** – Dashboard and data visualization
* **Docker** – PostgreSQL container
* **Git & GitHub** – Version control
* **VS Code** – Development environment

## Key Metrics

The project analyzes metrics such as:

* Total Events
* Total Activity Hours
* Productive Hours
* Interruption Count
* Interruption Rate
* Context-Switch Count
* Average Activity Duration

## Project Implementation

### 1. Activity Simulation

A Python-based simulator generates developer activity events containing information about developers, applications, activity types, duration, and interruptions.

### 2. Real-Time Streaming

The generated events are published to an Apache Kafka topic for real-time event processing.

### 3. PostgreSQL Storage

A PostgreSQL database running in Docker stores the incoming activity events.

A Python Kafka consumer reads events from Kafka and inserts them into the `developer_activity` table.

More than **2,000 activity events** were generated and processed during the project.

### 4. SQL Analytics

SQL was used to analyze the stored data and calculate developer-level and activity-level metrics, including activity hours and interruption rates.

### 5. Power BI Dashboard

The analyzed data was visualized using Power BI.

The dashboard includes:

* KPI cards
* Developer activity analysis
* Activity hours
* Interruption-rate analysis
* Activity-type analysis
* Application/platform activity
* Interactive filtering

## Project Structure

```text
Cognistream_Realtime_Project/
│
├── analytics/
├── consumer/
│   └── kafka_to_postgres.py
├── dashboard/
├── data/
│   └── activity_events.jsonl
├── streaming/
├── Cognistream_dashboard.pbix
├── README.md
└── .gitignore
```

## Key Skills Demonstrated

* Python
* SQL
* Real-Time Data Streaming
* Apache Kafka
* PostgreSQL
* Data Pipeline Development
* Data Analytics
* Power BI
* Data Visualization
* Docker
* Git & GitHub

## Project Outcome

Cognistream demonstrates an end-to-end real-time data analytics workflow:

**Data Generation → Real-Time Streaming → Database Storage → SQL Analytics → Power BI Dashboard**

The project converts raw developer activity events into meaningful analytics for understanding developer activity and interruption patterns.

## Author

**Madhuri Duddupudi**

**Data Analytics | Python | SQL | Power BI | Data Visualization**
