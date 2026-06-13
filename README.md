# ✈️ Real-Time Flight Tracking & Analytics Platform

## 📌 Project Overview

A scalable real-time flight tracking and analytics platform built using Apache Kafka, Apache Spark Structured Streaming, AWS S3, AWS RDS MySQL, and Power BI.

The system continuously ingests live flight data from the OpenSky Network API, processes streaming events using Spark, stores cleaned and aggregated datasets in a cloud-based data lake, and delivers operational insights through interactive Power BI dashboards.

The architecture is designed to simulate an enterprise-grade streaming data pipeline capable of handling thousands of concurrent flight updates with fault tolerance and scalability.


---

# 🎯 Business Problem

Air traffic generates massive volumes of continuously changing data including:

* Flight positions
* Altitude changes
* Velocity updates
* Origin countries
* Flight statuses

Traditional batch processing systems cannot provide timely insights for:

* Air traffic monitoring
* Operational analytics
* Flight density analysis
* Regional traffic trends

This project addresses the challenge by implementing a real-time streaming architecture capable of ingesting, processing, storing, and visualizing live flight information.

---

# 🏗️ System Architecture
<img width="1346" height="792" alt="Architecture" src="https://github.com/user-attachments/assets/397ef3eb-4516-4c2a-a5fd-109266b3f4bf" />

---

# ⚙️ Tech Stack

| Component            | Technology                        |
| -------------------- | --------------------------------- |
| Programming Language | Python                            |
| Streaming Platform   | Apache Kafka                      |
| Stream Processing    | Apache Spark Structured Streaming |
| Data Lake            | AWS S3                            |
| Relational Database  | AWS RDS MySQL                     |
| Visualization        | Power BI                          |
| Deployment           | AWS EC2                           |
| Cloud Provider       | AWS                               |
| API Source           | OpenSky Network API               |

---

# 🚀 Features

### Real-Time Data Ingestion

* Fetches live flight information every 60 seconds
* Handles 6,000–12,000 active flights per batch
* Publishes flight events to Kafka topics

  <img width="1916" height="1049" alt="python-producer" src="https://github.com/user-attachments/assets/336835e2-a0ab-428d-b5c6-99055e4ce26e" />


### Streaming Data Processing
<img width="1916" height="898" alt="kafka-topics" src="https://github.com/user-attachments/assets/d86d6d4e-220f-4687-864f-30cc40cd1dd6" />
<img width="1916" height="898" alt="kafka-consumer" src="https://github.com/user-attachments/assets/0d9dd1a4-0533-4ed5-8ba2-2b2161bf9bb8" />



* Schema validation
* Null handling
* Data type conversion
* Event-time processing
* Watermarking support
* Window-based aggregations

<img width="1916" height="1046" alt="spark_consumer" src="https://github.com/user-attachments/assets/5b0a0c75-dc89-4c6b-8ce1-e42f8b187f3f" />


### Data Lake Storage

Stores cleaned flight data in AWS S3 for:

* Historical analysis
* Future machine learning use cases
* Data archival

### Analytical Processing

Generates:

* Flight count by country
* Regional traffic trends
* Flight activity over time
* Window-based metrics
  
<img width="1916" height="1046" alt="rds_sql_schema" src="https://github.com/user-attachments/assets/b3da32ae-2ab5-4ce5-a2ba-dd6f5f0a1aa8" />


### Dashboarding

Power BI dashboard provides:

* Real-time flight monitoring
* Country-wise flight density
* Traffic trends
* Average Speed
* Average Altitude 

---

# 📂 Project Structure

```text
flight-tracking/

│
├── producer/
│   ├── kafka_producer.py
│   └── opensky_fetcher.py
│
├── spark/
│   ├── spark_consumer.py
│
├── infra/
│   ├── aws
|        └── iam_policy.json
|        └── security_groups.md
│   ├── kafka
|          └── server.properties
|   ├── rds
|        └── schema.sql
│   ├── s3
|        └── s3_bucket.md
│
├── tmp/checkpoints/
│
├── screenshots/
│
├── requirements.txt
|
├── Architecture.png
│
└── README.md
```

---

# 📥 Data Flow

## Step 1: Flight Data Collection

A Python producer continuously requests flight information from OpenSky Network.

Example record:

```json
{
  "icao24": "a1b2c3",
  "callsign": "AAL123",
  "origin_country": "United States",
  "longitude": -73.7781,
  "latitude": 40.6413,
  "altitude": 32000,
  "velocity": 245.5,
  "timestamp": "2025-06-01T12:00:00Z"
}
```

---

## Step 2: Kafka Streaming

The producer publishes flight events to Kafka.

Topic:

```text
flights
```

Benefits:

* Decouples ingestion and processing
* Supports scalability
* Provides fault tolerance

---

## Step 3: Spark Structured Streaming

Spark consumes data from Kafka and performs:

### Data Validation

* Remove malformed records
* Handle missing values

### Data Transformation

* Cast data types
* Generate event timestamps
* Standardize fields

### Watermarking

```python
.withWatermark(
    "event_timestamp",
    "10 minutes"
)
```

Purpose:

* Handles late arriving events
* Prevents unbounded state growth

---

## Step 4: Window Aggregation

Example aggregation:

```python
.groupBy(
    window(
        col("event_timestamp"),
        "5 minutes"
    ),
    col("origin_country")
)
.agg(
    count("*").alias("flight_count")
)
```

Produces:

| Window      | Country | Flight Count |
| ----------- | ------- | ------------ |
| 12:00-12:05 | USA     | 480          |
| 12:00-12:05 | India   | 220          |
| 12:00-12:05 | Germany | 190          |

---

## Step 5: Data Storage

### AWS S3

Stores:

```text
s3://flight-data-lake/clean/
```

Purpose:

* Historical analysis
* Backup storage
* Data lake architecture

### AWS RDS MySQL

Stores aggregated metrics for dashboard consumption.

Example table:

```sql
flight_metrics
```

| window_start |
| country |
| flight_count |

---

# 📊 Dashboard Metrics

The Power BI dashboard tracks:

### Flight Activity

* Total flights
* Active flights

### Geographic Analysis

* Flights by country
* Regional traffic density

### Time Analysis

* Flight trends over time
* Peak traffic periods

### Operational KPIs

* Top active countries
* Moving average flight volume
* Traffic growth trends

---

# ☁️ AWS Deployment

## EC2

Hosts:

* Kafka Broker
* Zookeeper
* Spark Streaming Application
* Producer Service

### Instance Configuration

Recommended: Bare Minimum

```text
t2.micro
1 GB RAM
1 vCPU
```

For larger workloads:

```text
t3.large
8 GB RAM
2 vCPU
```

---

## S3

Stores:

```text
Raw Data
Clean Data
Checkpoint Data
```

---

## RDS MySQL

Stores:

```text
Aggregated Flight Metrics
Dashboard Tables
```

---

# 📈 Scalability Considerations

Current Load:

```text
6,000 - 12,000 flights
1-minute ingestion interval
```

Estimated throughput:

```text
500-1000 records/sec
```

Scaling options:

### Kafka

* Increase topic partitions
* Add brokers

### Spark

* Increase executors
* Increase executor memory
* Add worker nodes

### Storage

* Partition S3 data by date
* Optimize MySQL indexes

---

# 🔒 Fault Tolerance

### Kafka

* Message persistence
* Consumer offsets

### Spark

Checkpointing:

```python
.option(
    "checkpointLocation",
    checkpoint_path
)
```

Benefits:

* Recovery after failure
* Exactly-once processing support

---

# 📊 Sample Business Insights

Using the dashboard, stakeholders can answer:

* Which countries currently have the highest air traffic?
* How does flight volume change throughout the day?
* What are the busiest traffic windows?
* Which regions experience the fastest growth in flight activity?

---

# 🧠 Key Learnings

* Real-time streaming architecture design
* Apache Kafka messaging systems
* Spark Structured Streaming
* Event-time processing
* Watermarking and state management
* AWS Data Lake implementation
* Cloud deployment
* Power BI dashboarding
* Distributed data processing

---

# 🔮 Future Enhancements

### Real-Time Alerts

* Delayed flights
* Flight anomalies

### Machine Learning

* Flight delay prediction
* Traffic forecasting

### Advanced Analytics

* Flight route optimization
* Congestion prediction

### Containerization

* Docker
* Kubernetes
* AWS ECS/EKS

---

# 👨‍💻 Author

Data Engineer | Data Analyst | Big Data Enthusiast

### Skills

* Apache Spark
* Kafka
* AWS
* SQL
* Python
* Power BI
* Data Engineering

---

## ⭐ If you found this project useful, consider starring the repository.
