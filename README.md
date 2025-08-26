# Dockerized Stock Market Data Pipeline with Airflow

This project is a **Dockerized data pipeline** built using **Apache Airflow** that automatically fetches stock market data from a free API, processes it, and stores it in a **PostgreSQL** database.  

---

## Overview  

The goal of this pipeline is simple and practical:  

1. **Fetch** stock market JSON data on a schedule.  
2. **Extract** useful fields from the API response.  
3. **Store** the data in PostgreSQL safely and reliably.  
4. **Run** the entire workflow smoothly inside Docker containers with Docker Compose.  

It demonstrates how **orchestration, error handling, and environment variable–based configuration** can be combined to build a **robust and scalable data workflow**.  

---

## Features  

- **Automated Scheduling**: Orchestrated using **Airflow DAGs** to fetch data daily/hourly.  
- **Data Fetching**: Uses Python `requests` library to call a stock market API.  
- **Data Processing**: Parses JSON response and extracts required fields.  
- **Database Storage**: Updates an existing **PostgreSQL** table with processed data.  
- **Error Handling**: Gracefully manages missing data or API failures with retries.  
- **Secure Configurations**: API keys & DB credentials handled via **environment variables**.  
- **Fully Dockerized**: One command (`docker-compose up`) spins up Airflow, PostgreSQL, and the pipeline.  

---

## Tech Stack  

- **Python**  
- **Apache Airflow** (for orchestration)  
- **PostgreSQL** (for data storage)  
- **Docker & Docker Compose** (for containerization)  

---

## Setup Instructions  

### 1️. Clone the repository  
```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```

### 2️. Create a .env file with environment variables
```bash
API_KEY
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_DB
```

### 3️. Start the pipeline with Docker Compose
```bash
docker-compose up --build
Access Airflow UI
---
Open: http://localhost:8080
(Default credentials: airflow / airflow)
```

Trigger the DAG stock_data_pipeline to start fetching and storing stock data.

## Deliverables Implemented
✔ docker-compose.yml – Defines services for Airflow, PostgreSQL, and dependencies.

✔ Airflow DAG – Contains the pipeline steps (fetch → transform → store).

✔ Data Fetching Script – Handles API calls & JSON parsing.

✔ Database Integration – Inserts extracted data into PostgreSQL.

✔ README.md – Explains how to run and use the project.

## Reference
Full project code and setup available here:
👉 [Project Repository](https://github.com/Pranathi-Chintarapu64/stock_market)
