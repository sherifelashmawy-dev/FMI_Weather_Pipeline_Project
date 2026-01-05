# 🌦️ FMI Weather Data Pipeline

> **Production Data Engineering Pipeline deployed on CSC Cloud Infrastructure**

[![Live Dashboard](https://img.shields.io/badge/🌐_Live-Dashboard-blue?style=for-the-badge)](http://195.148.30.152:8501)
[![Airflow](https://img.shields.io/badge/⚙️_Airflow-UI-orange?style=for-the-badge)](http://195.148.30.152:8080)
[![Kafka](https://img.shields.io/badge/📊_Kafka-UI-green?style=for-the-badge)](http://195.148.30.152:8082)

Real-time weather data streaming from 64 Finnish Meteorological Institute stations, processed through a production-grade data pipeline with Kafka, BigQuery, Airflow, and Streamlit.

---

## 🎯 Live System Access

| Service | URL | Description |
|---------|-----|-------------|
| 🌐 **Dashboard** | http://195.148.30.152:8501 | Interactive weather visualization |
| ⚙️ **Airflow** | http://195.148.30.152:8080 | Pipeline orchestration UI |
| 📊 **Kafka UI** | http://195.148.30.152:8082 | Real-time stream monitoring |

---

## 🏗️ Architecture
```
┌─────────────┐
│  FMI API    │  64 Weather Stations, 10-min polling
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Producer   │  Python service (systemd)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Kafka    │  Message broker (Docker)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Consumer   │  Python service (systemd)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  BigQuery   │  Data warehouse (Google Cloud)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Airflow   │  Daily processing (1 AM UTC)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Streamlit  │  Interactive dashboard
└─────────────┘
```

---

## ✨ Key Features

### Production Infrastructure
- ✅ **6 Systemd Services** - Auto-restart, auto-start on boot
- ✅ **Docker Containerization** - Kafka ecosystem
- ✅ **24/7 Operation** - Zero manual intervention
- ✅ **Self-healing** - Automatic recovery from failures
- ✅ **Cloud Deployment** - CSC infrastructure (7.6GB RAM, 4 cores)

### Data Pipeline
- ✅ **Real-time Streaming** - Kafka-based event architecture
- ✅ **64 Weather Stations** - Continuous monitoring
- ✅ **10-Minute Polling** - ~8,640 observations/day
- ✅ **Data Quality Checks** - Automated validation & scoring
- ✅ **Daily Aggregation** - Statistical summaries

### Technology Stack
- **Streaming:** Apache Kafka 7.4.0
- **Data Warehouse:** Google BigQuery
- **Orchestration:** Apache Airflow 2.10.4
- **Visualization:** Streamlit
- **Containerization:** Docker & Docker Compose
- **Cloud:** CSC (Finnish Academic Computing)
- **Service Management:** Systemd
- **Language:** Python 3.11

---

## 📂 Project Structure
```
├── kafka/
│   ├── producer.py              # Weather data producer
│   ├── consumer.py              # BigQuery consumer
│   ├── docker-compose.yml       # Kafka & Zookeeper
│   └── fmi_weather_client.py    # FMI API client
├── airflow/
│   └── dags/
│       └── fmi_processing_dag.py  # Daily processing workflow
├── streamlit/
│   └── streamlit_app.py         # Interactive dashboard
├── data/
│   └── FMI_stations_verified.csv  # 64 active stations
├── DEPLOYMENT.md                # Infrastructure details
└── README.md
```

---

## 🚀 Deployment

**CSC Cloud Infrastructure:**
- VM: standard.large (7.6GB RAM, 4 cores)
- OS: Ubuntu 24.04 LTS
- 6 systemd services + 3 Docker containers

**All services configured for:**
- Auto-start on boot
- Auto-restart on crash
- 24/7 autonomous operation

See [DEPLOYMENT.md](DEPLOYMENT.md) for complete infrastructure details.

---

## 📊 System Metrics

| Metric | Value |
|--------|-------|
| Weather Stations | 64 active |
| Data Points/Day | ~8,640 observations |
| Uptime | 24/7 |
| Data Quality | 85%+ completeness |
| Auto-restart | < 10 seconds |

---

## 🏆 Achievements

- ✅ Zero-downtime production deployment
- ✅ Fully automated data pipeline
- ✅ Self-healing infrastructure
- ✅ Real-time streaming (not batch)
- ✅ Complete documentation

---

**Status:** ✅ Production - Running 24/7  
**Author:** Sherif Elashmawy  
**Date:** December 2025 - January 2026
