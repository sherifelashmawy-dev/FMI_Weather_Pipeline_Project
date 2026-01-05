# CSC Cloud Production Deployment

## Live System

- **Dashboard:** http://195.148.30.152:8501
- **Airflow:** http://195.148.30.152:8080  
- **Kafka UI:** http://195.148.30.152:8082

## Architecture

Complete real-time weather data pipeline deployed on CSC cloud infrastructure.

### Services (All Systemd-Managed)

| Service | Status | Auto-Start | Auto-Restart |
|---------|--------|------------|--------------|
| fmi-streamlit | ✅ Running | ✅ Yes | ✅ Yes |
| fmi-airflow-webserver | ✅ Running | ✅ Yes | ✅ Yes |
| fmi-airflow-scheduler | ✅ Running | ✅ Yes | ✅ Yes |
| fmi-kafka (Docker) | ✅ Running | ✅ Yes | ✅ Yes |
| fmi-producer | ✅ Running | ✅ Yes | ✅ Yes |
| fmi-consumer | ✅ Running | ✅ Yes | ✅ Yes |

### Data Flow
```
FMI API (64 weather stations, 10-min polling)
    ↓
Producer Service (fmi-producer.service)
    ↓
Kafka Broker (Docker: confluentinc/cp-kafka:7.4.0)
    ↓
Consumer Service (fmi-consumer.service)
    ↓
Google BigQuery (data-analytics-project-482302.fmi_weather)
    ↓
Airflow Scheduler (Daily 1 AM processing)
    ↓
Streamlit Dashboard (Real-time visualization)
```

### Infrastructure

- **Cloud Provider:** CSC (Finnish Academic Cloud)
- **VM Size:** standard.large
- **RAM:** 7.6 GB
- **CPU:** 4 cores
- **OS:** Ubuntu 24.04 LTS
- **Orchestration:** Systemd
- **Containerization:** Docker & Docker Compose

### Resilience Features

1. **Auto-restart on failure** - All services monitored by systemd
2. **Auto-start on boot** - All services enabled
3. **Process recovery** - Tested with kill -9, auto-restarted within 10 seconds
4. **Independent operation** - Runs 24/7 without manual intervention
5. **Kafka offset management** - Consumer tracks progress
6. **Connection retry logic** - BigQuery and FMI API retries

### Verification Commands
```bash
# Check all services
sudo systemctl status fmi-*

# Check Docker containers
docker ps

# View producer logs
sudo journalctl -u fmi-producer -n 50

# View consumer logs
sudo journalctl -u fmi-consumer -n 50

# Check Kafka messages
docker exec -it fmi-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fmi-weather-raw \
  --from-beginning \
  --max-messages 5
```

### Deployment Date

December 31, 2025

### Zero-Downtime Operation

✅ Survives SSH disconnection  
✅ Survives terminal closure  
✅ Survives service crashes  
✅ Survives VM reboots  
✅ Continuous data collection 24/7
