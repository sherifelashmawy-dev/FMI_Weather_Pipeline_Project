"""
FMI Weather Data Producer
=========================
Fetches weather data from FMI and sends to Kafka.

Author: Sherif Elashmawy  
Project: FMI Weather Pipeline
Date: December 2025
"""

import json
import time
import pandas as pd
import logging
from datetime import datetime
from typing import List, Optional, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaError
import fmi_weather_client as fmi

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../data/logs/producer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FMIWeatherProducer:
    """Producer that fetches FMI weather data and sends to Kafka."""
    
    def __init__(self,
                 bootstrap_servers: List[str] = ['localhost:9092'],
                 topic_name: str = 'fmi-weather-raw',
                 stations_file: str = '../data/FMI_stations_verified.csv',
                 interval_seconds: int = 600):
        
        self.topic_name = topic_name
        self.interval_seconds = interval_seconds
        
        # Initialize FMI client
        self.fmi_client = fmi.FMIClient()
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        logger.info(f"Kafka producer initialized. Topic: {topic_name}")
        
        # Load stations
        self.stations = self._load_stations(stations_file)
        logger.info(f"Loaded {len(self.stations)} weather stations")
    
    def _load_stations(self, filepath: str) -> pd.DataFrame:
        """Load active weather stations."""
        try:
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                try:
                    df = pd.read_csv(filepath, encoding=encoding)
                    active = df[
                        (df['finish'].isna()) | 
                        (pd.to_numeric(df['finish'], errors='coerce') >= 2020)
                    ].copy()
                    return active
                except UnicodeDecodeError:
                    continue
            
            logger.warning("Using fallback station list")
            return pd.DataFrame({
                'FMISID': [100971, 101786, 101846, 101267, 100919],
                'Name': ['Helsinki Kaisaniemi', 'Oulu', 'Rovaniemi', 'Tampere', 'Mariehamn']
            })
        except Exception as e:
            logger.error(f"Error loading stations: {e}")
            return pd.DataFrame({'FMISID': [100971], 'Name': ['Helsinki Kaisaniemi']})
    
    def fetch_fmi_data(self, station_id: int) -> Optional[Dict]:
        """Fetch weather data using FMI client."""
        try:
            # Use the FMI client to get latest observations
            weather_data = self.fmi_client.get_latest_observations(str(station_id))
            
            if not weather_data:
                logger.warning(f"Station {station_id}: No data returned")
                return None
            
            # Log successful fetch
            if weather_data.get('temperature') is not None:
                logger.info(f"Station {station_id}: temp={weather_data['temperature']}°C")
            
            # Add ingestion_time (current UTC time)
            weather_data['ingestion_time'] = datetime.utcnow().isoformat()
            
            # Convert fmisid to integer
            weather_data['fmisid'] = int(weather_data['fmisid'])
            
            # Add location name from stations DataFrame
            station_row = self.stations[self.stations['FMISID'] == station_id]
            if not station_row.empty:
                weather_data['location'] = station_row.iloc[0]['Name']
            else:
                weather_data['location'] = None
            
            # cloud_cover and visibility now come from FMI client
            # They're already in weather_data from the client
            
            return weather_data
            
        except Exception as e:
            logger.error(f"Error fetching station {station_id}: {e}")
            return None
    
    def send_to_kafka(self, station_id: int, weather_data: Dict) -> bool:
        """Send weather data to Kafka."""
        try:
            future = self.producer.send(
                self.topic_name,
                key=station_id,
                value=weather_data
            )
            
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Sent station {station_id} to partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending station {station_id}: {e}")
            return False
    
    def run(self):
        """Main producer loop."""
        logger.info(f"Starting FMI Weather Producer")
        logger.info(f"Fetch interval: {self.interval_seconds} seconds")
        logger.info(f"Monitoring {len(self.stations)} weather stations")
        
        cycle = 1
        
        try:
            while True:
                logger.info(f"=== Cycle {cycle} at {datetime.now()} ===")
                
                success = 0
                failed = 0
                
                for _, row in self.stations.iterrows():
                    station_id = int(row['FMISID'])
                    
                    weather_data = self.fetch_fmi_data(station_id)
                    
                    if weather_data:
                        if self.send_to_kafka(station_id, weather_data):
                            success += 1
                        else:
                            failed += 1
                    else:
                        failed += 1
                    
                    # Small delay between stations
                    time.sleep(0.5)
                
                logger.info(f"Cycle {cycle} complete. Success: {success}, Failed: {failed}")
                logger.info(f"Sleeping for {self.interval_seconds} seconds...")
                
                time.sleep(self.interval_seconds)
                cycle += 1
                
        except KeyboardInterrupt:
            logger.info("Shutting down producer")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer stopped")


def main():
    producer = FMIWeatherProducer(
        bootstrap_servers=['localhost:9092'],
        topic_name='fmi-weather-raw',
        stations_file='../data/FMI_stations_verified.csv',
        interval_seconds=600
    )
    
    producer.run()


if __name__ == "__main__":
    main()