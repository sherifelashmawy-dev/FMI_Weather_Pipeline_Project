"""
FMI Weather Data Consumer
=========================
This program reads weather data from Kafka and uploads it to BigQuery.
It runs continuously, batching messages for efficient uploads.

Author: Sherif Elashmawy
Project: FMI Weather Pipeline
Date: December 2025
"""

# Import libraries
import json
import logging
from datetime import datetime
from typing import List, Dict
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from google.cloud import bigquery
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../data/logs/consumer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FMIKafkaConsumer:
    """
    Consumer that reads FMI weather data from Kafka and loads to BigQuery.
    
    This class handles:
    - Connecting to Kafka and subscribing to topic
    - Batching messages for efficiency
    - Creating BigQuery dataset and table
    - Uploading batches to BigQuery
    - Committing Kafka offsets
    """
    
    def __init__(self,
                 bootstrap_servers: List[str] = ['localhost:9092'],
                 topic_name: str = 'fmi-weather-raw',
                 group_id: str = 'fmi-bigquery-consumer-v5',
                 bigquery_project: str = 'data-analytics-project-482302',
                 bigquery_dataset: str = 'fmi_weather',
                 bigquery_table: str = 'raw_observations',
                 credentials_path: str = '/home/ubuntu/config/data-analytics-project-482302-254e4fd06277.json',
                 batch_size: int = 100):
        """
        Initialize the Kafka consumer.
        
        Parameters:
            bootstrap_servers: List of Kafka broker addresses
            topic_name: Name of Kafka topic to consume from
            group_id: Consumer group ID
            bigquery_project: GCP project ID
            bigquery_dataset: BigQuery dataset name
            bigquery_table: BigQuery table name
            credentials_path: Path to GCP credentials JSON
            batch_size: Number of messages to batch before uploading
        """
        self.topic_name = topic_name
        self.batch_size = batch_size
        self.message_batch = []  # List to collect messages
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='earliest',  # Skip old bad messages!
            enable_auto_commit=False,  # Manual commit after BigQuery upload
            max_poll_records=batch_size
        )
        logger.info(f"Kafka consumer initialized. Subscribed to topic '{topic_name}'")
        logger.info(f"Consumer group: {group_id}")
        
        # Initialize BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        self.bq_client = bigquery.Client(
            credentials=credentials,
            project=bigquery_project
        )
        
        self.project_id = bigquery_project
        self.dataset_id = bigquery_dataset
        self.table_id = bigquery_table
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        logger.info(f"BigQuery client initialized. Target table: {self.full_table_id}")
        
        # Ensure dataset and table exist
        self._setup_bigquery()
    
    def _setup_bigquery(self):
        """
        Create BigQuery dataset and table if they don't exist.
        This runs automatically when Consumer is created.
        """
        # Create dataset if it doesn't exist
        dataset_ref = self.bq_client.dataset(self.dataset_id)
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US" 
            self.bq_client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")
        
        # Create table if it doesn't exist
        table_ref = dataset_ref.table(self.table_id)
        try:
            self.bq_client.get_table(table_ref)
            logger.info(f"Table {self.table_id} already exists")
        except Exception:
            # Define table schema (column names and types)
            schema = [
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("fmisid", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("temperature", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("humidity", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("wind_speed", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("wind_direction", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("pressure", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("precipitation", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("cloud_cover", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("visibility", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(table_ref, schema=schema)
            
            # Partition by timestamp for better query performance
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"
            )
            
            self.bq_client.create_table(table)
            logger.info(f"Created table {self.table_id}")
    
    def upload_to_bigquery(self, records: List[Dict]):
        """
        Upload batch of records to BigQuery.
        
        Parameters:
            records: List of weather data dictionaries
        """
        if not records:
            logger.warning("No records to upload")
            return
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # CRITICAL FIX - Filter out records with null required fields
            initial_count = len(df)
            
            # Remove records with null timestamp or fmisid
            df = df.dropna(subset=['timestamp', 'fmisid'])
            
            # Remove records where timestamp or ingestion_time is empty string
            df = df[df['timestamp'].astype(str).str.strip() != '']
            df = df[df['ingestion_time'].astype(str).str.strip() != '']
            
            filtered_count = initial_count - len(df)
            if filtered_count > 0:
                logger.warning(f"Filtered out {filtered_count} records with null required fields")
            
            if len(df) == 0:
                logger.warning("All records filtered out - nothing to upload")
                return
            
            # Explicit type conversions
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['ingestion_time'] = pd.to_datetime(df['ingestion_time'], errors='coerce')
            
            # Drop any rows where timestamp conversion failed
            df = df.dropna(subset=['timestamp', 'ingestion_time'])
            
            if len(df) == 0:
                logger.warning("All records had invalid timestamps - nothing to upload")
                return
            
            # Convert fmisid to integer
            df['fmisid'] = df['fmisid'].astype('int64')
            
            # Convert numeric columns to float
            numeric_cols = ['latitude', 'longitude', 'temperature', 'humidity', 
                          'wind_speed', 'wind_direction', 'pressure', 
                          'precipitation', 'cloud_cover', 'visibility']
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Upload to BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ]
            )
            
            job = self.bq_client.load_table_from_dataframe(
                df,
                self.full_table_id,
                job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"✅ Successfully uploaded {len(df)} records to BigQuery")
            
        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {e}")
            # Log first record for debugging
            if records:
                logger.error(f"Sample record: {records[0]}")
            raise
    
    def process_message(self, message):
        """
        Process a single message from Kafka.
        Add to batch, upload when batch is full.
        
        Parameters:
            message: Kafka message object
        """
        try:
            # Add message to batch
            self.message_batch.append(message.value)
            
            # Upload batch when it reaches batch_size
            if len(self.message_batch) >= self.batch_size:
                logger.info(f"Batch size reached ({self.batch_size}), uploading to BigQuery")
                self.upload_to_bigquery(self.message_batch)
                
                # Commit Kafka offset after successful upload
                self.consumer.commit()
                logger.info("Committed Kafka offset")
                
                # Clear batch
                self.message_batch = []
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def run(self):
        """
        Main consumer loop - consume messages and upload to BigQuery.
        Runs forever until stopped with Ctrl+C.
        """
        logger.info("Starting consumer loop...")
        logger.info("Waiting for messages from Kafka...")
        
        try:
            for message in self.consumer:
                logger.debug(
                    f"Received message: partition={message.partition}, "
                    f"offset={message.offset}, key={message.key}"
                )
                
                self.process_message(message)
                
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user (Ctrl+C)")
        except Exception as e:
            logger.error(f"Unexpected error in consumer loop: {e}")
        finally:
            # Upload any remaining messages in batch
            if self.message_batch:
                logger.info(f"Uploading remaining {len(self.message_batch)} messages")
                try:
                    self.upload_to_bigquery(self.message_batch)
                    self.consumer.commit()
                except Exception as e:
                    logger.error(f"Error uploading final batch: {e}")
            
            self.consumer.close()
            logger.info("Consumer closed cleanly")


def main():
    """
    Main function to run the consumer.
    This runs when you execute: python consumer.py
    """
    # Configuration
    KAFKA_SERVERS = ['localhost:9092']
    TOPIC_NAME = 'fmi-weather-raw'
    GROUP_ID = 'fmi-bigquery-consumer-v5'  # New group - fresh start!
    
    # BigQuery configuration
    BIGQUERY_PROJECT = 'data-analytics-project-482302'
    BIGQUERY_DATASET = 'fmi_weather'
    BIGQUERY_TABLE = 'raw_observations'
    CREDENTIALS_PATH = '/home/ubuntu/config/data-analytics-project-482302-254e4fd06277.json'
    
    BATCH_SIZE = 100
    
    # Create and run consumer
    consumer = FMIKafkaConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        topic_name=TOPIC_NAME,
        group_id=GROUP_ID,
        bigquery_project=BIGQUERY_PROJECT,
        bigquery_dataset=BIGQUERY_DATASET,
        bigquery_table=BIGQUERY_TABLE,
        credentials_path=CREDENTIALS_PATH,
        batch_size=BATCH_SIZE
    )
    
    # Run continuously
    consumer.run()


if __name__ == "__main__":
    main()
