"""
JobPulse - Apache Kafka Streaming Ingestion Pipeline
Handles ingestion of raw CSV job postings into Apache Kafka topics.
Supports both live Kafka brokers (via kafka-python/confluent-kafka) and a high-fidelity
standalone Kafka stream simulator for zero-dependency local execution.
"""
import os
import sys
import json
import time
import logging
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("JobPulse.Kafka")

# Constants
DEFAULT_TOPIC = "jobpulse-raw-jobs"
DEFAULT_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
STREAM_BUFFER_FILE = os.path.join(DATA_DIR, "kafka_stream_buffer.jsonl")


class KafkaStreamBroker:
    """
    High-fidelity Kafka Topic & Partition Broker Simulator.
    Used when an external Kafka cluster is not running. Persists messages to a buffer
    with exact Kafka offsets, partitions, message keys, and timestamps.
    """
    def __init__(self, topic: str = DEFAULT_TOPIC, num_partitions: int = 3):
        self.topic = topic
        self.num_partitions = num_partitions
        self.buffer_file = STREAM_BUFFER_FILE
        self._ensure_buffer_file()

    def _ensure_buffer_file(self):
        os.makedirs(os.path.dirname(self.buffer_file), exist_ok=True)
        if not os.path.exists(self.buffer_file):
            with open(self.buffer_file, 'w', encoding='utf-8') as f:
                pass

    def get_partition_for_key(self, key: str) -> int:
        if not key:
            return 0
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16) % self.num_partitions

    def get_latest_offset(self, partition: int) -> int:
        count = 0
        if os.path.exists(self.buffer_file):
            with open(self.buffer_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            msg = json.loads(line)
                            if msg.get("topic") == self.topic and msg.get("partition") == partition:
                                count = max(count, msg.get("offset", 0) + 1)
                        except json.JSONDecodeError:
                            continue
        return count

    def append_message(self, key: str, value: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        partition = self.get_partition_for_key(key)
        offset = self.get_latest_offset(partition)
        timestamp = datetime.utcnow().isoformat() + "Z"

        record = {
            "topic": self.topic,
            "partition": partition,
            "offset": offset,
            "key": key,
            "timestamp": timestamp,
            "headers": headers or {},
            "value": value
        }

        with open(self.buffer_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record) + "\n")

        return record

    def read_messages(self, from_beginning: bool = True, max_messages: Optional[int] = None) -> List[Dict[str, Any]]:
        messages = []
        if not os.path.exists(self.buffer_file):
            return messages

        with open(self.buffer_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        if record.get("topic") == self.topic:
                            messages.append(record)
                            if max_messages and len(messages) >= max_messages:
                                break
                    except json.JSONDecodeError:
                        continue
        return messages

    def clear(self):
        if os.path.exists(self.buffer_file):
            with open(self.buffer_file, 'w', encoding='utf-8') as f:
                pass


class JobPostingKafkaProducer:
    """
    Kafka Producer for streaming raw CSV job postings into the message broker.
    Attempts live Kafka connection first; falls back smoothly to KafkaStreamBroker.
    """
    def __init__(self, bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS, topic: str = DEFAULT_TOPIC):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.live_producer = None
        self.mode = "simulator"
        self._init_producer()

    def _init_producer(self):
        try:
            from kafka import KafkaProducer
            self.live_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                request_timeout_ms=3000,
                max_block_ms=3000
            )
            self.mode = "live"
            logger.info(f"Connected to Live Apache Kafka Broker at {self.bootstrap_servers}")
        except Exception as e:
            self.mode = "simulator"
            self.broker = KafkaStreamBroker(topic=self.topic)
            logger.info(f"Live Kafka not reachable ({e}). Using Kafka Stream Simulator (KRaft-compliant buffer).")

    def send_record(self, key: str, record_data: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        if self.mode == "live" and self.live_producer:
            try:
                future = self.live_producer.send(
                    self.topic,
                    key=key,
                    value=record_data
                )
                meta = future.get(timeout=5)
                return {
                    "topic": meta.topic,
                    "partition": meta.partition,
                    "offset": meta.offset,
                    "key": key,
                    "mode": "live"
                }
            except Exception as e:
                logger.warning(f"Live Kafka send failed: {e}. Falling back to stream buffer.")
                self.mode = "simulator"
                self.broker = KafkaStreamBroker(topic=self.topic)

        # Fallback to local streaming broker
        return self.broker.append_message(key=key, value=record_data, headers=headers)

    def flush(self):
        if self.mode == "live" and self.live_producer:
            try:
                self.live_producer.flush()
            except Exception as e:
                logger.warning(f"Error flushing live producer: {e}")

    def ingest_csv(self, csv_path: str, max_rows: Optional[int] = None, delay_ms: int = 5, clear_buffer_before_ingest: bool = False) -> Dict[str, Any]:
        """
        Reads CSV and streams each record as a serialized Kafka event message.
        """
        import pandas as pd
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        if clear_buffer_before_ingest and hasattr(self, 'broker') and self.broker:
            self.broker.clear()

        logger.info(f"Ingesting raw CSV into Kafka topic '{self.topic}' from {csv_path}...")
        df = pd.read_csv(csv_path, encoding='utf-8', on_bad_lines='skip')
        if max_rows:
            df = df.head(max_rows)

        total_rows = len(df)
        published_records = []
        start_time = time.time()

        for idx, row in df.iterrows():
            record = row.to_dict()
            # Generate deterministic message key: company + job_title
            key = f"{str(record.get('company', 'unknown'))}::{str(record.get('job_title', 'unknown'))}"
            
            headers = {
                "source_file": os.path.basename(csv_path),
                "row_index": str(idx),
                "ingestion_engine": "JobPulse-Kafka-Producer",
                "schema_version": "1.0"
            }

            event_payload = {
                "event_id": f"evt_{int(time.time()*1000)}_{idx}",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "raw_data": record
            }

            res = self.send_record(key=key, record_data=event_payload, headers=headers)
            published_records.append(res)

            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)

        self.flush()
        elapsed = time.time() - start_time
        throughput = total_rows / elapsed if elapsed > 0 else 0

        logger.info(f"Kafka Ingestion Complete: {total_rows} records published to topic '{self.topic}' in {elapsed:.3f}s ({throughput:.1f} msgs/sec). Mode: {self.mode}")

        return {
            "topic": self.topic,
            "total_records": total_rows,
            "duration_seconds": round(elapsed, 3),
            "throughput_msg_per_sec": round(throughput, 1),
            "broker_mode": self.mode,
            "sample_published": published_records[:3]
        }


class JobPostingKafkaConsumer:
    """
    Kafka Consumer to pull raw messages from the topic for downstream Spark processing.
    """
    def __init__(self, bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS, topic: str = DEFAULT_TOPIC, group_id: str = "spark-quality-consumer-group"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.broker = KafkaStreamBroker(topic=self.topic)

    def consume_all(self, max_messages: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Consumes messages from Kafka topic.
        """
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers.split(','),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=self.group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=2000
            )
            messages = []
            for msg in consumer:
                messages.append({
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key.decode('utf-8') if msg.key else None,
                    "timestamp": msg.timestamp,
                    "value": msg.value
                })
                if max_messages and len(messages) >= max_messages:
                    break
            consumer.close()
            if messages:
                logger.info(f"Consumed {len(messages)} messages from live Kafka topic '{self.topic}'")
                return messages
        except Exception:
            pass

        # Fallback to local Kafka stream simulator
        messages = self.broker.read_messages(from_beginning=True, max_messages=max_messages)
        logger.info(f"Consumed {len(messages)} messages from Kafka stream buffer for topic '{self.topic}'")
        return messages

    def get_stats(self) -> Dict[str, Any]:
        """
        Returns stats about topic partitions, total messages, and byte size.
        """
        messages = self.broker.read_messages(from_beginning=True)
        partitions = {}
        total_bytes = 0
        for m in messages:
            p = m.get("partition", 0)
            partitions[p] = partitions.get(p, 0) + 1
            total_bytes += len(json.dumps(m))

        return {
            "topic": self.topic,
            "total_messages": len(messages),
            "partitions": partitions,
            "estimated_payload_bytes": total_bytes,
            "consumer_group": self.group_id,
            "latest_offset": max([m.get("offset", 0) for m in messages], default=0)
        }


def produce_raw_csv_to_kafka(csv_path: Optional[str] = None) -> Dict[str, Any]:
    """Convenience helper to run the producer against the default CSV."""
    if not csv_path:
        csv_path = os.path.join(DATA_DIR, "jobs_dataset.csv")
    producer = JobPostingKafkaProducer()
    return producer.ingest_csv(csv_path)


if __name__ == "__main__":
    res = produce_raw_csv_to_kafka()
    print(json.dumps(res, indent=2))
