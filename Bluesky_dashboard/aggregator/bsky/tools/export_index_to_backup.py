import os
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import gzip
import logging

# === SETTINGS ===

# Configuration settings
ELASTICSEARCH_HOSTS = ["https://localhost:9200"]
ELASTICSEARCH_VERIFY_CERTS = True
ELASTICSEARCH_CA_CERTS = "ca.crt"
ELASTICSEARCH_USER = "elastic"
ELASTICSEARCH_PASSWORD = "changeme"
INDEX_NAME = "bsky_feed_posts"
BACKUP_DIR = "elasticsearch_backups"
LOG_FILE = "backup_log.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
logging.getLogger('elasticsearch').setLevel(logging.WARNING)

def create_es_client():
    return Elasticsearch(
        hosts=ELASTICSEARCH_HOSTS,
        verify_certs=ELASTICSEARCH_VERIFY_CERTS,
        ca_certs=ELASTICSEARCH_CA_CERTS,
        http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
    )

def ensure_backup_directory(directory):
    os.makedirs(directory, exist_ok=True)

def create_compressed_backup(es_client, index_name, backup_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(backup_dir, f"{index_name}_backup_{timestamp}.json.gz")
    
    logging.info(f"Fetching data from index '{index_name}'...")
    results = scan(es_client, index=index_name)
    
    total_docs = es_client.count(index=index_name)["count"]
    logging.info(f"Total documents to backup: {total_docs}")
    
    logging.info(f"Writing backup to compressed file '{backup_file}'...")
    count = 0
    with gzip.open(backup_file, "wt", encoding="utf-8") as f:
        for doc in results:
            json.dump(doc, f)
            f.write("\n")
            count += 1
            if count % 100000 == 0 or count == total_docs:
                logging.info(f"Backed up {count}/{total_docs} documents...")
    
    logging.info(f"Backup completed successfully: {backup_file}")
    return backup_file

def main():
    logging.info("Starting Elasticsearch backup process...")
    
    es_client = create_es_client()
    ensure_backup_directory(BACKUP_DIR)
    
    try:
        create_compressed_backup(es_client, INDEX_NAME, BACKUP_DIR)
    except Exception as e:
        logging.error(f"Error during backup: {e}")
    finally:
        es_client.close()
    
    logging.info("Backup process completed.")

if __name__ == "__main__":
    main()
