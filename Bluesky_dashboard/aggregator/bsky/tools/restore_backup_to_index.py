import os
import gzip
import json
from elasticsearch import Elasticsearch, helpers
import logging

# === SETTINGS ===

# Elasticsearch configuration
ELASTICSEARCH_HOSTS = ["https://localhost:9200"]
ELASTICSEARCH_VERIFY_CERTS = True
ELASTICSEARCH_CA_CERTS = "ca.crt"
ELASTICSEARCH_USER = "elastic"
ELASTICSEARCH_PASSWORD = "changeme"
INDEX_NAME = "bsky_feed_posts"
BACKUP_FILE = "elasticsearch_backups/bsky_feed_posts_backup_20241207_212338.json.gz"
LOG_FILE = "restore_log.log"

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

def restore_from_backup(es_client, index_name, backup_file):
    if not os.path.exists(backup_file):
        logging.error(f"Backup file '{backup_file}' does not exist.")
        return

    logging.info(f"Restoring data from '{backup_file}' into index '{index_name}'...")
    
    with gzip.open(backup_file, "rt", encoding="utf-8") as f:
        documents = []
        for line in f:
            doc = json.loads(line)
            source = doc["_source"]
            doc_id = doc["_id"] if "_id" in doc else None
            action = {
                "_index": index_name,
                "_source": source,
            }
            if doc_id:
                action["_id"] = doc_id
            documents.append(action)
            
            if len(documents) >= 1000:
                helpers.bulk(es_client, documents)
                logging.info(f"Uploaded {len(documents)} documents...")
                documents.clear()
        
        if documents:
            helpers.bulk(es_client, documents)
            logging.info(f"Uploaded final {len(documents)} documents.")

    logging.info(f"Data restoration to index '{index_name}' completed successfully.")

def main():
    logging.info("Starting Elasticsearch restore process...")
    
    es_client = create_es_client()
    
    try:
        restore_from_backup(es_client, INDEX_NAME, BACKUP_FILE)
    except Exception as e:
        logging.error(f"Error during restore: {e}")
    finally:
        es_client.close()
    
    logging.info("Restore process completed.")

if __name__ == "__main__":
    main()
