import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Set

from elasticsearch import Elasticsearch, helpers

# === SETTINGS ===
ELASTICSEARCH_HOSTS = ["https://localhost:9200"]
ELASTICSEARCH_VERIFY_CERTS = True
ELASTICSEARCH_CA_CERTS = "ca.crt"
ELASTICSEARCH_USER = "elastic"
ELASTICSEARCH_PASSWORD = "changeme"
INDEX_NAME = "bsky_feed_posts"  # Replace this with your index name
STOPWORDS_FILE = "STOP.txt"
LOG_FILE = "elasticsearch_update.log"
LOG_LEVEL = logging.INFO
BATCH_SIZE = 500

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)

def load_stopwords(file_path: str) -> Set[str]:
    with open(file_path, "r", encoding="utf-8") as f:
        return {word.strip().lower() for word in f}

stop_words: Set[str] = load_stopwords(STOPWORDS_FILE)

def create_es_client() -> Elasticsearch:
    return Elasticsearch(
        hosts=ELASTICSEARCH_HOSTS,
        verify_certs=ELASTICSEARCH_VERIFY_CERTS,
        ca_certs=ELASTICSEARCH_CA_CERTS,
        basic_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
    )

es: Elasticsearch = create_es_client()

def clean_text(text: str, current_year: int) -> str:

    text = text.lower()
    text = re.sub(r"http\S+|www\.\S+", "", text)
    text = re.sub(r"[^\w\s,]", "", text, flags=re.UNICODE)
    text = re.sub(r"[^\w\s]", " ", text)
    year_range = range(current_year - 2, current_year + 3)
    years_pattern = r"\b(?!\b" + r"\b|\b".join(map(str, year_range)) + r"\b)\d+\b"
    text = re.sub(years_pattern, "", text)

    return text

def process_text(text: str) -> List[str]:
    current_year = datetime.now().year
    cleaned_text = clean_text(text, current_year)
    words = re.findall(r"\b\w+\b", cleaned_text)
    filtered_words = [word for word in words if word not in stop_words]

    return filtered_words

def update_records(index_name: str) -> None:
    query = {
        "query": {
            "match": {
                "commit.record.langs": "nl",
            }
        }
    }

    try:
        results = helpers.scan(es, index=index_name, query=query)
    except Exception as e:
        logging.error(f"Error fetching records from Elasticsearch: {e}")
        return

    actions = []
    total_processed = 0
    total_updated = 0

    logging.info(f"Starting processing for index: {index_name}")

    for record in results:
        record_id = record.get("_id")
        record_body = record.get("_source", {})

        text = record_body.get("commit", {}).get("record", {}).get("text", "")
        if not text:
            logging.warning(f"Record with ID {record_id} has no text. Skipping.")
            continue

        new_wordcloud = process_text(text)
        existing_wordcloud = record_body.get("wordcloud", [])

        if new_wordcloud == existing_wordcloud:
            total_processed += 1
            continue

        action = {
            "_op_type": "update",
            "_index": index_name,
            "_id": record_id,
            "doc": {"wordcloud": new_wordcloud},
        }
        actions.append(action)

        total_processed += 1
        total_updated += 1

        if total_processed % 100 == 0:
            logging.info(f"{total_processed} records processed...")

        if len(actions) >= BATCH_SIZE:
            try:
                helpers.bulk(es, actions)
                logging.info(f"Batch of {len(actions)} records updated.")
            except Exception as e:
                logging.error(f"Error updating records: {e}")
            finally:
                actions.clear()

    if actions:
        try:
            helpers.bulk(es, actions)
            logging.info(f"Last batch of {len(actions)} records updated.")
        except Exception as e:
            logging.error(f"Error updating last batch of records: {e}")

    logging.info(f"Processing completed: {total_processed} records processed, {total_updated} records updated.")

if __name__ == "__main__":
    try:
        update_records(INDEX_NAME)
    except Exception as e:
        logging.error(f"Error during processing: {e}")