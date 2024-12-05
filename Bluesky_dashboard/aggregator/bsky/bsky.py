import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosedError, InvalidStatus
from elasticsearch import AsyncElasticsearch
from dotenv import load_dotenv
import os

# === SETTINGS ===

# External WebSocket URLs
EXTERNAL_WS_URLS = [
    "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    "wss://jetstream1.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
]

# Language filter: Set desired languages (e.g., ["en", "nl", "unknown"]) or [] to allow all
LANGUAGE_FILTER: List[str] = []

# Elasticsearch settings
ELASTICSEARCH_HOST = "https://localhost:9200"
ELASTICSEARCH_INDEX = "bsky_feed_posts"

# Load environment variables from ../.env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
ELASTICSEARCH_SERVICE_TOKEN = os.getenv("ELASTICSEARCH_SERVICE_TOKEN")
ES_USERNAME = os.getenv("ES_USERNAME")
ES_PASSWORD = os.getenv("ES_PASSWORD")

SSL_CERT_PATH = os.path.join(os.path.dirname(__file__), '..', 'certs', 'es', 'es.crt')

# === DYNAMIC MAPPING FUNCTION ===

async def create_index_with_dynamic_mapping(es_client: AsyncElasticsearch, index_name: str) -> None:
    mapping = {
        "mappings": {
            "dynamic_templates": [
                {
                    "strings_as_keywords": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "keyword"
                        }
                    }
                }
            ],
            "properties": {
                "commit": {
                    "properties": {
                        "record": {
                            "properties": {
                                "text": {
                                    "type": "text"
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    exists = await es_client.indices.exists(index=index_name)
    if not exists:

        await es_client.indices.create(index=index_name, body=mapping)
        logging.info(f"Index '{index_name}' created with dynamic mapping.")
    else:
        logging.info(f"Index '{index_name}' already exists.")

# === WEBSOCKET CLIENT ===

class WebSocketClient:
    def __init__(self, urls: List[str], message_queue: asyncio.Queue) -> None:
        self.urls = urls
        self.message_queue = message_queue
        self.current_url_index = 0
        self.running = True

    async def run(self) -> None:
        retry_delay = 1
        while self.running:
            current_url = self.urls[self.current_url_index]
            try:
                await self.connect_and_listen(current_url)
                retry_delay = 1
            except (ConnectionClosedError, ConnectionError, OSError) as exc:
                logging.warning(f"Connection error with {current_url}: {exc}")
            except InvalidStatus as exc:
                logging.error(f"Invalid status for {current_url}: {exc}")
            except Exception as exc:
                logging.error(f"Unhandled exception with {current_url}: {exc}", exc_info=True)
            finally:
                self.current_url_index = (self.current_url_index + 1) % len(self.urls)
                delay = retry_delay + random.uniform(0, 1)
                logging.info(f"Retrying with {self.urls[self.current_url_index]} after {delay:.2f} seconds...")
                await asyncio.sleep(delay)
                retry_delay = min(retry_delay * 2, 60)

    async def connect_and_listen(self, url: str) -> None:
        logging.info(f"Connecting to WebSocket at {url}...")
        try:
            async with websockets.connect(url) as websocket:
                logging.info(f"Connected to WebSocket at {url}.")
                async for message in websocket:
                    await self.message_queue.put(message)
        except asyncio.TimeoutError:
            logging.error(f"Connection attempt to {url} timed out.")
        except Exception as exc:
            logging.error(f"Unhandled exception with {url}: {exc}", exc_info=True)

    async def stop(self) -> None:
        self.running = False

# === MESSAGE PROCESSOR ===

class MessageProcessor:
    def __init__(
        self,
        message_queue: asyncio.Queue,
        language_filter: Optional[List[str]] = None,
        session: Optional[aiohttp.ClientSession] = None,
        es_client: Optional[AsyncElasticsearch] = None,
    ) -> None:
        self.message_queue = message_queue
        self.language_filter = language_filter or []
        self.lock = asyncio.Lock()
        self.session = session or aiohttp.ClientSession()
        self.running = True
        self.es_client = es_client

    async def worker(self) -> None:
        while self.running:
            try:
                message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                try:
                    await self.handle_message(message)
                except Exception as exc:
                    logging.error(f"Error handling message: {exc}", exc_info=True)
                finally:
                    self.message_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                logging.info("Worker task cancelled. Exiting...")
                break

    async def handle_message(self, message: str) -> None:
        async with self.lock:
            try:
                data: Dict[str, Any] = json.loads(message)
                await self.process_message(data)
            except json.JSONDecodeError as exc:
                logging.error(f"Failed to decode JSON message: {exc}")

    async def process_message(self, data: Dict[str, Any]) -> None:
        commit = data.get("commit", {})
        record = commit.get("record", {})
        langs = record.get("langs", [])
        rkey = commit.get("rkey", "unknown")
        did = data.get("did", "unknown")

        if not self.is_language_allowed(langs):
            return

        timestamp = self.get_timestamp(data.get("time_us"))
        profile_url = f"https://bsky.app/profile/{did}/post/{rkey}"
        data["post_url"] = profile_url
        data["record_time"] = timestamp

        await self.index_data(data)

    def is_language_allowed(self, langs: List[str]) -> bool:
        if not self.language_filter:
            return True
        return any(lang in self.language_filter for lang in langs)

    async def index_data(self, data: Dict[str, Any]) -> None:
        if not self.es_client:
            logging.error("Elasticsearch client is not initialized.")
            return

        try:
            doc_id = data.get("commit", {}).get("cid", None)
            if not doc_id:
                doc_id = f"{data.get('did', '')}_{data.get('time_us', '')}"

            await self.es_client.index(
                index=ELASTICSEARCH_INDEX,
                id=doc_id,
                document=data
            )
            logging.info(f"Indexed document ID {doc_id}")
        except Exception as exc:
            logging.error(f"Failed to index document: {exc}", exc_info=True)

    @staticmethod
    def get_timestamp(time_us: Optional[int]) -> datetime:
        try:
            if time_us:
                return datetime.fromtimestamp(time_us / 1_000_000, tz=timezone.utc)
            return datetime.now(tz=timezone.utc)
        except (ValueError, TypeError) as exc:
            logging.warning(f"Invalid timestamp format: {time_us} ({exc})")
            return datetime.now(tz=timezone.utc)

    async def stop(self) -> None:
        self.running = False
        await self.session.close()

# === SHUTDOWN FUNCTION ===

async def shutdown(client: WebSocketClient, processor: MessageProcessor, tasks: List[asyncio.Task]) -> None:
    logging.info("Shutting down gracefully...")
    await client.stop()
    await processor.stop()
    for task in tasks:
        task.cancel()
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logging.info("Tasks successfully cancelled.")
    logging.info("Shutdown complete.")

# === MAIN FUNCTION ===

async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    message_queue = asyncio.Queue(maxsize=100000)
    client = WebSocketClient(urls=EXTERNAL_WS_URLS, message_queue=message_queue)

    es_client = AsyncElasticsearch(
        [ELASTICSEARCH_HOST],
        http_auth=(ES_USERNAME, ES_PASSWORD),
        ca_certs=SSL_CERT_PATH,
        verify_certs=True,
    )

    await create_index_with_dynamic_mapping(es_client, ELASTICSEARCH_INDEX)

    async with aiohttp.ClientSession() as session:
        processor = MessageProcessor(
            message_queue=message_queue,
            language_filter=LANGUAGE_FILTER,
            session=session,
            es_client=es_client
        )

        client_task = asyncio.create_task(client.run())
        processor_tasks = [asyncio.create_task(processor.worker()) for _ in range(10)]

        try:
            await asyncio.gather(client_task, *processor_tasks)
        except asyncio.CancelledError:
            logging.info("Main tasks cancelled.")
        finally:
            await shutdown(client, processor, [client_task] + processor_tasks)
            await es_client.close()

if __name__ == "__main__":
    asyncio.run(main())
