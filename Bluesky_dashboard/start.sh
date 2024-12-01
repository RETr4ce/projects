#!/bin/bash
ES_PORT=$(awk -F '=' '/^ES_PORT=/{print $2}' .env)

docker compose up -d

echo "Waiting for Elasticsearch to be ready..."
until curl -s --cacert certs/es/es.crt https://localhost:${ES_PORT} > /dev/null; do
  sleep 5
  echo "Waiting for Elasticsearch..."
done

echo "Generating an Elasticsearch service token for Kibana..."
TOKEN=$(docker exec -it elasticsearch /usr/share/elasticsearch/bin/elasticsearch-service-tokens create elastic/kibana kibana | rg -oP '(?<= = )\S+')

if [ -z "$TOKEN" ]; then
    echo "Error generating the service token."
    exit 1
fi

echo "Service token generated: $TOKEN"

if [ ! -f .env ]; then
  echo "Error: .env file not found."
  exit 1
fi

sed -i '' "s/^ELASTICSEARCH_SERVICE_TOKEN=.*/ELASTICSEARCH_SERVICE_TOKEN=$TOKEN/" .env

echo "All services are up and running."