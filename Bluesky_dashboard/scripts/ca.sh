#!/bin/bash

if [ -f ".env" ]; then
  while IFS='=' read -r key value; do
    if [[ ! $key =~ ^# && -n $key ]]; then
      export "$key=$value"
    fi
  done < .env
else
  echo ".env file not found!"
  exit 1
fi

if [ -z "${ES_PASSWORD}" ]; then
  echo "Set the ELASTIC_PASSWORD environment variable in the .env file"
  exit 1
elif [ -z "${KIBANA_PASSWORD}" ]; then
  echo "Set the KIBANA_PASSWORD environment variable in the .env file"
  exit 1
fi

if [ ! -f "${ES_CERTS_DIR}/ca.zip" ]; then
  echo "Creating CA"
  bin/elasticsearch-certutil ca --silent --pem -out "${ES_CERTS_DIR}/ca.zip"
  unzip "${ES_CERTS_DIR}/ca.zip" -d "${ES_CERTS_DIR}"
fi

if [ ! -f "${ES_CERTS_DIR}/certs.zip" ]; then
  echo "Creating certificates"

  cat  > "${ES_CERTS_DIR}/instances.yml" <<EOF
instances:
  - name: es
    dns:
      - elasticsearch
      - localhost
    ip:
      - 127.0.0.1
EOF

  bin/elasticsearch-certutil cert --silent --pem -out "${ES_CERTS_DIR}/certs.zip" \
    --in "${ES_CERTS_DIR}/instances.yml" \
    --ca-cert "${ES_CERTS_DIR}/ca/ca.crt" \
    --ca-key "${ES_CERTS_DIR}/ca/ca.key"
  unzip "${ES_CERTS_DIR}/certs.zip" -d "${ES_CERTS_DIR}"
fi

echo "Setting file permissions"
chown -R root:root "${ES_CERTS_DIR}"
find . -type d -exec chmod 750 {} \;
find . -type f -exec chmod 640 {} \;

echo "Waiting for Elasticsearch availability"
until curl -s --cacert ${ES_CERTS_DIR}/ca/ca.crt https://elasticsearch:${ES_PORT} | grep -q "missing authentication credentials"; do
  sleep 30
done

echo "Setting kibana_system password"
until curl -s -X POST --cacert ${ES_CERTS_DIR}/ca/ca.crt -u "${ES_USERNAME}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  "https://elasticsearch:${ES_PORT}/_security/user/kibana_system/_password" \
  -d "{\"password\":\"${KIBANA_PASSWORD}\"}" | grep -q "^{}"; do
  sleep 10
done

echo "All done!"