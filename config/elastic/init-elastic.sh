#!/bin/sh
set -eu

ES_URL="${ELASTICSEARCH_URL:-http://elasticsearch:9200}"
KB_URL="${KIBANA_URL:-http://kibana:5601}"
ES_USER="${ELASTIC_ADMIN_USERNAME:-elastic}"
ES_PASS="${ELASTIC_ADMIN_PASSWORD:?ELASTIC_ADMIN_PASSWORD is required}"
INGEST_USER="${ELASTIC_INGEST_USERNAME:-fandogh_ingest}"
INGEST_PASS="${ELASTIC_INGEST_PASSWORD:?ELASTIC_INGEST_PASSWORD is required}"
VIEW_USER="${KIBANA_VIEWER_USERNAME:-fandogh_observer}"
VIEW_PASS="${KIBANA_VIEWER_PASSWORD:?KIBANA_VIEWER_PASSWORD is required}"
KIBANA_SYSTEM_PASS="${KIBANA_SYSTEM_PASSWORD:?KIBANA_SYSTEM_PASSWORD is required}"

echo "Waiting for Elasticsearch..."
until curl -s -u "${ES_USER}:${ES_PASS}" "${ES_URL}/_cluster/health" >/dev/null 2>&1; do
  sleep 3
done

echo "Setting kibana_system password..."
while true; do
  code="$(curl -s -o /tmp/kibana_system_pw.json -w "%{http_code}" \
    -u "${ES_USER}:${ES_PASS}" \
    -X POST "${ES_URL}/_security/user/kibana_system/_password" \
    -H "Content-Type: application/json" \
    -d "{
      \"password\": \"${KIBANA_SYSTEM_PASS}\"
    }")"
  if [ "${code}" = "200" ]; then
    break
  fi
  sleep 3
done

echo "Waiting for Kibana..."
until curl -s "${KB_URL}/api/status" >/dev/null 2>&1; do
  sleep 3
done

echo "Creating Elasticsearch roles/users..."
curl -s -u "${ES_USER}:${ES_PASS}" -X PUT "${ES_URL}/_security/role/fandogh_log_ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "cluster": ["monitor"],
    "indices": [
      {
        "names": ["fandogh-logs-*"],
        "privileges": ["auto_configure", "create_doc", "create_index", "write", "view_index_metadata"]
      }
    ]
  }' >/dev/null

curl -s -u "${ES_USER}:${ES_PASS}" -X PUT "${ES_URL}/_security/role/fandogh_logs_reader" \
  -H "Content-Type: application/json" \
  -d '{
    "cluster": ["monitor"],
    "indices": [
      {
        "names": ["fandogh-logs-*"],
        "privileges": ["read", "view_index_metadata"]
      }
    ]
  }' >/dev/null

curl -s -u "${ES_USER}:${ES_PASS}" -X POST "${ES_URL}/_security/user/${INGEST_USER}" \
  -H "Content-Type: application/json" \
  -d "{
    \"password\": \"${INGEST_PASS}\",
    \"roles\": [\"fandogh_log_ingest\"],
    \"full_name\": \"Fandogh Log Ingest\"
  }" >/dev/null

curl -s -u "${ES_USER}:${ES_PASS}" -X POST "${ES_URL}/_security/user/${VIEW_USER}" \
  -H "Content-Type: application/json" \
  -d "{
    \"password\": \"${VIEW_PASS}\",
    \"roles\": [\"kibana_admin\", \"fandogh_logs_reader\"],
    \"full_name\": \"Fandogh Kibana Viewer\"
  }" >/dev/null

echo "Creating index template..."
curl -s -u "${ES_USER}:${ES_PASS}" -X PUT "${ES_URL}/_ilm/policy/fandogh-logs-30d" \
  -H "Content-Type: application/json" \
  -d '{
    "policy": {
      "phases": {
        "hot": {
          "actions": {}
        },
        "delete": {
          "min_age": "30d",
          "actions": {
            "delete": {}
          }
        }
      }
    }
  }' >/dev/null

curl -s -u "${ES_USER}:${ES_PASS}" -X PUT "${ES_URL}/_index_template/fandogh-logs-template" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["fandogh-logs-*"],
    "template": {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.lifecycle.name": "fandogh-logs-30d"
      },
      "mappings": {
        "dynamic": true,
        "properties": {
          "@timestamp": {"type": "date"},
          "ts": {"type": "date"},
          "event": {"type": "keyword"},
          "platform": {"type": "keyword"},
          "level": {"type": "keyword"},
          "logger": {"type": "keyword"},
          "service_name": {"type": "keyword"},
          "user_id": {"type": "keyword"},
          "chat_id": {"type": "keyword"},
          "status": {"type": "keyword"},
          "error": {"type": "text"},
          "message": {"type": "text"}
        }
      }
    }
  }' >/dev/null

echo "Creating Kibana data view..."
curl -s -u "${ES_USER}:${ES_PASS}" -X POST "${KB_URL}/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "id": "fandogh-logs-dataview",
      "title": "fandogh-logs-*",
      "name": "Fandogh Logs",
      "timeFieldName": "@timestamp"
    }
  }' >/dev/null || true

echo "Creating Kibana saved searches..."
curl -s -u "${ES_USER}:${ES_PASS}" -X POST "${KB_URL}/api/saved_objects/search/fandogh-user-activity?overwrite=true" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "attributes": {
      "title": "Fandogh - User Activity Stream",
      "description": "Stream of user behavior and business events",
      "columns": ["@timestamp", "event", "platform", "user_id", "status", "message", "error"],
      "sort": [["@timestamp", "desc"]],
      "kibanaSavedObjectMeta": {
        "searchSourceJSON": "{\"query\":{\"language\":\"kuery\",\"query\":\"service_name : \\\"fandogh-bridge\\\"\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
      }
    },
    "references": [
      {
        "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
        "type": "index-pattern",
        "id": "fandogh-logs-dataview"
      }
    ]
  }' >/dev/null

curl -s -u "${ES_USER}:${ES_PASS}" -X POST "${KB_URL}/api/saved_objects/search/fandogh-delivery-errors?overwrite=true" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "attributes": {
      "title": "Fandogh - Delivery Failures",
      "description": "Only failed/expired/retry delivery events",
      "columns": ["@timestamp", "event", "platform", "user_id", "status", "error", "message"],
      "sort": [["@timestamp", "desc"]],
      "kibanaSavedObjectMeta": {
        "searchSourceJSON": "{\"query\":{\"language\":\"kuery\",\"query\":\"service_name : \\\"fandogh-bridge\\\" and status : (FAILED or EXPIRED or RETRY)\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
      }
    },
    "references": [
      {
        "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
        "type": "index-pattern",
        "id": "fandogh-logs-dataview"
      }
    ]
  }' >/dev/null

echo "Creating Kibana dashboard..."
curl -s -u "${ES_USER}:${ES_PASS}" -X POST "${KB_URL}/api/saved_objects/dashboard/fandogh-user-activity-dashboard?overwrite=true" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "attributes": {
      "title": "Fandogh - User Activity Dashboard",
      "description": "Operational dashboard for user actions, delivery outcomes, and failures",
      "hits": 0,
      "timeRestore": false,
      "optionsJSON": "{\"useMargins\":true,\"syncColors\":false,\"syncCursor\":true,\"syncTooltips\":true}",
      "panelsJSON": "[{\"type\":\"search\",\"gridData\":{\"x\":0,\"y\":0,\"w\":48,\"h\":20,\"i\":\"1\"},\"panelIndex\":\"1\",\"panelRefName\":\"panel_0\",\"version\":\"8.14.3\",\"embeddableConfig\":{},\"title\":\"User Activity Stream\"},{\"type\":\"search\",\"gridData\":{\"x\":0,\"y\":20,\"w\":48,\"h\":16,\"i\":\"2\"},\"panelIndex\":\"2\",\"panelRefName\":\"panel_1\",\"version\":\"8.14.3\",\"embeddableConfig\":{},\"title\":\"Delivery Failures / Retries\"}]",
      "kibanaSavedObjectMeta": {
        "searchSourceJSON": "{\"query\":{\"language\":\"kuery\",\"query\":\"service_name : \\\"fandogh-bridge\\\"\"},\"filter\":[]}"
      }
    },
    "references": [
      {"name": "panel_0", "type": "search", "id": "fandogh-user-activity"},
      {"name": "panel_1", "type": "search", "id": "fandogh-delivery-errors"}
    ]
  }' >/dev/null

echo "Elastic/Kibana bootstrap completed."
