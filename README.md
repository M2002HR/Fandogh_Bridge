# Fandogh Bridge

**An asynchronous two-way messaging bridge between Telegram and Bale with contacts, delivery retries, usage credits, payments, and centralized observability.**

Fandogh Bridge provides a shared button-driven user experience across both platforms. Users can register, discover or save contacts, exchange supported media, track message delivery, purchase usage packages, and interact with support workflows.

## Engineering highlights

- Asynchronous Python service built around `asyncio`
- Unified conversational UI for Telegram and Bale
- Two-way relay for text, photo, and voice messages
- Contact discovery, saved contacts, blocking, and paginated management
- Delivery acknowledgement and retry queue with backoff
- MySQL 8 operational database with migration support from SQLite
- JSON-configured product catalog and usage-credit accounting
- Multiple payment workflows, including platform payments and manually reviewed receipts
- Centralized JSON logging through Fluent Bit, Elasticsearch, and Kibana
- Docker Compose stack with health-oriented operational tooling
- pytest-based verification

## Technology

`Python` · `asyncio` · `Telegram Bot API` · `Bale Bot API` · `MySQL` · `SQLAlchemy` · `Redis` · `Docker Compose` · `Elasticsearch` · `Kibana` · `Fluent Bit` · `pytest`

## Architecture

```text
Telegram user                         Bale user
      │                                   │
      └──────────► Bridge service ◄───────┘
                         │
              registration and contacts
                         │
                routing and retry queue
                         │
            usage credits and payments
                         │
                         ▼
                       MySQL
                         │
                         ├── phpMyAdmin
                         └── structured logs
                                  │
                                  ▼
                       Fluent Bit → Elasticsearch → Kibana
```

## Main capabilities

### Registration and identity

- staged rules acceptance and phone registration
- platform-aware user identity
- contact lookup by Fandogh ID, phone number, or username
- notification when a previously unavailable contact completes registration

### Messaging

- two-way text, image, and voice relay
- reply/connect actions attached to received messages
- seen acknowledgement sent back to the original sender
- retry queue with bounded backoff for transient failures

### Contact management

- save contacts under custom names
- connect, block, unblock, inspect, or delete
- paginated contact lists
- admin-assisted requests for users who have not registered

### Credits and payments

- JSON-defined packages and usage rules
- remaining-credit display
- purchase history and payment state
- support for Telegram Stars, Crypto Pay/TON, Bale wallet invoices, and manually reviewed payment receipts when configured
- package gifting to another registered user

All payment methods are configuration-gated. Unconfigured methods remain unavailable in the UI.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Configure platform tokens, database credentials, administrator IDs, and the sales catalog, then run locally:

```bash
python -m bridge.main
```

Development auto-reload:

```bash
python -m bridge.dev
```

Run the complete stack:

```bash
docker compose up --build -d
```

## Service endpoints

Default local service ports are configurable. The example Compose stack exposes:

- phpMyAdmin for database inspection
- Elasticsearch API
- Kibana with a prepared activity dashboard

Do not expose administrative services publicly without access controls and network restrictions.

## Configuration

Runtime configuration belongs in `.env`; non-secret package definitions belong in `config/sales_catalog.json`.

Configuration groups include:

- Telegram and Bale tokens and allowed update types
- administrator identities and support destinations
- MySQL and Redis connectivity
- media limits and temporary storage
- retry timing and worker behavior
- usage-credit rules and package prices
- payment-provider credentials and feature switches
- structured logging, retention, and audit behavior
- Elasticsearch and Kibana credentials
- development watcher behavior

Secrets, receipts, phone numbers, and message content must not be committed.

## Database and migration

The runtime database is MySQL 8. Existing SQLite data can be migrated with the provided script:

```bash
PYTHONPATH=src python scripts/migrate_sqlite_to_mysql.py \
  --sqlite ./app_data/bridge.db
```

Review row counts and application behavior after migration before retiring the source database.

## Observability

The stack supports:

- structured JSON application logs
- centralized ingestion with Fluent Bit
- searchable logs and operational events in Elasticsearch
- Kibana dashboards for user activity and delivery failures
- configurable retention and audit-event capture

Raw network logs and credentials should remain redacted. Full message-text capture should only be enabled when required and handled as sensitive data.

## Verification

```bash
source .venv/bin/activate
pytest -q
docker compose config --quiet
```

Platform payment and messaging flows require separately approved integration tests with non-production identities or controlled test accounts.

## Security notes

- Keep tokens, payment credentials, database passwords, and platform sessions in `.env`.
- Restrict phpMyAdmin, Elasticsearch, and Kibana to trusted networks.
- Validate all callback payloads and platform identifiers.
- Treat phone numbers, receipts, messages, and audit records as sensitive personal data.
- Use idempotent payment and credit updates to prevent duplicate charging or balance changes.

## Project status

Fandogh Bridge is a multi-service messaging and commerce backend that demonstrates asynchronous integration, cross-platform UX, persistent delivery, relational data modeling, payment workflows, containerized infrastructure, and centralized observability.