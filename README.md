# Activity Monitoring

Flask service that reports real-time operator/station activity from the
production MySQL databases. One of the services behind the Manufacturing
Dashboard (`manufacturing_dashboard`), proxied there under `/activity`.

## Routes

- `GET /` — activity overview page
- `GET /operator/<operator_identifier>` — single-operator view
- `GET /api/operator_today` — today's activity for an operator (JSON)
- `GET /api/get-models-stations` — available models/stations (JSON)
- `GET /api/download_csv` — CSV export
- `GET /api/statistics` — aggregate statistics (JSON)
- `GET /health` — health check

## Configuration

Database connection is read entirely from the environment — there is no
hardcoded fallback, so the app will fail to start if any of these are
unset:

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DB_HOST` | yes | — | MySQL host |
| `DB_USER` | yes | — | MySQL user |
| `DB_PASSWORD` | yes | — | MySQL password |
| `DB_POOL_SIZE` | no | `15` | connection pool size |

Copy `.env.example` to `.env` and fill in real values before running —
`.env` is git-ignored and read automatically by `docker-compose.yml`.

Databases listed in `hidden_databases` (`config.py`) are excluded from
the per-database queries/listing (currently `smt`, `onanofflimited`).

## Running

```bash
cp .env.example .env   # fill in DB_HOST / DB_USER / DB_PASSWORD
docker compose up -d --build
```

Serves on port 5000. Requires the external Docker network
`manufacturing_network` to already exist (shared with the other
dashboard services):

```bash
docker network create manufacturing_network
```

## Notes

- Requires a `readonly_user`-style MySQL account with `SELECT` access
  across the production databases it reports on.
- See Odoo task 33623 for deployment history/context (migration to
  kts-dashboard CT 124, reading from the kms-web MySQL replica).
