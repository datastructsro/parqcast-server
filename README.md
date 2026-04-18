# parqcast-server

HTTP Parquet receiver with UUID-split tree storage and API key authentication.

Receives raw Parquet files from [parqcast](https://github.com/datastructsro/parqcast) and stores them as-is in a date/UUID-partitioned directory tree:

```
{data_root}/{namespace}/{table}/{year}/{month}/{day}/{ab}/{cd}/{ef}/{gh}/{uuid}/data.parquet
```

## Dependencies

`parqcast-core` is not published to PyPI. Install it from the latest `main` of [datastructsro/parqcast](https://github.com/datastructsro/parqcast):

```bash
pip install "parqcast-core @ git+https://github.com/datastructsro/parqcast.git@main#subdirectory=packages/parqcast-core"
```

Or pin it via `[tool.uv.sources]` in your own `pyproject.toml`:

```toml
[tool.uv.sources]
parqcast-core = { git = "https://github.com/datastructsro/parqcast.git", subdirectory = "packages/parqcast-core", branch = "main" }
```

## Quick start

```bash
uv run uvicorn parqcast.server.app:app --host 0.0.0.0 --port 8420
```

Starts the server on port 8420.

## Configuration

The server reads `config.toml` from the working directory (or the path in `PARQCAST_CONFIG`):

```toml
[server]
data_root = "/tmp/parqcast"

[auth]
api_key = "your-secret-key"
```

Environment variables override the config file:

| Variable | Description | Default |
|---|---|---|
| `PARQCAST_DATA_ROOT` | Directory for stored files | `/var/parqcast` |
| `PARQCAST_API_KEY` | API key for authentication | _(none, auth disabled)_ |
| `PARQCAST_CONFIG` | Path to config file | `config.toml` |

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check (no auth) |
| `POST` | `/upload/{namespace}/{table}` | Upload a Parquet file |
| `POST` | `/upload/{namespace}/_manifest` | Upload a manifest JSON |
| `GET` | `/download/{path}` | Download a stored file |
| `GET` | `/browse/{path}` | Browse the directory tree |
| `GET` | `/read/{namespace}/{table}` | Read recent Parquet data as JSON |
| `GET` | `/docs` | OpenAPI docs (no auth) |

All endpoints except `/health`, `/docs`, and `/openapi.json` require an `X-API-Key` header when authentication is enabled.

## About

Built and maintained by **[DataStruct s.r.o.](https://datastruct.tech)** — an **[Odoo Official Partner](https://www.odoo.com/partners)** based in the Czech Republic, specialising in demand forecasting and ERP implementation for manufacturing, retail, and logistics in the Czech–German–Polish triangle.

- **See it in production:** [SAP→Odoo migration, 84% cost reduction](https://datastruct.tech/post/sap-to-odoo-vibecoded-migration)
- **Book a 30-min technical diagnostic:** [cal.com/oleg-popov-sjwko9/30min](https://cal.com/oleg-popov-sjwko9/30min)
- **Questions / partnership:** info@datastruct.tech
- **Companion:** [parqcast](https://github.com/datastructsro/parqcast) (the exporter that uploads here) · [foreqcast](https://github.com/datastructsro/foreqcast) · [parqcast-lambda](https://github.com/datastructsro/parqcast-lambda)

## License

Apache-2.0
