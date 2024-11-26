A webserver to evaluate cost of cargo insurance.

# Deployment

## Preparing local environment

1. Install [Python 3.12.7](https://www.python.org/downloads/release/python-3127/)
   or higher version of Python 3.12.
2. Open terminal in the root directory of this repository.
3. Initialize virtual environment inside `.venv` directory and activate it according to the
   [tutorial](https://docs.python.org/3/library/venv.html).
4. Update dependencies.
   ```
   python -m pip install -U pip setuptools wheel pip-tools
   ```
5. Generate `requirements.txt` from `requirements.in`.
   ```
   python -m piptools compile -U --strip-extras
   ```
6. Install all necessary packages.
   ```
   python -m pip install -r requirements.txt --no-deps
   ```

## Run service locally

Create an empty file and name it `local.env`.

### Database

If there is no database deployed, you can deploy PostgreSQL 17 locally via Docker.

```bash
docker run -d --name postgresql -p5432:5432 -v $HOME/postgresql:/var/lib/postgresql/data -e POSTGRES_PASSWORD=mysecretpassword postgres:17
```

Once it is running, add lines below to `local.env`.

```
DATABASE_USER=postgres
DATABASE_PASSWORD=mysecretpassword
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_DBNAME=postgres
```

### Kafka

If there is no broker deployed, you can deploy Redpanda 24.2 locally via Docker
following this [guide](https://docs.redpanda.com/current/get-started/quick-start).

Once it is running, add lines below to file `local.env`.

```
KAFKA_SERVERS=localhost:9092
KAFKA_TOPIC=crud_logs
```

### Web server

Run `python start_server.py` to start the server locally.

## Run service via Docker

Create an empty file called `.env` and fill it as follows:

```
DATABASE_USER=<Desired user name>
DATABASE_PASSWORD=<User password>
DATABASE_HOST=db
DATABASE_PORT=5432
DATABASE_DBNAME=<Desired starting db name>

KAFKA_SERVERS=redpanda:9092
KAFKA_TOPIC=crud_logs
```

Then run `docker compose up -d` to start all the services.

# Usage

After the server is deployed, open `http://<server_host>:2127/docs` in browser
to view available endpoints and documentation.
To access documentation on locally deployed server, open http://localhost:2127/docs.
To view messages sent to locally deployed Kafka, open http://localhost:8080/topics/crud_logs.
