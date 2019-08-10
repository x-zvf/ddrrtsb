# DDRRTSB DevRant RealTime Stats Bot

### xzvf no longer works on this, @ewpratten will hopefully continue it.

## Usage

0. Install docker, sqlalchemy, python-rq
1. unpack the db.tar.gz file into postgres/data
2. use docker-compose to start redis and postges.
3. Start workers. with `rq worker comments rants standard new`
4. Run actions from test.py

## DB

User: ddrrtsb
Pass: ddrrtsb
db:   drdata

Schema: see db.py

