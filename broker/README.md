# Broker


## Sample Commands to Run the broker first time
```
source setup_db.sh broker_1 && python3 run.py --flask_host 0.0.0.0:8000 --clear_db
source setup_db.sh broker_2 && python3 run.py --flask_host 0.0.0.0:8001 --clear_db
source setup_db.sh broker_3 && python3 run.py --flask_host 0.0.0.0:8002 --clear_db
```

## Sample Commands to Run the broker
```
source setup_db.sh broker_1 && python3 run.py --flask_host 0.0.0.0:8000
source setup_db.sh broker_2 && python3 run.py --flask_host 0.0.0.0:8001
source setup_db.sh broker_3 && python3 run.py --flask_host 0.0.0.0:8002
```

- Then use run.py to start the app
