# Instructions

- First run the manager cluster
```
cd unit_tests/
python3 run_manager_cluster.py
```

- Now run the brokers
```
cd broker/
source setup_db.sh broker_1 && python3 run.py --flask_host 0.0.0.0:8000 --clear_db
source setup_db.sh broker_2 && python3 run.py --flask_host 0.0.0.0:8001 --clear_db
source setup_db.sh broker_3 && python3 run.py --flask_host 0.0.0.0:8002 --clear_db
```

- Now run the load balancer
```
cd load_balancer/
python3 load_balancer.py
```