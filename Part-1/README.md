## Steps to run the code
```
python3 ATM.py --self-addr 7000 --seed-nodes 7001 7002
python3 ATM.py --self-addr 7001 --seed-nodes 7002 7000
python3 ATM.py --self-addr 7002 --seed-nodes 7000 7001
```

- Add new node
```
python3 ATM.py --self-addr 7003 --seed-nodes 7000
```