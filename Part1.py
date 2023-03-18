import sys, argparse
from pysyncobj import SyncObj
from pysyncobj.batteries import ReplCounter, ReplDict

server = "localhost"

class Switch_Case:
    def run(self, data, choice):
        default = "Incorrect input"
        try:
            if int(choice) < 0 or int(choice) > 5:
                return default
        except Exception as error_message:
            print(default)
            return error_message

        user_id = input("Enter user ID:")
        if data.get(user_id) == None:
            data.set(user_id, 0, sync=True)

        return getattr(self, 'case_' + choice)(data, user_id)
 
    def case_1(self, data, user_id):
        amt = int(input("Enter amount to be withdrawn:"))

        if data.get(user_id) >= amt:
            data.set(user_id, data.get(user_id) - amt, sync=True)
            return "Success"
        return "Failure"
 
    def case_2(self, data, user_id):
        amt = int(input("Enter amount to deposit:"))
        data.set(user_id, data.get(user_id) + amt, sync=True)
        return "Success"
 
    def case_3(self, data, user_id):
        return data.get(user_id)
    
    def case_4(self, data, user_id):
        amt = int(input("Enter amount to be transferred:"))

        if data.get(user_id) >= amt:
            user_2 = input("Enter beneficiary user ID:")
            if data.get(user_2) == None:
                data.set(user_2, 0, sync=True)

            data.set(user_id, data.get(user_id) - amt, sync=True)
            data.set(user_2, data.get(user_2) + amt, sync=True)
            return "Success"
        
        return "Failure"

    def case_5(self, data, user_id):
        sys.exit()

def ATM(args, cnt, data):
    other_nodes = []
    for i in args.seed_nodes:
        other_nodes.append(server+":"+i)
    return SyncObj(server+":"+args.self_addr, other_nodes, consumers=[cnt, data])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-addr", required=True, help="Address of this node")
    parser.add_argument("--seed-nodes", nargs="*", help="Addresses of seed nodes")
    args = parser.parse_args()

    cnt = ReplCounter()
    data = ReplDict()
    syncObj = ATM(args, cnt, data)

    while (True):
        print("Enter the number corresponnding to your choice:")
        print("1. Withdrawal")
        print("2. Deposit")
        print("3. Balance inquiry")
        print("4. Transfer to other account")
        print("5. Exit")
        ch = input()

        switch = Switch_Case()
        print(switch.run(data, ch))

if __name__ == '__main__':
    main()