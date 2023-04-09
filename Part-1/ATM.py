import sys, argparse
from pysyncobj import SyncObj
from pysyncobj.batteries import ReplCounter, ReplDict
from metadata import server, STATUS_FAILURE, STATUS_SUCCESS, STATUS_TERMINATED, status_msg

class ATM:
    def __init__(self, args):
        other_nodes = []
        for i in args.seed_nodes:
            other_nodes.append(server+":"+i)
        self.cnt = ReplCounter()
        self.data = ReplDict()
        self.syncobj = SyncObj(server+":"+args.self_addr, other_nodes, consumers=[self.cnt, self.data])

    def exit(self):
        self.syncobj.destroy()

class Switch_Case:
    def run(self, choice:int, atm:ATM):
        default = "Incorrect input"
        try:
            if choice < 0 or choices > 5:
                return default
        except Exception as error_message:
            print(default)
            return error_message

        return getattr(self, 'case_' + str(choice))(atm)
 
    def case_1(self, atm:ATM):
        user_id = input(">>> Enter A/C Number: ")
        if atm.data.get(user_id) == None:
            atm.data.set(user_id, 0, sync=True)

        amt = int(input(">>> Enter amount to be withdrawn: Rs."))

        if atm.data.get(user_id) >= amt:
            atm.data.set(user_id, atm.data.get(user_id) - amt, sync=True)
            return STATUS_SUCCESS
        return STATUS_FAILURE
 
    def case_2(self, atm:ATM):
        user_id = input(">>> Enter A/C Number: ")
        if atm.data.get(user_id) == None:
            atm.data.set(user_id, 0, sync=True)

        amt = int(input(">>> Enter amount to deposit: Rs."))
        atm.data.set(user_id, atm.data.get(user_id) + amt, sync=True)
        return STATUS_SUCCESS
 
    def case_3(self, atm:ATM):
        user_id = input(">>> Enter A/C Number: ")
        if atm.data.get(user_id) == None:
            atm.data.set(user_id, 0, sync=True)

        balance = atm.data.get(user_id)
        print(f"Balance Available: Rs.{balance}")
        return STATUS_SUCCESS
    
    def case_4(self, atm:ATM):
        user_id = input(">>> Enter A/C Number: ")
        if atm.data.get(user_id) == None:
            atm.data.set(user_id, 0, sync=True)

        amt = int(input(">>> Enter amount to be transferred: Rs."))

        if atm.data.get(user_id) >= amt:
            user_2 = input(">>> Enter beneficiary A/C Number: ")
            if atm.data.get(user_2) == None:
                atm.data.set(user_2, 0, sync=True)

            atm.data.set(user_id, atm.data.get(user_id) - amt, sync=True)
            atm.data.set(user_2, atm.data.get(user_2) + amt, sync=True)
            return STATUS_SUCCESS
        
        return STATUS_FAILURE

    def case_5(self, atm:ATM):
        atm.exit()
        return STATUS_TERMINATED

# def ATM(args, cnt, data):
#     other_nodes = []
#     for i in args.seed_nodes:
#         other_nodes.append(server+":"+i)
#     return SyncObj(server+":"+args.self_addr, other_nodes, consumers=[cnt, data])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-addr", required=True, help="Address of this node")
    parser.add_argument("--seed-nodes", nargs="*", help="Addresses of seed nodes")
    args = parser.parse_args()

    atm = ATM(args)

    while (True):
        print(36*'#')
        print("Please select a service:")
        print("1. Withdrawal")
        print("2. Deposit")
        print("3. Balance inquiry")
        print("4. Transfer to other account")
        print("5. Exit")
        print(36*"-")
        ch = int(input(">>> "))
        print(12*"-"+" processing "+(12*"-"))
        switch = Switch_Case()
        result = switch.run(ch, atm)
        print("<<< operation completion status :",status_msg[result],">>>")   
        print(36*"#","\n")
        if result == STATUS_TERMINATED :
            sys.exit(0)


if __name__ == '__main__':
    main()