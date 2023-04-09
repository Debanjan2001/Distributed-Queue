import sys, argparse
from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplCounter, ReplDict
from metadata import server, STATUS_FAILURE, STATUS_SUCCESS, STATUS_TERMINATED, STATUS_INVALID, status_msg


class ATM:
    def __init__(self, args):
        other_nodes = []
        for i in args.seed_nodes:
            other_nodes.append(server+":"+i)
        self.cnt = ReplCounter()
        self.data = ReplDict()
        self.conf = SyncObjConf(dynamicMembershipChange=True)
        self.syncobj = SyncObj(server+":"+args.self_addr, other_nodes, consumers=[self.cnt, self.data], conf=self.conf)

    def exit(self):
        self.syncobj.destroy()


class Switch_Case:
    def run(self, choice:int, atm:ATM):
        default = "Incorrect input"
        try:
            if choice <= 0 or choice > 7:
                print(default)
                return STATUS_INVALID
        except Exception as error_message:
            print(default)
            return STATUS_INVALID

        return getattr(self, 'case_' + str(choice))(atm)
 
    def case_1(self, atm:ATM):
        user_id = input("Enter A/C Number: ")
        if atm.data.get(user_id) == None:
            atm.data.set(user_id, 0, sync=True)

        amt = int(input("Enter amount to be withdrawn: Rs."))

        if atm.data.get(user_id) >= amt:
            atm.data.set(user_id, atm.data.get(user_id) - amt, sync=True)
            return STATUS_SUCCESS
        else:
            print("Insufficient funds.")
            return STATUS_FAILURE
 
    def case_2(self, atm:ATM):
        user_id = input("Enter A/C Number: ")
        if atm.data.get(user_id) == None:
            atm.data.set(user_id, 0, sync=True)

        amt = int(input("Enter amount to deposit: Rs."))
        atm.data.set(user_id, atm.data.get(user_id) + amt, sync=True)
        return STATUS_SUCCESS
 
    def case_3(self, atm:ATM):
        user_id = input("Enter A/C Number: ")
        if atm.data.get(user_id) == None:
            atm.data.set(user_id, 0, sync=True)

        balance = atm.data.get(user_id)
        print(f"Balance Available: Rs.{balance}")
        return STATUS_SUCCESS
    
    def case_4(self, atm:ATM):
        user_id = input("Enter A/C Number: ")
        if atm.data.get(user_id) == None:
            atm.data.set(user_id, 0, sync=True)

        amt = int(input("Enter amount to be transferred: Rs."))

        if atm.data.get(user_id) >= amt:
            user_2 = input("Enter beneficiary A/C Number: ")
            if atm.data.get(user_2) == None:
                atm.data.set(user_2, 0, sync=True)

            atm.data.set(user_id, atm.data.get(user_id) - amt, sync=True)
            atm.data.set(user_2, atm.data.get(user_2) + amt, sync=True)
            return STATUS_SUCCESS
        else:
            print("Insufficient funds.")
            return STATUS_FAILURE

    def case_5(self, atm:ATM):
        new_node = input("Enter new node port: ")
        try:
            atm.syncobj.addNodeToCluster(server+":"+new_node)
            return STATUS_SUCCESS
        except Exception as _:
            print(_)
            return STATUS_FAILURE
        
    def case_6(self, atm:ATM):
        node_to_remove = input("Enter node port to remove: ")
        try:
            atm.syncobj.removeNodeFromCluster(server+":"+node_to_remove)
            return STATUS_SUCCESS
        except Exception as _:
            print(_)
            return STATUS_FAILURE
        
    def case_7(self, atm:ATM):
        atm.exit()
        return STATUS_TERMINATED


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
        print("5. Add new node")
        print("6. Remove node")
        print("7. Exit")
        print(36*"-")
        try:
            ch = int(input(">>> "))
            print(12*"-"+" processing "+(12*"-"))
            switch = Switch_Case()
            result = switch.run(ch, atm)
            print("<<< operation completion status :",status_msg[result],">>>")   
            print(36*"#")
            print()
            if result == STATUS_TERMINATED :
                sys.exit(0)
        except Exception as e:
            print(f"Unknown exception: {e}")
            print("Please try again.")
            print()
            continue

if __name__ == '__main__':
    main()