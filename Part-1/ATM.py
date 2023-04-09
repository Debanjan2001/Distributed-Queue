import sys, argparse
from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplCounter, ReplDict

server = "localhost"

class Switch_Case:
    def prompt_user_id(self, data, prompt="Enter user ID: "):
        user_id = input(prompt)
        if data.get(user_id) == None:
            print("User id not found. Creating new user...")
            data.set(user_id, 0, sync=True)
        return user_id

    def run(self, syncObj, data, choice):
        default = "Incorrect input"
        try:
            if int(choice) < 0 or int(choice) > 6:
                return default
        except Exception as error_message:
            print(default)
            return error_message

        return getattr(self, 'case_' + choice)(syncObj, data)
 
    def case_1(self, syncObj, data):
        amt = int(input("Enter amount to be withdrawn:"))

        user_id = self.prompt_user_id(data)
        if data.get(user_id) >= amt:
            data.set(user_id, data.get(user_id) - amt, sync=False)
            return "Success"
        
        print("Enough balance not found.")
        return "Failure"
 
    def case_2(self, syncObj, data):
        user_id = self.prompt_user_id(data)
        amt = int(input("Enter amount to deposit:"))
        data.set(user_id, data.get(user_id) + amt, sync=False)
        return "Success"
 
    def case_3(self, syncObj, data):
        user_id = self.prompt_user_id(data)
        balance = data.get(user_id)
        print(f"Balance = {balance}")
        return "Success"
    
    def case_4(self, syncObj, data):
        user_1 = self.prompt_user_id(data, "Enter user ID to transfer from:")
        amt = int(input("Enter amount to be transferred:"))

        if data.get(user_1) >= amt:
            user_2 = self.prompt_user_id(data, "Enter beneficiary user ID:")

            data.set(user_1, data.get(user_1) - amt, sync=False)
            data.set(user_2, data.get(user_2) + amt, sync=False)
            return "Success"
        
        print("Enough balance not found in user_1' .")
        return "Failure"
    
    def case_5(self, syncObj, data):
        new_node = input("Enter new node port:")
        syncObj.addNodeToCluster(server+":"+new_node)
        return "Success"
    
    def case_6(self, syncObj, data):
        node_to_remove = input("Enter node port to remove:")
        syncObj.removeNodeFromCluster(server+":"+node_to_remove)
        return "Success"

    def case_7(self, syncObj, data):
        sys.exit()

def ATM(args, cnt, data):
    other_nodes = []
    for i in args.seed_nodes:
        other_nodes.append(server+":"+i)

    return SyncObj(
        server+":"+args.self_addr, 
        other_nodes, 
        consumers=[cnt, data], 
        conf=SyncObjConf(dynamicMembershipChange=True)
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-addr", required=True, help="Address of this node")
    parser.add_argument("--seed-nodes", nargs="*", help="Addresses of seed nodes")
    args = parser.parse_args()

    cnt = ReplCounter()
    data = ReplDict()
    syncObj = ATM(args, cnt, data)

    while (True):
        print(25*"-")
        print("Enter the number corresponnding to your choice:")
        print("1. Withdrawal")
        print("2. Deposit")
        print("3. Balance inquiry")
        print("4. Transfer to other account")
        print("5. Add new node")
        print("6. Remove node")
        print("7. Exit")
        print(25*"-")
        ch = input("Please enter your choice:")

        switch = Switch_Case()
        print(switch.run(syncObj, data, ch))

if __name__ == '__main__':
    main()