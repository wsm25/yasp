import sys
import re


class node_info:
    def __init__(self, id: int):
        self.id = id
        self.proposals: list[str] = []
        self.commits: dict[int, str] = {}
        self.maxSeq = 0
        self.read_from_log(id)

    def read_from_log(self, id: int):
        filename = "../log/instancelog" + str(id) + ".log"
        propose_pattern = re.compile(
            r"generate Instance\[(.*?)\] in seq (\d+) at (\d+)"
        )
        commit_pattern = re.compile(r"commit Instance\[(.*?)\] in seq (\d+) at (\d+)")
        with open(filename, "r") as f:
            lines = f.readlines()
            for line in lines:
                m1 = propose_pattern.search(line)
                m2 = commit_pattern.search(line)
                if m1 != None:
                    self.proposals.append(m1.group(1))
                if m2 != None:
                    if int(m2.group(2)) not in self.commits.keys():
                        self.maxSeq = max(self.maxSeq, int(m2.group(2)))
                        self.commits[int(m2.group(2))] = m2.group(1)
                    else:
                        print(f"node {id} commited {m2.group(2)} more than once")
                        sys.exit(-1)

            print(f"node {id} commits {len(self.commits.keys())} times")


def check_safety(nodes: list[node_info]):
    n = len(nodes)
    for i in range(n):
        for j in range(n):
            if i < j:
                commits1 = nodes[i].commits
                commits2 = nodes[j].commits
                k = 0
                for k in range(min(nodes[i].maxSeq, nodes[j].maxSeq)):
                    instance1 = None
                    if k in commits1.keys():
                        instance1 = commits1[k]
                    instance2 = None
                    if k in commits2.keys():
                        instance2 = commits2[k]
                    if instance1 != instance2:
                        print(
                            f"node {i} has different commit with node {j} at position {k}, node {i} is {instance1} while node {j} is {instance2}"
                        )
                        return False
    return True


def check_validity(nodes: list[node_info]):
    all_proposals = []
    all_commits = []
    for node in nodes:
        if len(node.commits) == 0:
            print(f"node {node.id} have empty commit")
            print("Hint: you may set greater test time")
            return False

        all_proposals += node.proposals
        all_commits += node.commits.values()
    for commit in all_commits:
        if commit not in all_proposals:
            print(f"commit {commit} has never been proposed!")
            return False
    return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 check.py [node number]")
        print("\t node number: total node number \n")
        sys.exit()
    n = 0
    try:
        n = int(sys.argv[1])
        crash_nodes = []
    except:
        print("The parameter must be an integer")
        sys.exit()
    print("----------begin to check------------")
    nodes = []
    nodes_for_safety = []

    for i in range(n):
        node = node_info(i)
        nodes.append(node)

    for i in range(n):
        if i not in crash_nodes:
            nodes_for_safety.append(nodes[i])

    print("---------- check validity ------------")
    if check_validity(nodes):
        print("pass")
    else:
        print("not pass")

    print("---------- check safety --------------")
    if check_safety(nodes_for_safety):
        print("pass")
    else:
        print("not pass")
    print("---------- end -----------------------")
