import json
import os

os.makedirs("../config", exist_ok=True)

N = 5
F = 2
datasize = 1000
base_port = 10000
step = 10
committee = [base_port + i * step for i in range(N)]

for i in range(N):
    cfg = {
        "id": i,
        "n": N,
        "f": F,
        "port": base_port + i * step,
        "datasize": datasize,
        "committee": committee,
        "bcpeers": [(i - 2) % N, (i - 1) % N, i]
    }
    with open(f"../config/node{i}.json", "w") as f:
        json.dump(cfg, f, indent=4)