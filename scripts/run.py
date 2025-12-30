import subprocess
import sys

subprocess.run("cd ../cmd && make", shell=True, check=True)

procs = []
for i in range(5):
    f = open(f"../log/log{i}", "w")
    proc = subprocess.Popen(
        [
            "../cmd/node",
            "-i",
            str(i),
            "-o",
            sys.argv[1],
            "-l",
            sys.argv[2],
            "-t",
            sys.argv[3],
        ],
        stdout=f,
        stderr=f,
    )
    procs.append(proc)

for p in procs:
    p.wait()
