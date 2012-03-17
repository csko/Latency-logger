import subprocess
import time
from mongolog import MongoLogger, get_targets
from config import MY_ID

def measure_latency():
    hosts = get_targets()
    cmd = "fping %s -e -a -C 4" % (" ".join(hosts))

    popen = subprocess.Popen(cmd.split(" "), stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    results = {}
    for line in iter(popen.stderr.readline, ""):
      if len(line) > 1 and line.find("ICMP Time Exceeded") == -1:
          words = line.split(":")
          hostname = words[0].strip()
          pings = [float(x) if x != "-" else -1.0 for x in
              words[1].strip().split()]
          results[hostname] = pings

    return results

mongologger = MongoLogger()

DELAY = 5

while True:
    pings = measure_latency()
    for k, v in pings.items():
        mongologger.log(MY_ID, k, v)
    print "Uploaded latency results:", pings
    time.sleep(DELAY) # TODO: do this properly


