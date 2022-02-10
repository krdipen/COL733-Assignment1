import tasks
import math
import sys
import os

data_dir = sys.argv[1]
all_files = [os.path.join(path, file) for path, dirs, files in os.walk(data_dir) for file in files]

jobs = []
batch = 200
n = math.ceil(len(all_files)/batch)-1
for i in range(n):
    job = tasks.count.s(all_files[i*batch:(i+1)*batch]).delay()
    jobs.append(job)
job = tasks.count.s(all_files[n*batch:len(all_files)]).delay()
jobs.append(job)

wc = {}
while len(jobs) > 0:
    done = []
    for job in jobs:
        if job.ready():
            count = job.get()
            for word in count:
                if word not in wc:
                    wc[word] = 0
                wc[word] += count[word]
            done.append(job)
    for job in done:
        jobs.remove(job)

print(wc)
