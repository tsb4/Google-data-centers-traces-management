import json
import gzip
import pandas as pd
from tqdm import tqdm  # pip install tqdm
import os
import glob

def open_file(fname):
    if fname.endswith(".gz"):
        return gzip.open(fname, "rt")
    return open(fname, "rt")

clusters=['a','b','c','d','e','f','g','h']
filename = "instance_events-000000000000.json.gz"  # seu arquivo
filename = "collection_events-000000000000.json.gz"  # seu arquivo
file_indexes = list(range(10))

# Events
SUBMIT_EVENTS = [3]
END_EVENTS = [4, 5, 6, 7, 8]

# Total records
total_records = []

for cluster in clusters:
    job_data={}
    records = []
    folder = f"cluster_{cluster}"
    
    # Get all JSON.GZ files in the cluster folder
    files = glob.glob(os.path.join(folder, "collection_events-*.json.gz"))
    
    for filename in files:
        print("File:", filename)

        
        with open_file(filename) as f:
            print("Estimating total lines...")
            total_lines = sum(1 for _ in f)
        print(f"Total number of lines in the file: {total_lines:,}")

        print("Starting analysis...")

        with open_file(filename) as f:
            for line in tqdm(f, total=total_lines, desc="Processing lines"):
                row = json.loads(line)
                
                time = int(row["time"])
                coll_id = int(row["collection_id"])
                coll_type = int(row["collection_type"])
                ev_type = int(row["type"])
                
                #Only retrieving jobs
                if coll_type != 0:
                    continue
                
                if coll_id not in job_data:
                    job_data[coll_id] = {"submit_time": None, "end_time": None}
                
                if ev_type in SUBMIT_EVENTS:
                    if job_data[coll_id]["submit_time"] is None:
                        job_data[coll_id]["submit_time"] = time
                    elif time <= job_data[coll_id]["submit_time"]:
                        job_data[coll_id]["submit_time"] = time
                elif ev_type in END_EVENTS:
                    if job_data[coll_id]["end_time"] is None:
                        job_data[coll_id]["end_time"] = time
                    elif time <= job_data[coll_id]["end_time"]:
                        job_data[coll_id]["end_time"] = time

        print("Converting to data frame...")

    for coll_id, times in job_data.items():
        if times["submit_time"] is not None and times["end_time"] is not None:
            duration_seconds = (times["end_time"] - times["submit_time"]) / 1e6
            records.append({
                "collection_id": coll_id,
                "submit_time": times["submit_time"],
                "end_time": times["end_time"],
                "duration_seconds": duration_seconds
            })
    total_records+=records
    df = pd.DataFrame(records)

    stats = {
        'count': int(df['duration_seconds'].count()),
        'min': float(df['duration_seconds'].min()),
        'max': float(df['duration_seconds'].max()),
        'mean': float(df['duration_seconds'].mean()),
        'median': float(df['duration_seconds'].median()),
        'stddev': float(df['duration_seconds'].std())
    }

    print("Job Duration Statistics (seconds):")
    with open(f"results_{cluster}.json","w") as file:
        json.dump(stats, file, indent=4)
    with open(f"records_{cluster}.json","w") as file:
        json.dump(records, file, indent=4)
    print(stats)


df = pd.DataFrame(total_records)

stats = {
    'count': int(df['duration_seconds'].count()),
    'min': float(df['duration_seconds'].min()),
    'max': float(df['duration_seconds'].max()),
    'mean': float(df['duration_seconds'].mean()),
    'median': float(df['duration_seconds'].median()),
    'stddev': float(df['duration_seconds'].std())
}

print("Job Duration Statistics (seconds):")
with open(f"results_total.json","w") as file:
    json.dump(stats, file, indent=4)
print(stats)
