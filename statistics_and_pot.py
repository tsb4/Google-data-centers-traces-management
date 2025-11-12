import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
import os

def plot_time_histograms(records, cluster):
    

    # if not records:
    #     raise ValueError("The input records list is empty.")
    
    # Convert to DataFrame
    df = pd.DataFrame(records)

    # Reference datetime for timestamp=0
    ref_time = datetime(2019, 5, 1)

    # Convert timestamps to datetimes
    for col in ["submit_time", "end_time"]:
        df[col] = df[col].apply(lambda ts: ref_time + timedelta(seconds=float(ts)))


    # Create output directory if it doesn't exist
    os.makedirs(f"Figs/{cluster}", exist_ok=True)

    # --- 1. Histogram of submit_time ---
    plt.figure(figsize=(8, 5))
    df["submit_time"].hist(bins=30)
    plt.title("Distribution of Submit Times")
    plt.xlabel("Submit Time")
    plt.ylabel("Count")
    plt.grid(False)
    plt.tight_layout()
    plt.savefig(f"Figs/{cluster}/submit_time_hist.png")
    plt.close()

    # --- 2. Histogram of end_time ---
    plt.figure(figsize=(8, 5))
    df["end_time"].hist(bins=30)
    plt.title("Distribution of End Times")
    plt.xlabel("End Time")
    plt.ylabel("Count")
    plt.grid(False)
    plt.tight_layout()
    plt.savefig(f"Figs/{cluster}/end_time_hist.png")
    plt.close()

    # --- 3. Histogram of duration_seconds ---
    plt.figure(figsize=(8, 5))
    df["duration_seconds"].hist(bins=30)
    plt.title("Distribution of Duration (Seconds)")
    plt.xlabel("Duration (s)")
    plt.ylabel("Count")
    plt.grid(False)
    plt.tight_layout()
    plt.savefig(f"Figs/{cluster}/duration_seconds_hist.png")
    plt.close()

import json
import gzip
import pandas as pd
from tqdm import tqdm  # pip install tqdm
import os
import glob
cluster ='a'
with open(f"records_{cluster}.json", "r") as file:
    data = json.load(file)

plot_time_histograms(data, cluster)