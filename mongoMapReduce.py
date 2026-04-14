from datetime import datetime
from collections import defaultdict

# ─────────────────────────────────────────────
# DUMMY DATA
# ─────────────────────────────────────────────
observations = [
    {"observationTimestamp": datetime(2024, 3, 5),  "numAnimals": 15},
    {"observationTimestamp": datetime(2024, 3, 18), "numAnimals": 8},
    {"observationTimestamp": datetime(2024, 4, 2),  "numAnimals": 22},
    {"observationTimestamp": datetime(2024, 4, 25), "numAnimals": 5},
    {"observationTimestamp": datetime(2024, 3, 30), "numAnimals": 12},
    {"observationTimestamp": datetime(2024, 5, 10), "numAnimals": 30},
]

# ─────────────────────────────────────────────
# HELPER: run any map+reduce and print results
# ─────────────────────────────────────────────
def run_mapreduce(name, map_fn, reduce_fn, data):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    # MAP
    emitted = []
    for doc in data:
        pairs = map_fn(doc)
        if pairs:
            for pair in pairs:
                emitted.append(pair)

    print("After MAP:")
    for k, v in emitted:
        print(f"  emit({k!r}, {v})")

    # SHUFFLE
    grouped = defaultdict(list)
    for key, value in emitted:
        grouped[key].append(value)

    print("After SHUFFLE:")
    for key, values in grouped.items():
        print(f"  {key!r} → {values}")

    # REDUCE
    results = {}
    for key, values in grouped.items():
        results[key] = reduce_fn(key, values)

    print("Final Result:")
    for key in sorted(results, key=lambda x: str(x)):
        print(f"  {key}: {results[key]}")

    return results


# ─────────────────────────────────────────────
# 1. AGGREGATION  (original)
#    total animals per month
# ─────────────────────────────────────────────
def map_aggregation(doc):
    year  = doc["observationTimestamp"].year
    month = doc["observationTimestamp"].month
    return [(f"{year}-{month}", doc["numAnimals"])]

def reduce_aggregation(key, values):
    return sum(values)

run_mapreduce("1. AGGREGATION — total animals per month",
              map_aggregation, reduce_aggregation, observations)


# ─────────────────────────────────────────────
# 2. FILTERING
#    only documents where numAnimals > 10
# ─────────────────────────────────────────────
def map_filtering(doc):
    if doc["numAnimals"] > 10:
        return [("large_observation", doc["numAnimals"])]
    return []

def reduce_filtering(key, values):
    return values   # just return the list as-is

run_mapreduce("2. FILTERING — observations where numAnimals > 10",
              map_filtering, reduce_filtering, observations)


# ─────────────────────────────────────────────
# 3. TRANSFORMATION
#    classify each observation into a season
# ─────────────────────────────────────────────
def map_transformation(doc):
    month = doc["observationTimestamp"].month
    if month in [12, 1, 2]:
        season = "winter"
    elif month in [3, 4, 5]:
        season = "spring"
    elif month in [6, 7, 8]:
        season = "summer"
    else:
        season = "autumn"
    return [(season, doc["numAnimals"])]

def reduce_transformation(key, values):
    return values   # all animal counts for that season

run_mapreduce("3. TRANSFORMATION — group by season",
              map_transformation, reduce_transformation, observations)


# ─────────────────────────────────────────────
# 4. MAXIMUM PER MONTH
#    highest animal count per month
# ─────────────────────────────────────────────
def map_maximum(doc):
    month = doc["observationTimestamp"].month
    return [(month, doc["numAnimals"])]

def reduce_maximum(key, values):
    return max(values)

run_mapreduce("4. MAXIMUM — peak animal count per month",
              map_maximum, reduce_maximum, observations)


# ─────────────────────────────────────────────
# 5. BUCKETING
#    count how many observations are small/medium/large
# ─────────────────────────────────────────────
def map_bucketing(doc):
    n = doc["numAnimals"]
    if n < 10:
        bucket = "small"
    elif n < 20:
        bucket = "medium"
    else:
        bucket = "large"
    return [(bucket, n)]

def reduce_bucketing(key, values):
    return len(values)   # count of observations in this bucket

run_mapreduce("5. BUCKETING — small / medium / large observations",
              map_bucketing, reduce_bucketing, observations)


# ─────────────────────────────────────────────
# 6. ANOMALY DETECTION
#    values that are 2x above the month average
# ─────────────────────────────────────────────
def map_anomaly(doc):
    month = doc["observationTimestamp"].month
    return [(month, doc["numAnimals"])]

def reduce_anomaly(key, values):
    avg    = sum(values) / len(values)
    spikes = [v for v in values if v > avg * 2]
    return spikes if spikes else "no spikes"

run_mapreduce("6. ANOMALY DETECTION — spikes 2x above monthly average",
              map_anomaly, reduce_anomaly, observations)


# ─────────────────────────────────────────────
# 7. BUILDING AN INDEX
#    dates where numAnimals > 10
# ─────────────────────────────────────────────
def map_index(doc):
    if doc["numAnimals"] > 10:
        date = doc["observationTimestamp"].strftime("%Y-%m-%d")
        return [("high_count_dates", date)]
    return []

def reduce_index(key, values):
    return sorted(values)

run_mapreduce("7. INDEX — dates with high animal counts",
              map_index, reduce_index, observations)