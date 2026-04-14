**PlayGround**
Simple Algorithmic Implementations
# Bitcask
A simplest implementation of the Bitcask key-value storage engine in Python for better understanding.
Every write appends to a log file on disk. An in-memory hash table (KeyDir) maps each key to its offset in that file, so reads are always a single disk seek.


# MapReduce

A simple implementation of the MapReduce programming model in Python.
Split input → **Map** (process each chunk) → **Shuffle** (group by key) → **Reduce** (aggregate).
