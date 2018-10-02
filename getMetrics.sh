#!/bin/bash

source /home/johara/metrics/bin/activate
python3.6 /home/johara/metrics/checkDatasetCount.py
python3.6 /home/johara/metrics/datasetMetrics.py
python3.6 /home/johara/metrics/github_metrics.py
python3.6 /home/johara/metrics/sandboxMetrics.py