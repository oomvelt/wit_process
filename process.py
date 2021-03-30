#!/usr/bin/env python
import pandas
import sys
from pathlib import Path
import time
import json
import hmac
import hashlib

empty_signature = ''.join(['0'] * 64)
HMAC_KEY = "fed53116f20684c067774ebf9e7bcbdc"
API_KEY = "ei_fd83..."

try:
  input_file = sys.argv[1]
except IndexError:
  raise SystemExit(f"Usage: {sys.argv[0]} <file_to_process> <output_file>")

try:
  seconds = sys.argv[2]
except IndexError:
  seconds = 1

# We need to read the first line to grab the date
f = open(input_file)
header = f.readline()
header_parts = header.rstrip('\n').split(' ')
start_time = header_parts[1]
device_name = header_parts[4]

base_name = Path(input_file).stem

# Read the data into a data frame
df = pandas.read_csv(input_file, skiprows=[0], delimiter='\t', dtype={
  'address': str,
  'Time(s)': str,
  'ax(g)': float,
  'ay(g)': float,
  'az(g)': float,
  'wx(deg/s)': float,
  'wy(deg/s)': float,
  'wz(deg/s)': float,
  'AngleX(deg)': float,
  'AngleY(deg)': float,
  'AngleZ(deg)': float,
  'T(°)': float,
  'hx': float,
  'hy': float,
  'hz ': float
})

# We need to add the date to the time column so that we can parse it
for idx,row in df.iterrows():
  df.at[idx, 'Time(s)'] = f"{start_time}{df.at[idx, 'Time(s)']}"
  df.at[idx, 'ax(g)'] = df.at[idx, 'ax(g)'] * 9.81
  df.at[idx, 'ay(g)'] = df.at[idx, 'ay(g)'] * 9.81
  df.at[idx, 'az(g)'] = df.at[idx, 'az(g)'] * 9.81
  df.at[idx, 'hx'] = df.at[idx, 'hx'] / 1000000.0
  df.at[idx, 'hy'] = df.at[idx, 'hy'] / 1000000.0
  df.at[idx, 'hz '] = df.at[idx, 'hz '] / 1000000.0

# Parse the time into two new fields, with different granularity
# timestamp - used for grouping, granularity in seconds
# timestamp_e - used for interpolation, granularity in miliseconds
df['timestamp_e'] = pandas.to_datetime(df['Time(s)'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
df['timestamp'] = pandas.to_datetime(df['Time(s)'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
df.index = df['timestamp_e']

# Convert each group into the EdgeImpulse format and write it to a file
for n, g in df.groupby(pandas.Grouper(key='timestamp',freq=f"{seconds}S")):
  fname = f"{base_name}_{n.strftime('%Y_%m_%d_%H_%M_%S')}.json"

  data = {
    "protected": {
      "ver": "v1",
      "alg": "HS256",
      "iat": time.time()
    },
    "signature": empty_signature,
    "payload": {
      "device_name": device_name,
      "device_type": "RAT",
      "interval_ms": 1,
      "sensors": [
        { "name": "aX", "units": "m/s2" },
        { "name": "aY", "units": "m/s2" },
        { "name": "aZ", "units": "m/s2" },
        { "name": "wX", "units": "count" },
        { "name": "wY", "units": "count" },
        { "name": "wZ", "units": "count" },
        { "name": "angleX", "units": "deg" },
        { "name": "angleY", "units": "deg" },
        { "name": "angleZ", "units": "deg" },
        { "name": "t", "units": "Cel" },
        { "name": "hX", "units": "T" },
        { "name": "hY", "units": "T" },
        { "name": "hZ", "units": "T" },
      ],
      "values": [],
    },
  }

  # Resample to 1ms and interpolate
  gr = g.resample('1L').mean()

  gr["ax(g)"] = gr["ax(g)"].interpolate()
  gr["ay(g)"] = gr["ay(g)"].interpolate()
  gr["az(g)"] = gr["az(g)"].interpolate()
  gr["wx(deg/s)"] = gr["wx(deg/s)"].interpolate()
  gr["wy(deg/s)"] = gr["wy(deg/s)"].interpolate()
  gr["wz(deg/s)"] = gr["wz(deg/s)"].interpolate()
  gr["AngleX(deg)"] = gr["AngleX(deg)"].interpolate()
  gr["AngleY(deg)"] = gr["AngleY(deg)"].interpolate()
  gr["AngleZ(deg)"] = gr["AngleZ(deg)"].interpolate()
  gr["T(°)"] = gr["T(°)"].interpolate()
  gr["hx"] = gr["hx"].interpolate()
  gr["hy"] = gr["hy"].interpolate()
  gr["hz "] = gr["hz "].interpolate()

  for idx,row in gr.iterrows():
    data["payload"]["values"].append([
      row["ax(g)"],
      row["ay(g)"],
      row["az(g)"],
      row["wx(deg/s)"],
      row["wy(deg/s)"],
      row["wz(deg/s)"],
      row["AngleX(deg)"],
      row["AngleY(deg)"],
      row["AngleZ(deg)"],
      row["T(°)"],
      row["hx"],
      row["hy"],
      row["hz "],
    ])

  encoded = json.dumps(data)
  # Sign message
  signature = hmac.new(bytes(HMAC_KEY, 'utf-8'), msg = encoded.encode('utf-8'), digestmod = hashlib.sha256).hexdigest()

  # Set the signature again in the message, and encode again
  data['signature'] = signature
  encoded = json.dumps(data)

  print(encoded)
  with open(fname, 'w') as file:
    file.write(encoded)
  print(f"Saved file {fname}.")
