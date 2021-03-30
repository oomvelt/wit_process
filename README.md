# WIT Process

This utility will process a data file from the WIT software and turn it into a set of files that can be submitted to EdgeImpulse.

## Dependencies

* Python 3
* Pandas: `pip install pandas`

## Configuration

Edit `process.py` and set the values for `HMAC_KEY` and `API_KEY` at the top of the file.

## Usage

`./process.py input_file number_of_seconds`

The number of seconds is used for grouping the data. It is optional and it defaults to 1 second.

The data will be interpolated at a 1ms interval, grouped by the specified number of seconds, and saved into a separate file for each group. E.g. for the suppplied Doreen.txt file, it will generate files named similar to `Doreen_2020_08_19_12_25_31.json`, `Doreen_2020_08_19_12_25_33.json`, etc.
