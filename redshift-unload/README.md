# Load a table from AWS Redshift to Rockset

## Overview

This process first dumps the data from your Redshift table to your
S3 bucket. Then you create a Rockset collection and load data from
that S3 bucket.


## Edit the configuration file - config.json

Replace all the fields marked in `<>` with the corresponding values
from your environment.

```
{
    "db": {
        "host": "<redshift-cluster>",
        "port": "<port>",
        "database": "<database-name>",
        "user": "<user>",
        "password": "<password>"
    },
    "aws_access_key_id": "<key>",
    "aws_secret_access_key": "<secret>",
    "unload_options": [
    	"ADDQUOTES",
    	"PARALLEL ON",
    	"ALLOWOVERWRITE",
    	"DELIMITER ','",
        "NULL AS '_rockset_null'"
    ]
}
```

# Install the python dependency by running:

```
pip install -U psycopg2
```
If pip is not installed, use the following instructions: https://pip.pypa.io/en/stable/installing/

# Run the script

```
usage: unload.py [-h] [-t T] [-c C] [-f F] [-r R] [-r1 R1] [-r2 R2]

optional arguments:
  -h, --help  show this help message and exit
  -t T        Table name
  -c C        Schema name
  -f F        Desired S3 file path
  -r R        Range column
  -r1 R1      Range start
  -r2 R2      Range end
```

## Common usage:
```
python ./unload.py -t <table-name> -f s3://<your-s3-bucket>/
```

The above command uploads the contents of your RedShift table into the 
specified S3 bucket. It also creates a local file named <table-name>.schema.yaml
that contains the metadata about the Redshift table.

Now you can create a Rockset collection to load the data from the 
S3 bucket.
```
rock create collection <collectionname> s3://bucket_path
             --integration <name f integration>
             --format=CSV
             --csv-schema-file=<redshift-table-name>.schema.yaml
```
