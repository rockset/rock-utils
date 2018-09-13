# Unload CSVs from Redshift to Rockset.

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

# Install the python dependencies by running:

```
pip install -r requirements.txt
```
If pip is not installed, use the following instructions: https://pip.pypa.io/en/stable/installing/


# Run the script

```
python ./unload.py -t <table-name> -f s3://<your-s3-bucket>/
```