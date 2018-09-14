'''
Based on https://github.com/openbridge/ob_redshift_unload
Rockset Document
'''

import json
import os
import psycopg2
import argparse
import yaml
from pprint import pprint

mapping = {
    "INTEGER": ("SMALLINT", "INT2", "INTEGER", "INT", "INT4", "BIGINT", "DECIMAL"),
    "STRING": ("CHAR", "CHARACTER", "NCHAR", "BPCHAR", "VARCHAR", "CHARACTER VARYING", "NVARCHAR", "TEXT"),
    "FLOAT": ("REAL", "FLOAT4", "DOUBLE PRECISION", "FLOAT8", "FLOAT"),
    "BOOLEAN": ("BOOLEAN", "BOOL"),
    "DATETIME": ("TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"),
    "DATE": ("DATE"),
    "TIMESTAMP": ("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE")
}

def _get_rockset_type(redshift_type):
    for key in mapping:
        if redshift_type.upper() in mapping[key]:
            return key
    # if all else fails, call it a string.
    return "STRING"


def _simple_sanitize(s):
    return s.split(';')[0]

def _read_schema(cursor, schema_name, table_name):
    if schema_name:
        query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' AND table_schema = '{}' ORDER BY ordinal_position".format(table_name, schema_name)
    else:
        query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position".format(table_name)
    cursor.execute(query)
    res = cursor.fetchall()
    return res

def _dump_schema_file(table_name, schema_def):
    file = table_name + '.schema.yaml'
    serialized = dict()
    serialized['version'] = 1.0
    serialized['types'] = list()
    for v, t in schema_def:
        serialized['types'].append({v: _get_rockset_type(t)})
    with open(file, 'w') as outfile:
        yaml.dump(serialized, outfile, default_flow_style=False)
    print('=== Completed write to {} ==='.format(file))

def run(config, table_name, file_path, schema_name=None, range_col=None, range_start=None, range_end=None):
    if not file_path:
        file_path = table_name
    conn = psycopg2.connect(**config['db'])
    res = _read_schema(conn.cursor(), schema_name, table_name)
    _dump_schema_file(table_name, res)

    cast_columns = []
    for col in res:
        if 'boolean' in col[1]:
            cast_columns.append("CASE {} WHEN 1 THEN \\\'true\\\' ELSE \\\'false\\\'::text END".format(col[0]))
        else:
            cast_columns.append("{}::text".format(col[0]))
    cast_columns_str = ", ".join(cast_columns)

    cursor = conn.cursor()
    where_clause = ""
    unload_options = '\n'.join(config.get('unload_options', []))
    if range_col and range_start and range_end:
        where_clause = cursor.mogrify("WHERE {} BETWEEN \\\'{}\\\' AND \\\'{}\\\'".format(range_col, range_start, range_end,))
    query = """
    UNLOAD (\'SELECT {0} FROM {1}{2} {3}\')
    TO \'{6}\'
    CREDENTIALS 'aws_access_key_id={4};aws_secret_access_key={5}'
    {7}
    """.format(cast_columns_str, '{}.'.format(schema_name) if schema_name else '', table_name,
               where_clause, config['aws_access_key_id'],
               config['aws_secret_access_key'], file_path, unload_options)
    print("The following UNLOAD query is being run: \n" + query)
    cursor.execute(query)
    print('=== Completed write to {} ==='.format(file_path))

if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')
    with open(config_path, 'r') as f:
        config = json.loads(f.read())
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', help='Table name')
    parser.add_argument('-c', help='Schema name')
    parser.add_argument('-f', help='Desired S3 file path')
    parser.add_argument('-r', help='Range column')
    parser.add_argument('-r1', help='Range start')
    parser.add_argument('-r2', help='Range end')
    raw_args = parser.parse_args()
    if 's' in vars(raw_args) and raw_args.s:
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), raw_args.s), 'r') as f:
            raw_args.s = f.read()
    args = {}
    for k, v in vars(raw_args).items():
        if v:
            args[k] = _simple_sanitize(v)
        else:
            args[k] = None
    run(config, args['t'], args['f'], args['c'], args['r'], args['r1'], args['r2'])
