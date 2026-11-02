"""Batch ETL: bounded memory, atomic replacement and source-row lineage."""
import gzip
import io
import json
import logging
import os
import tempfile
from pathlib import Path

import psycopg2
from psycopg2 import sql
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.csv as arrow_csv

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get('TAXI_DATA_DIR', '/opt/airflow/data'))


def connect():
    return psycopg2.connect(os.environ['TAXI_DATABASE_URL'])


def unpack(path, directory):
    with path.open('rb') as source:
        magic = source.read(4)
    if magic == b'PAR1':
        return pq.ParquetFile(path)
    if magic[:2] != b'\x1f\x8b':
        raise ValueError(f'Unsupported file signature: {path.name}: {magic!r}')
    target = Path(directory) / 'input.parquet'
    import shutil
    with gzip.open(path, 'rb') as source, target.open('wb') as dest:
        shutil.copyfileobj(source, dest)
    return pq.ParquetFile(target)


def extract_load():
    files = sorted(DATA.glob('yellow_tripdata_2022-*.parquet.gz'))
    expected = {f'yellow_tripdata_2022-{month:02d}.parquet.gz' for month in range(1, 13)}
    if {p.name for p in files} != expected:
        raise ValueError('Expected exactly the 12 monthly source files for 2022')
    with tempfile.TemporaryDirectory() as temp, connect() as conn, conn.cursor() as cur:
        columns = []
        counts = {}
        for path in files:
            parquet = unpack(path, temp)
            counts[path.name] = parquet.metadata.num_rows
            for name in parquet.schema_arrow.names:
                if name not in columns:
                    columns.append(name)
        cur.execute('CREATE SCHEMA IF NOT EXISTS raw')
        cur.execute('DROP TABLE IF EXISTS raw.trips')
        definitions = [sql.SQL('{} text').format(sql.Identifier(c)) for c in columns]
        cur.execute(sql.SQL('CREATE TABLE raw.trips (source_file text NOT NULL, source_row bigint NOT NULL, {})').format(sql.SQL(',').join(definitions)))
        cur.execute('CREATE TABLE IF NOT EXISTS raw.files (source_file text PRIMARY KEY, row_count bigint NOT NULL)')
        cur.execute('TRUNCATE raw.files')
        for path in files:
            parquet = unpack(path, temp)
            offset = 0
            for batch in parquet.iter_batches(batch_size=25000):
                arrays = [pa.array([path.name] * batch.num_rows),
                          pa.array(range(offset + 1, offset + batch.num_rows + 1))]
                arrays += [batch.column(batch.schema.get_field_index(c))
                           if c in batch.schema.names else pa.nulls(batch.num_rows)
                           for c in columns]
                table = pa.Table.from_arrays(arrays, names=['source_file', 'source_row'] + columns)
                buffer = pa.BufferOutputStream()
                arrow_csv.write_csv(table, buffer, write_options=arrow_csv.WriteOptions(include_header=False))
                cur.copy_expert('COPY raw.trips FROM STDIN WITH (FORMAT CSV)',
                                io.BytesIO(buffer.getvalue().to_pybytes()))
                offset += batch.num_rows
            if offset != counts[path.name]:
                raise ValueError(f'Row count mismatch for {path.name}')
            cur.execute('INSERT INTO raw.files VALUES (%s, %s)', (path.name, offset))
            logging.info('Loaded %s: %s rows', path.name, offset)


def transform():
    with connect() as conn, conn.cursor() as cur:
        cur.execute((ROOT / 'sql/transform.sql').read_text())


def report():
    answers = {}
    with connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT (SELECT sum(row_count) FROM raw.files), (SELECT count(*) FROM raw.trips), (SELECT count(*) FROM trusted.trips), (SELECT count(*) FROM refined.trips)')
        counts = tuple(int(value) if value is not None else None for value in cur.fetchone())
        if not counts[0] or len(set(counts)) != 1:
            raise ValueError(f'Layer count mismatch: {counts}')
        answers['layer_counts'] = dict(zip(['source', 'raw', 'trusted', 'refined'], counts))
        logging.info('Validated layer counts: %s', answers['layer_counts'])
        cur.execute('SELECT count(*) FROM refined.trips WHERE is_valid IS NOT TRUE')
        answers['flagged_records'] = cur.fetchone()[0]
        for index, query in enumerate((ROOT / 'sql/answers.sql').read_text().split(';'), 1):
            if query.strip():
                logging.info('Computing question %s', index)
                cur.execute(query)
                names = [d.name for d in cur.description]
                answers[f'question_{index}'] = [dict(zip(names, row)) for row in cur.fetchall()]
    output = ROOT / 'results/answers.json'
    output.parent.mkdir(exist_ok=True)
    temporary = output.with_suffix('.tmp')
    temporary.write_text(json.dumps(answers, indent=2, default=str) + '\n')
    temporary.replace(output)
    logging.info('Answers: %s', answers)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('step', choices=['extract', 'transform', 'report', 'all'])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    for name, function in [('extract', extract_load), ('transform', transform), ('report', report)]:
        if args.step in (name, 'all'):
            function()
