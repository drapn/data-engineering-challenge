"""Run only against a disposable database: TAXI_DATABASE_URL=.../taxi_test."""
import gzip
import os
import json
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline import etl


class IntegrationTest(unittest.TestCase):
    def test_boundaries_nulls_lineage_and_idempotence(self):
        self.assertTrue(os.environ['TAXI_DATABASE_URL'].endswith('/taxi_test'))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'sql').mkdir()
            shutil.copy(etl.ROOT / 'sql/answers.sql', root / 'sql/answers.sql')
            rows = [
                {'tpep_pickup_datetime': datetime(2022, 6, 17), 'tpep_dropoff_datetime': datetime(2022, 6, 17, 1), 'trip_distance': 2.0, 'passenger_count': None, 'total_amount': 10.0},
                {'tpep_pickup_datetime': datetime(2022, 6, 17, 23), 'tpep_dropoff_datetime': datetime(2022, 6, 18), 'trip_distance': 8.0, 'passenger_count': 1.0, 'total_amount': 20.0},
                {'tpep_pickup_datetime': datetime(2022, 6, 17), 'tpep_dropoff_datetime': datetime(2022, 6, 16), 'trip_distance': -1.0, 'passenger_count': 1.0, 'total_amount': 0.0},
            ]
            for month in range(1, 13):
                sink = pa.BufferOutputStream()
                pq.write_table(pa.Table.from_pylist(rows), sink)
                content = sink.getvalue().to_pybytes()
                (root / f'yellow_tripdata_2022-{month:02d}.parquet.gz').write_bytes(
                    gzip.compress(content) if month % 2 else content)
            with patch.object(etl, 'DATA', root):
                for _ in range(2):
                    etl.extract_load()
                    etl.transform()
                    with etl.connect() as conn, conn.cursor() as cur:
                        cur.execute('SELECT count(*), count(DISTINCT (source_file, source_row)), count(*) FILTER (WHERE is_valid IS NOT TRUE) FROM refined.trips')
                        self.assertEqual(cur.fetchone(), (36, 36, 12))
                        cur.execute((etl.ROOT / 'sql/answers.sql').read_text().split(';')[1])
                        self.assertEqual(cur.fetchone()[0], 12)
                        cur.execute('SELECT count(*) FROM trusted.trips WHERE passenger_count IS NULL')
                        self.assertEqual(cur.fetchone()[0], 12)
                with patch.object(etl, 'ROOT', root):
                    etl.report()
                answers = json.loads((root / 'results/answers.json').read_text())
                self.assertEqual(answers['question_1'][0]['total_records'], 36)
                self.assertEqual(answers['question_2'][0]['trips_june_17'], 12)
                self.assertEqual(len(answers['question_3']), 12)
                self.assertEqual(answers['question_4'][0]['quartiles'], [-1.0, 2.0, 8.0])


if __name__ == '__main__':
    unittest.main()
