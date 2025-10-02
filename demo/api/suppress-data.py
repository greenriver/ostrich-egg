import json
import os
import tempfile
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

# Set DuckDB home directory for extension installation
os.environ['HOME'] = '/tmp'

from ostrich_egg.engine import Engine
from ostrich_egg.config import (
    Config, DatasetConfig, DataSource, Metric, Aggregations,
    MarkRedacted, MarkRedactedParameters
)

# Sample data from README.md - Library in County A donor participation analysis
# Using a larger dataset similar to the test data to demonstrate varying levels of suppression
SAMPLE_DATA = [
    {"count": 3, "age": 30, "sex": "M", "zip_code": "00000", "library_friend": "Yes"},
    {"count": 20, "age": 40, "sex": "F", "zip_code": "00000", "library_friend": "Yes"},
    {"count": 25, "age": 30, "sex": "M", "zip_code": "00000", "library_friend": "No"},
    {"count": 12, "age": 40, "sex": "F", "zip_code": "00000", "library_friend": "No"},
    {"count": 13, "age": 40, "sex": "M", "zip_code": "00001", "library_friend": "Yes"},
    {"count": 21, "age": 40, "sex": "F", "zip_code": "00001", "library_friend": "Yes"},
    {"count": 26, "age": 40, "sex": "M", "zip_code": "00001", "library_friend": "No"},
    {"count": 15, "age": 40, "sex": "F", "zip_code": "00001", "library_friend": "No"},
    {"count": 14, "age": 40, "sex": "M", "zip_code": "00002", "library_friend": "Yes"},
    {"count": 25, "age": 40, "sex": "F", "zip_code": "00002", "library_friend": "Yes"},
    {"count": 27, "age": 40, "sex": "M", "zip_code": "00002", "library_friend": "No"},
    {"count": 16, "age": 40, "sex": "F", "zip_code": "00002", "library_friend": "No"},
]

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Set CORS headers
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

        # Parse query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        raw_data = query_params.get('raw_data', ['false'])[0] == 'true'
        threshold = int(query_params.get('threshold', ['11'])[0])
        redacted_dimension = query_params.get('redacted_dimension', ['sex'])[0]

        if raw_data:
            # For raw data, run ostrich_egg to identify at-risk records but return raw data with risk flags
            csv_lines = ["count,age,sex,zip_code,library_friend"]
            for row in SAMPLE_DATA:
                csv_line = f"{row['count']},{row['age']},{row['sex']},{row['zip_code']},{row['library_friend']}"
                csv_lines.append(csv_line)

            csv_content = '\n'.join(csv_lines)

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_csv:
                temp_csv.write(csv_content)
                temp_csv_path = temp_csv.name

            output_file = f"/tmp/ostrich_raw_{os.getpid()}_{threshold}.parquet"

            config = Config(
                threshold=threshold,
                datasource=DataSource(
                    connection_type="file",
                    connection_params={"output_directory": "/tmp/"}
                ),
                datasets=[
                    DatasetConfig(
                        source_file=temp_csv_path,
                        dimensions=["age", "sex", "zip_code", "library_friend"],
                        metrics=[
                            Metric(aggregation=Aggregations.SUM, column="count", alias="count")
                        ],
                        suppression_strategies=[
                            MarkRedacted(
                                parameters=MarkRedactedParameters(redacted_dimension=redacted_dimension)
                            )
                        ]
                    )
                ]
            )

            engine = Engine(config=config)
            engine.datasets[0].output_file = output_file
            engine.run()

            # Read risk flags from output
            df = engine.connector.duckdb_connection.execute(f"SELECT * FROM '{output_file}'").df()

            # Build raw data with risk flags
            raw_data_with_risk = []
            for idx, row in df.iterrows():
                raw_data_with_risk.append({
                    'count': int(row['count']),
                    'age': row['age'],
                    'sex': row['sex'],
                    'zip_code': row['zip_code'],
                    'library_friend': 'Yes' if row['library_friend'] else 'No',
                    'is_at_risk': row.get('is_redacted', False),
                    'is_below_threshold': row['count'] < threshold
                })

            # Sort by count descending
            raw_data_with_risk.sort(key=lambda x: x['count'], reverse=True)

            # Cleanup
            os.unlink(temp_csv_path)
            if os.path.exists(output_file):
                os.unlink(output_file)

            total_cells = len(raw_data_with_risk)
            at_risk_cells = sum(1 for row in raw_data_with_risk if row['is_at_risk'])
            below_threshold_cells = sum(1 for row in raw_data_with_risk if row['is_below_threshold'])

            response = {
                'success': True,
                'data': raw_data_with_risk,
                'type': 'raw',
                'stats': {
                    'total_cells': total_cells,
                    'at_risk_cells': at_risk_cells,
                    'below_threshold_cells': below_threshold_cells,
                    'at_risk_rate': round((at_risk_cells / total_cells * 100) if total_cells > 0 else 0, 1),
                    'threshold_used': threshold,
                    'dimension_suppressed': redacted_dimension
                },
                'message': 'Raw County A library demographic data with privacy risk indicators'
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
            return

        # Create temporary CSV file
        csv_lines = ["count,age,sex,zip_code,library_friend"]
        for row in SAMPLE_DATA:
            csv_line = f"{row['count']},{row['age']},{row['sex']},{row['zip_code']},{row['library_friend']}"
            csv_lines.append(csv_line)

        csv_content = '\n'.join(csv_lines)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_csv:
            temp_csv.write(csv_content)
            temp_csv_path = temp_csv.name

        # Configure and run Ostrich Egg engine
        # Create output file path
        output_file = f"/tmp/ostrich_output_{os.getpid()}_{threshold}.parquet"

        config = Config(
            threshold=threshold,
            datasource=DataSource(
                connection_type="file",
                connection_params={"output_directory": "/tmp/"}
            ),
            datasets=[
                DatasetConfig(
                    source_file=temp_csv_path,
                    dimensions=["age", "sex", "zip_code", "library_friend"],
                    metrics=[
                        Metric(aggregation=Aggregations.SUM, column="count", alias="count")
                    ],
                    suppression_strategies=[
                        MarkRedacted(
                            parameters=MarkRedactedParameters(redacted_dimension=redacted_dimension)
                        )
                    ]
                )
            ]
        )

        engine = Engine(config=config)
        engine.datasets[0].output_file = output_file
        engine.run()

        # Read from the output file (not the result table!)
        df = engine.connector.duckdb_connection.execute(f"SELECT * FROM '{output_file}'").df()

        suppressed_data = df.to_dict('records')

        # Sort by count descending first (before applying redaction)
        suppressed_data.sort(key=lambda x: x['count'], reverse=True)

        total_cells = len(suppressed_data)
        redacted_cells = sum(1 for row in suppressed_data if row.get('is_redacted', False))
        at_risk_cells = sum(1 for row in suppressed_data if row['count'] < threshold)

        stats = {
            'total_cells': total_cells,
            'redacted_cells': redacted_cells,
            'at_risk_cells': at_risk_cells,
            'suppression_rate': round((redacted_cells / total_cells * 100) if total_cells > 0 else 0, 1),
            'at_risk_rate': round((at_risk_cells / total_cells * 100) if total_cells > 0 else 0, 1),
            'threshold_used': threshold,
            'dimension_suppressed': redacted_dimension
        }

        cleaned_data = []
        for row in suppressed_data:
            if row.get('is_redacted', False):
                cleaned_data.append({
                    'count': 'Redacted',
                    'age': 'Redacted',
                    'sex': 'Redacted',
                    'zip_code': 'Redacted',
                    'library_friend': 'Redacted',
                })
            else:
                cleaned_data.append({
                    'count': int(row['count']),
                    'age': row['age'],
                    'sex': row['sex'],
                    'zip_code': row['zip_code'],
                    'library_friend': 'Yes' if row['library_friend'] else 'No',
                })

        # Cleanup temp files
        os.unlink(temp_csv_path)
        if os.path.exists(output_file):
            os.unlink(output_file)

        response = {
            'success': True,
            'data': cleaned_data,
            'type': 'suppressed',
            'stats': stats,
            'method': 'Actual Ostrich Egg Engine',
            'ostrich_egg_available': True,
            'message': 'Privacy-protected County A library data with Ostrich Egg suppression applied'
        }

        self.wfile.write(json.dumps(response).encode('utf-8'))

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
