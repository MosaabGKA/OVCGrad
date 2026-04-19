#!/usr/bin/env python3
"""
Run a complex BigQuery query against the IDC v23 DICOM pivot table.
Saves results to a CSV file or prints a preview.
"""

import argparse
import logging
import sys

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


# The original SQL query (without any LIMIT)
BASE_QUERY = """
SELECT
  dicom_pivot.PatientID,
  dicom_pivot.collection_id,
  dicom_pivot.source_DOI,
  dicom_pivot.StudyInstanceUID,
  dicom_pivot.SeriesInstanceUID,
  dicom_pivot.SOPInstanceUID,
  dicom_pivot.gcs_url
FROM
  `bigquery-public-data.idc_v23.dicom_pivot` dicom_pivot
WHERE
  StudyInstanceUID IN (
    SELECT
      StudyInstanceUID
    FROM
      `bigquery-public-data.idc_v23.dicom_pivot` dicom_pivot
    WHERE
      StudyInstanceUID IN (
        SELECT
          StudyInstanceUID
        FROM
          `bigquery-public-data.idc_v23.dicom_pivot` dicom_pivot
        WHERE
          (dicom_pivot.Modality IN ('CT', 'OT', 'MR'))
        GROUP BY
          StudyInstanceUID
        INTERSECT DISTINCT
        SELECT
          StudyInstanceUID
        FROM
          `bigquery-public-data.idc_v23.dicom_pivot` dicom_pivot
        WHERE
          (dicom_pivot.collection_id IN ('TCGA', 'tcga_ov'))
        GROUP BY
          StudyInstanceUID
      )
    GROUP BY
      StudyInstanceUID
  )
GROUP BY
  dicom_pivot.PatientID,
  dicom_pivot.collection_id,
  dicom_pivot.source_DOI,
  dicom_pivot.StudyInstanceUID,
  dicom_pivot.SeriesInstanceUID,
  dicom_pivot.SOPInstanceUID,
  dicom_pivot.gcs_url
ORDER BY
  dicom_pivot.PatientID ASC,
  dicom_pivot.collection_id ASC,
  dicom_pivot.source_DOI ASC,
  dicom_pivot.StudyInstanceUID ASC,
  dicom_pivot.SeriesInstanceUID ASC,
  dicom_pivot.SOPInstanceUID ASC,
  dicom_pivot.gcs_url ASC
"""


def main():
    parser = argparse.ArgumentParser(description="Run IDC v23 query and export results.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit the number of output rows (adds LIMIT clause).")
    parser.add_argument("--output", type=str, default=None,
                        help="Output CSV file path. If not provided, prints first 20 rows.")
    parser.add_argument("--project", type=str, default=None,
                        help="GCP project ID to use for billing (required if not set in env).")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logs.")
    args = parser.parse_args()

    if not args.quiet:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING)

    # Build final SQL with optional LIMIT
    sql = BASE_QUERY
    if args.limit is not None:
        sql = f"{BASE_QUERY}\nLIMIT {args.limit}"

    logging.info("Initializing BigQuery client...")
    try:
        # If project is provided, use it; otherwise client will infer from environment
        client = bigquery.Client(project=args.project)
        logging.info(f"Using project: {client.project}")
    except Exception as e:
        logging.error(f"Failed to create BigQuery client: {e}")
        sys.exit(1)

    logging.info("Submitting query job...")
    try:
        query_job = client.query(sql)
        # Wait for job to complete
        result = query_job.result()  # blocks
        logging.info(f"Query completed. Total rows: {result.total_rows}")
    except GoogleAPIError as e:
        logging.error(f"BigQuery error: {e}")
        sys.exit(1)

    if args.output:
        # Export to CSV using BigQuery's extract job (more efficient for large results)
        # But that requires a GCS bucket. Alternative: fetch all rows and write to CSV locally.
        # For simplicity and portability, we fetch rows into a pandas DataFrame and write CSV.
        try:
            import pandas as pd
        except ImportError:
            logging.error("pandas is required for CSV output. Install with: pip install pandas")
            sys.exit(1)
        logging.info(f"Fetching all rows into DataFrame (this may take memory)...")
        df = result.to_dataframe()
        logging.info(f"Writing {len(df)} rows to {args.output}")
        df.to_csv(args.output, index=False)
        logging.info("Done.")
    else:
        # Print a preview
        logging.info("No output file specified. Displaying first 20 rows:")
        rows = list(result)
        if not rows:
            print("Query returned no rows.")
            return
        # Print header
        headers = list(rows[0].keys())
        print("\t".join(headers))
        for row in rows[:20]:
            print("\t".join(str(row[h]) for h in headers))
        if len(rows) > 20:
            print(f"... and {len(rows) - 20} more rows.")


if __name__ == "__main__":
    main()
