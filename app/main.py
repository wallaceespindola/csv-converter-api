import argparse
import io
import logging
import os
from datetime import datetime

import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException, Body

# configure basic logging
logging.basicConfig(level=logging.DEBUG)

_logger = logging.getLogger(__name__)

"""
Postman Testing Instructions:
1. Start the FastAPI server: `uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload`
2. Open Postman and create a new request.
3. Set the request type to POST.
4. Use the following URLs for each conversion:
   - SK to WP: `http://127.0.0.1:8001/api/converters/sk-to-wp`
   - WP to SK: `http://127.0.0.1:8001/api/converters/wp-to-sk`
5. In the 'Body' section, select 'form-data' and upload a CSV file under the 'file' key.
6. Click 'Send' and check the response for the converted CSV output.
"""

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SK_REFERENCE_FILE = os.path.join(BASE_DIR, "..", "resources", "sk-reference.csv")
WP_REFERENCE_FILE = os.path.join(BASE_DIR, "..", "resources", "wp-reference.csv")
WP_TO_SK = "wp-to-sk"
SK_TO_WP = "sk-to-wp"

app = FastAPI(
    title="CSV Converter",
    version="1.0.0",
    description="API for managing CSV conversions via REST",
)

_logger.info(" >>>>> STARTING CSV CONVERTER APP <<<<<")


def convert_sk_to_wp(sk_csv: pd.DataFrame, wp_csv: pd.DataFrame) -> pd.DataFrame:
    """Converts SK CSV to WP CSV format, filling missing values from WP CSV and auto-incrementing IDs."""
    _logger.info(" conversion started: convert_sk_to_wp")

    # Auto-increment IDs starting from the initial value
    initial_id = 20000
    sk_csv['ID'] = range(initial_id, initial_id + len(sk_csv))

    _logger.info(f" Assigned IDs: {sk_csv['ID'].tolist()}")  # This logs the id column to check the values

    wp_defaults = wp_csv.iloc[0]  # Assume first row contains example values
    for col in wp_csv.columns:
        if col not in sk_csv.columns:
            sk_csv[col] = wp_defaults[col]

    # Reorder the columns to match the wp_csv structure and ensure 'ID' is replaced as the first column
    wp_columns = ['ID'] + [col for col in wp_csv.columns if col != 'ID']
    return sk_csv[wp_columns]





def convert_wp_to_sk(wp_csv: pd.DataFrame, sk_csv: pd.DataFrame) -> pd.DataFrame:
    """Converts WP CSV to SK CSV format, keeping only the necessary columns."""
    _logger.info(" conversion started: convert_wp_to_sk")
    return wp_csv[sk_csv.columns]


@app.get("/api", tags=["Health"])
def health_check():
    """
    Simple health check to verify that the API is alive.
    """
    _logger.info("Health check! Status is ok!")
    return {"status": "ok", "message": "CSV Converter is healthy.", "time": datetime.now()}


@app.post("/api/converters/file/sk-to-wp")
async def convert_sk_to_wp_file(file: UploadFile = File(...)):
    _logger.info(f" conversion started: convert_sk_to_wp_file - received:\n{file}")
    try:
        content = await file.read()
        sk_df = pd.read_csv(io.StringIO(content.decode("utf-8")))
        wp_df = pd.read_csv(WP_REFERENCE_FILE)
        converted_df = convert_sk_to_wp(sk_df, wp_df)
        output = io.StringIO()
        converted_df.to_csv(output, index=False)
        return {"converted_csv": output.getvalue()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/converters/file/wp-to-sk")
async def convert_wp_to_sk_file(file: UploadFile = File(...)):
    _logger.info(f" conversion started: convert_wp_to_sk_file - received:\n{file}")
    try:
        content = await file.read()
        wp_df = pd.read_csv(io.StringIO(content.decode("utf-8")))
        sk_df = pd.read_csv(SK_REFERENCE_FILE)
        converted_df = convert_wp_to_sk(wp_df, sk_df)
        output = io.StringIO()
        converted_df.to_csv(output, index=False)
        return {"converted_csv": output.getvalue()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/converters/text/sk-to-wp")
async def convert_sk_to_wp_text(body: str = Body(..., media_type="text/csv")):
    _logger.info(f" conversion started: convert_sk_to_wp_text - received:\n{body}")
    try:
        sk_df = pd.read_csv(io.StringIO(body))
        wp_df = pd.read_csv(WP_REFERENCE_FILE)
        converted_df = convert_sk_to_wp(sk_df, wp_df)
        output = io.StringIO()
        converted_df.to_csv(output, index=False)
        return {"converted_csv": output.getvalue()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/converters/text/wp-to-sk")
async def convert_wp_to_sk_text(body: str = Body(..., media_type="text/csv")):
    _logger.info(f" conversion started: convert_wp_to_sk_text - received:\n{body}")
    try:
        wp_df = pd.read_csv(io.StringIO(body))
        sk_df = pd.read_csv(SK_REFERENCE_FILE)
        converted_df = convert_wp_to_sk(wp_df, sk_df)
        output = io.StringIO()
        converted_df.to_csv(output, index=False)
        return {"converted_csv": output.getvalue()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    """
    Test Procedure for Command-Line Execution:
    1. Run the script from the terminal with the following command:
       python main.py [direction] [input_file] [output_file]
    2. Replace placeholders with actual file paths and desired conversion direction.
    3. Example usage:
       - SK to WP: `python main.py sk-to-wp sk.csv output.csv`
       - WP to SK: `python main.py wp-to-sk wp.csv output.csv`
    4. The converted file will be saved at the specified output location.
    """

    test_cmd = False  # False means test via IDE, True means command line with args
    direction = SK_TO_WP
    input_file = SK_REFERENCE_FILE if direction == SK_TO_WP else WP_REFERENCE_FILE
    current_date = datetime.now().isoformat(timespec='seconds')
    output_file = os.path.join(BASE_DIR, "..", "outputs", f"output_{direction}_{current_date}.csv")

    if test_cmd:
        parser = argparse.ArgumentParser(description="Convert CSV files between SK and WP formats")
        parser.add_argument("direction", choices=[SK_TO_WP, WP_TO_SK], help="Conversion direction")
        parser.add_argument("input_file", help="Path to the input CSV file")
        parser.add_argument("output_file", help="Path to save the converted CSV file")
        args = parser.parse_args()
        input_file = args.input_file
        direction = args.direction
        output_file = args.output_file

    input_df = pd.read_csv(input_file)
    if direction == SK_TO_WP:
        reference_df = pd.read_csv(WP_REFERENCE_FILE)
        converted_df = convert_sk_to_wp(input_df, reference_df)
    else:
        reference_df = pd.read_csv(SK_REFERENCE_FILE)
        converted_df = convert_wp_to_sk(input_df, reference_df)

    converted_df.to_csv(output_file, index=False)
    print(f"Converted file saved to {output_file}")
