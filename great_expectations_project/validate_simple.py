import great_expectations as gx
import pandas as pd
import json
import os
import logging
import re
import time
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, JSON, MetaData
from datetime import datetime

# === Configure Logging ===
logging.basicConfig(
    filename="validation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def validate_data(data_source_path: str, expectations_suite_path: str, results_config_path: str = None):
    """
    Perform data validation using Great Expectations by loading an expectation suite directly from a JSON file.

    :param data_source_path: Path to the data source JSON file.
    :param expectations_suite_path: Path to the expectation suite JSON file.
    :param results_config_path: (Optional) Path to the results capture JSON file.
    """
    logging.info("Starting data validation process.")

    try:
        # === Load Configurations ===
        with open(data_source_path, "r") as f:
            config = json.load(f)

        # Load Expectation Suite JSON
        with open(expectations_suite_path, "r") as f:
            expectation_suite = json.load(f)

        # Load Centralized DB Config if provided
        db_config = None
        if results_config_path and os.path.exists(results_config_path):
            with open(results_config_path, "r") as f:
                results_config = json.load(f)
            db_config = results_config.get("centralized_database")

        source_type = config.get("source_type")

        if not source_type:
            raise ValueError("source_type is missing in configuration.")

        # === Load Data ===
        if source_type in ["csv", "parquet"]:
            file_path = config.get("file_path")
            if not file_path or not os.path.exists(file_path):
                raise FileNotFoundError(f"File {file_path} does not exist!")
            df = pd.read_csv(file_path) if source_type == "csv" else pd.read_parquet(file_path)

        elif source_type in ["postgresql", "mysql"]:
            db_config = config.get("database")
            if not db_config:
                raise ValueError("Database configuration is missing.")

            try:
                engine = create_engine(
                    f"{db_config['type']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
                )
                df = pd.read_sql(f"SELECT * FROM {db_config['table_name']}", engine)
            except Exception as e:
                logging.error(f"Database connection failed: {e}")
                raise
        else:
            raise ValueError("Invalid source_type in configuration file!")

        logging.info(f"File or database source validated successfully. Proceeding with data validation.")

        # === Initialize Great Expectations ===
        context = gx.get_context()
        suite_name = expectation_suite.get("expectation_suite_name", "default_suite")

        # Save expectation suite
        context.add_expectation_suite(expectation_suite_name=suite_name, expectations=expectation_suite["expectations"])

        # Create a Validator
        validator = context.get_validator(batch_kwargs={"dataset": df}, expectation_suite_name=suite_name)

        # === Run Validation ===
        results = context.run_checkpoint(checkpoint_name="dynamic_checkpoint")

        # Extract detailed records
        detailed_records = [
            {
                "expectation_type": result["expectation_config"]["expectation_type"],
                "column_name": result["expectation_config"].get("kwargs", {}).get("column", "N/A"),
                "success": str(result["success"]),
                "unexpected_count": result["result"].get("unexpected_count", 0),
                "unexpected_list": json.dumps(result["result"].get("unexpected_list", []))
            }
            for result in results.to_json_dict().get("results", [])
        ]

        # === Save Results in JSON ===
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(f"validation_results/{timestamp}", exist_ok=True)

        with open(f"validation_results/{timestamp}/validation_summary.json", "w") as f:
            json.dump({"success": results.success, "statistics": results.statistics}, f, indent=4)

        with open(f"validation_results/{timestamp}/validation_details.json", "w") as f:
            json.dump(detailed_records, f, indent=4)

        # === Store Results in Centralized Database (if available) ===
        if db_config:
            centralized_engine = create_engine(
                f"{db_config['type']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )

            metadata = MetaData()

            validation_results = Table(
                "validation_results",
                metadata,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("timestamp", String),
                Column("success", String),
                Column("evaluated_expectations", Integer),
                Column("successful_expectations", Integer),
                Column("unsuccessful_expectations", Integer),
                Column("success_percent", Float),
                Column("summary", JSON),
                Column("details_record", JSON)  # Stores detailed validation records
            )

            metadata.create_all(centralized_engine)

            summary_record = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "success": str(results.success),
                "evaluated_expectations": results.statistics["evaluated_expectations"],
                "successful_expectations": results.statistics["successful_expectations"],
                "unsuccessful_expectations": results.statistics["unsuccessful_expectations"],
                "success_percent": results.statistics["success_percent"],
                "summary": json.dumps(results.to_json_dict()["results"]),
                "details_record": json.dumps(detailed_records)
            }

            with centralized_engine.connect() as conn:
                conn.execute(validation_results.insert().values(summary_record))

            logging.info("Validation completed successfully and results stored in centralized database.")

        logging.info("Validation completed successfully.")
        print("Validation completed. Results saved.")

    except Exception as e:
        logging.error(f"Validation failed: {e}")
        print(f"Error: {e}")
