import json
import pandas as pd
import logging
import sys
from great_expectations.data_context import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset, SqlAlchemyDataset
from sqlalchemy import create_engine
from great_expectations.exceptions import GreatExpectationsError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_json_file(filepath):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON file: {filepath}")
        sys.exit(1)

def load_data_asset(config):
    data_asset_type = config["data_asset_type"]
    data_asset_path = config["data_asset_path"]
    connection_string = config.get("connection_string")

    if data_asset_type == "flat_file":
        data_dict = load_json_file(data_asset_path)
        return PandasDataset(pd.DataFrame(data_dict))
    elif data_asset_type == "database_table":
        if connection_string is None:
            logging.error("Connection string is required for database tables.")
            sys.exit(1)
        engine = create_engine(connection_string)
        return SqlAlchemyDataset(data_asset_path, engine=engine)
    else:
        logging.error(f"Unsupported data asset type: {data_asset_type}")
        sys.exit(1)

def main(expectation_suite_path, data_asset_config_path):
    # Load expectation suite from JSON file
    expectation_suite_dict = load_json_file(expectation_suite_path)
    expectation_suite = ExpectationSuite(expectation_suite_dict)
    logging.info("Loaded expectation suite.")

    # Load data asset configuration
    data_asset_config = load_json_file(data_asset_config_path)
    data_asset = load_data_asset(data_asset_config)
    logging.info("Loaded data asset.")

    try:
        # Initialize DataContext
        context = DataContext()

        # Validate data asset against expectation suite
        results = context.run_validation_operator(
            "action_list_operator",
            assets_to_validate=[data_asset],
            run_id="data_quality_check"
        )

        # Capture summary and details of expectation results
        summary = {
            "success": results["success"],
            "statistics": results["statistics"]
        }
        details = results["results"]

        # Save summary to JSON file
        with open('../results/summary.json', 'w') as f:
            json.dump(summary, f, indent=4)

        # Save details to JSON file
        with open('../results/details.json', 'w') as f:
            json.dump(details, f, indent=4)

        logging.info("Validation results saved to summary.json and details.json")

    except GreatExpectationsError as e:
        logging.error(f"Great Expectations error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Usage: python run_checks.py <expectation_suite_path> <data_asset_config_path>")
        sys.exit(1)

    expectation_suite_path = sys.argv[1]
    data_asset_config_path = sys.argv[2]

    main(expectation_suite_path, data_asset_config_path)
