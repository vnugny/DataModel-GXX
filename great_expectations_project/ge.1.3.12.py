import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from great_expectations.data_context import DataContext
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
import argparse

def run_data_validation(data_source_path, expectations_suite_path, results_capture_path=None):
    # Load configuration files
    with open(data_source_path, "r") as f:
        data_config = json.load(f)
    with open(expectations_suite_path, "r") as f:
        expectations_config = json.load(f)

    # Load the dataset
    source_type = data_config.get("source_type")
    if source_type == "csv":
        df = pd.read_csv(data_config["file_path"])
    elif source_type == "parquet":
        df = pd.read_parquet(data_config["file_path"])
    elif source_type in ["postgresql", "mysql"]:
        db = data_config["database"]
        engine = create_engine(
            f"{db['type']}://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
        )
        df = pd.read_sql(f"SELECT * FROM {db['table_name']}", engine)
    else:
        raise ValueError("Unsupported source_type. Only 'csv', 'parquet', 'postgresql', and 'mysql' are supported.")

    # Initialize Great Expectations context
    context = DataContext()
    suite_name = expectations_config.get("expectation_suite_name", "default_suite")

    if suite_name not in [s.expectation_suite_name for s in context.list_expectation_suites()]:
        context.create_expectation_suite(expectation_suite_name=suite_name, overwrite_existing=True)

    # Create a RuntimeBatchRequest
    batch_request = RuntimeBatchRequest(
        datasource_name="pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default"}
    )

    # Get a validator and attach expectations
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    for exp in expectations_config["expectations"]:
        validator.expectation_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type=exp["expectation_type"],
                kwargs=exp["kwargs"]
            )
        )

    context.save_expectation_suite(validator.expectation_suite)

    # Run validation
    checkpoint_result = validator.validate()

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_slug = suite_name.replace(" ", "_")
    os.makedirs("validation_results", exist_ok=True)

    summary_file = f"validation_results/{suite_slug}_validation_summary_{timestamp}.json"
    details_file = f"validation_results/{suite_slug}_validation_details_{timestamp}.json"
    html_file = f"validation_results/{suite_slug}_validation_report_{timestamp}.html"

    with open(summary_file, "w") as f:
        json.dump({"success": checkpoint_result.success, "statistics": checkpoint_result.statistics}, f, indent=4)

    with open(details_file, "w") as f:
        json.dump(checkpoint_result.to_json_dict()["results"], f, indent=4)

    # Save HTML report
    document_model = ValidationResultsPageRenderer().render(checkpoint_result)
    html_content = DefaultJinjaPageView().render(document_model)

    with open(html_file, "w") as f:
        f.write(html_content)

    print("\n✅ Validation completed!")
    print("Summary file:", summary_file)
    print("Details file:", details_file)
    print("HTML report:", html_file)
    print("Success:", checkpoint_result.success)

    return {
        "summary_file": summary_file,
        "details_file": details_file,
        "html_file": html_file,
        "success": checkpoint_result.success
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Great Expectations validation using local config files.")
    parser.add_argument("--data-source", required=True, help="Path to data_source_config.json")
    parser.add_argument("--expectations", required=True, help="Path to expectations_suite.json")
    parser.add_argument("--results", required=False, help="Path to results_capture.json (optional)")
    args = parser.parse_args()

    run_data_validation(args.data_source, args.expectations, args.results)
