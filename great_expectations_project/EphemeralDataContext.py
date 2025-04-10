import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import argparse
import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.render.renderer.v1.render_validation_results import ValidationResultsPageRenderer
from great_expectations.render.view.view import DefaultJinjaPageView
from pyspark.sql import SparkSession

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
        engine_type = "pandas"
    elif source_type == "parquet":
        df = pd.read_parquet(data_config["file_path"])
        engine_type = "pandas"
    elif source_type == "spark_csv":
        spark = SparkSession.builder.appName("GE_Spark_Validation").getOrCreate()
        df = spark.read.option("header", True).csv(data_config["file_path"])
        engine_type = "spark"
    elif source_type in ["postgresql", "mysql"]:
        db = data_config["database"]
        engine = create_engine(
            f"{db['type']}://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
        )
        df = pd.read_sql(f"SELECT * FROM {db['table_name']}", engine)
        engine_type = "pandas"
    else:
        raise ValueError("Unsupported source_type. Only 'csv', 'parquet', 'spark_csv', 'postgresql', and 'mysql' are supported.")

    # Initialize Great Expectations context (must be a full GE project)
    context = gx.get_context()

    # Create or get expectation suite
    suite_name = expectations_config.get("expectation_suite_name", "default_suite")
    try:
        context.get_expectation_suite(suite_name)
    except Exception:
        context.add_expectation_suite(expectation_suite_name=suite_name)

    # Create or load Fluent-style datasource
    if engine_type == "spark":
        if "spark_filesystem_datasource" not in context.datasources:
            context.sources.add_spark(name="spark_filesystem_datasource")
        datasource_name = "spark_filesystem_datasource"
    else:
        if "pandas_filesystem_datasource" not in context.datasources:
            context.sources.add_pandas(name="pandas_filesystem_datasource")
        datasource_name = "pandas_filesystem_datasource"

    # Use in-memory asset
    asset = context.datasources[datasource_name].add_dataframe_asset(
        name="temp_asset", dataframe=df
    )

    batch_request = asset.build_batch_request()

    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Apply expectations
    for exp in expectations_config["expectations"]:
        getattr(validator, exp["expectation_type"])(**exp["kwargs"])

    # Save suite
    validator.save_expectation_suite()

    # Run validation
    results = validator.validate()

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_slug = suite_name.replace(" ", "_")
    os.makedirs("validation_results", exist_ok=True)

    summary_file = f"validation_results/{suite_slug}_validation_summary_{timestamp}.json"
    details_file = f"validation_results/{suite_slug}_validation_details_{timestamp}.json"
    html_file = f"validation_results/{suite_slug}_validation_report_{timestamp}.html"

    with open(summary_file, "w") as f:
        json.dump({"success": results.success, "statistics": results.statistics}, f, indent=4)

    with open(details_file, "w") as f:
        json.dump(results.to_json_dict()["results"], f, indent=4)

    # Save HTML report
    document_model = ValidationResultsPageRenderer().render(results)
    html_content = DefaultJinjaPageView().render(document_model)

    with open(html_file, "w") as f:
        f.write(html_content)

    print("\n✅ Validation completed!")
    print("Summary file:", summary_file)
    print("Details file:", details_file)
    print("HTML report:", html_file)
    print("Success:", results.success)

    return {
        "summary_file": summary_file,
        "details_file": details_file,
        "html_file": html_file,
        "success": results.success
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Great Expectations validation using local config files.")
    parser.add_argument("--data-source", required=True, help="Path to data_source_config.json")
    parser.add_argument("--expectations", required=True, help="Path to expectations_suite.json")
    parser.add_argument("--results", required=False, help="Path to results_capture.json (optional)")
    args = parser.parse_args()

    run_data_validation(args.data_source, args.expectations, args.results)
