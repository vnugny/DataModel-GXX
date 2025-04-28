from great_expectations.data_context import DataContext

context = DataContext()
datasource_config = {
    "name": "spark_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier"]
        }
    }
}
context.test_yaml_config(yaml.dump(datasource_config))