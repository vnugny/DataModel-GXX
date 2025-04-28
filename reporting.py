from great_expectations.data_context import DataContext

context = DataContext()
batch_identifier = {
    "datasource_name": "your_datasource",
    "data_connector_name": "your_data_connector",
    "data_asset_name": "your_data_asset"
}
context.build_data_docs()
context.open_data_docs(resource_identifier=batch_identifier)