from great_expectations.data_context import DataContext

context = DataContext()
suite = context.create_expectation_suite("order_suite", overwrite_existing=True)
batch_request = {
    "datasource_name": "your_datasource",
    "data_connector_name": "your_data_connector",
    "data_asset_name": "your_data_asset"
}
batch = context.get_batch(batch_request=batch_request)
batch.expect_column_values_to_be_in_set("status", ["paid", "unpaid", "shipped"])
context.save_expectation_suite(suite)