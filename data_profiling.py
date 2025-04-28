from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from great_expectations.data_context import DataContext

context = DataContext()
batch_request = {
    "datasource_name": "your_datasource",
    "data_connector_name": "your_data_connector",
    "data_asset_name": "your_data_asset",
    "limit": 1000
}
batch = context.get_batch(batch_request=batch_request)
profiler = UserConfigurableProfiler(profile_dataset=batch)
expectation_suite = profiler.build_suite()
context.save_expectation_suite(expectation_suite, 'profiling_suite')