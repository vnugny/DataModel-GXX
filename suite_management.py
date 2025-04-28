from great_expectations.data_context import DataContext

context = DataContext()
suite_name = "customer_data_suite"
suite = context.create_expectation_suite(suite_name)
# Add or update expectations here
context.save_expectation_suite(suite)