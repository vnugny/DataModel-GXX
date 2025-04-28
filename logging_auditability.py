from great_expectations.data_context import DataContext

context = DataContext()
result = context.run_validation_operator("action_list_operator", assets_to_validate=[])
context.build_data_docs()