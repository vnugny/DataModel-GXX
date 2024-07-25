import great_expectations as ge
from src.data_validation import run_and_capture_results

# Initialize Great Expectations context
context = ge.data_context.DataContext()

# Define batch kwargs
batch_kwargs = {"path": "path/to/your/customer_data.csv", "datasource": "your_datasource_name"}

# Run validations and capture results
run_and_capture_results(context, batch_kwargs, "customer_data_suite", "customer_datasource", "postgresql://username:password@localhost/great_expectations")
