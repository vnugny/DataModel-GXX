import json
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

def import_expectation_suite_from_json(json_file_path: str, suite_name: str):
    # Load JSON content
    with open(json_file_path, "r") as f:
        suite_dict = json.load(f)

    # Load context
    context = gx.get_context()

    # Convert JSON to ExpectationSuite object
    suite = ExpectationSuite(**suite_dict)
    suite.expectation_suite_name = suite_name

    # Save to context
    context.save_expectation_suite(suite)
    print(f"Expectation suite '{suite_name}' successfully added to context.")

# Example usage
if __name__ == "__main__":
    import_expectation_suite_from_json("expectation_suite.json", "my_suite")