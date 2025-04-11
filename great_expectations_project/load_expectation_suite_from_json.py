import great_expectations as gx
import json

def load_expectation_suite_from_json(json_file_path: str):
    """
    Loads an expectation suite from a JSON file and adds expectations correctly for GE v1.3.13.

    :param json_file_path: Path to the expectation suite JSON file.
    :return: Expectation suite object.
    """
    context = gx.get_context()

    with open(json_file_path, "r") as f:
        suite_dict = json.load(f)

    suite_name = suite_dict["expectation_suite_name"]

    try:
        suite = context.get_expectation_suite(expectation_suite_name=suite_name)
        print(f"Suite '{suite_name}' already exists. Updating existing suite.")
    except Exception:
        suite = context.create_expectation_suite(expectation_suite_name=suite_name)
        print(f"Created new suite '{suite_name}'.")

    # Correctly add expectations directly from JSON
    for exp in suite_dict["expectations"]:
        suite.add_expectation(exp)

    context.save_expectation_suite(suite)

    return suite

# Example usage
if __name__ == "__main__":
    json_path = "path/to/your_expectation_suite.json"
    load_expectation_suite_from_json(json_path)
