import json
import great_expectations as ge
from great_expectations.core.expectation_configuration import ExpectationConfiguration

def load_expectations_from_json(json_path):
    with open(json_path, "r") as f:
        return json.load(f)

def create_or_get_suite(context, suite_name: str):
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name)
    return suite

def add_expectations_to_suite(suite, expectations: list, overwrite_existing=True):
    if overwrite_existing:
        existing_keys = {(e.expectation_type, str(e.kwargs)) for e in suite.expectations}
        for exp in expectations:
            key = (exp["expectation_type"], str(exp["kwargs"]))
            if key not in existing_keys:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type=exp["expectation_type"],
                        kwargs=exp["kwargs"]
                    )
                )
    else:
        for exp in expectations:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type=exp["expectation_type"],
                    kwargs=exp["kwargs"]
                )
            )

def save_suite(context, suite):
    context.save_expectation_suite(suite)

def create_suite_from_json(context, suite_name: str, json_path: str):
    expectations = load_expectations_from_json(json_path)
    suite = create_or_get_suite(context, suite_name)
    add_expectations_to_suite(suite, expectations)
    save_suite(context, suite)