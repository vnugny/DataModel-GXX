
import pandas as pd
import json
import logging
import psycopg2
from great_expectations.data_context import DataContext
from great_expectations.exceptions import GreatExpectationsError

# Setup logging
logging.basicConfig(
    filename='gx_rule_application.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

def load_rules_from_postgres_table(host, port, database, user, password, table_name):
    """
    Load rules from a PostgreSQL rule table.
    """
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, connection)
        logger.info(f"Loaded {len(df)} rules from PostgreSQL table '{table_name}'")
        return df
    except Exception as e:
        logger.error(f"Error loading rules from PostgreSQL: {e}", exc_info=True)
        raise
    finally:
        if connection:
            connection.close()


def add_expectations_from_rules(context: DataContext, suite_name: str, rules_df: pd.DataFrame):
    """
    Adds expectations to an expectation suite from rules dataframe including rule_id, row_condition, and group_by.
    """
    try:
        logger.info(f"Creating or loading suite: {suite_name}")
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

        for index, row in rules_df.iterrows():
            try:
                expectation_type = row['expectation_type']
                column_name = row['column_name']
                rule_params = row['rule_params']
                rule_id = row['rule_id']
                rule_name = row.get('rule_name', 'N/A')
                severity = row.get('severity', 'Medium')
                row_condition = row.get('row_condition', None)
                group_by = row.get('group_by', None)

                # Ensure rule_params is a dict
                if isinstance(rule_params, str):
                    rule_params = json.loads(rule_params)

                # Add the column if not already specified
                if 'column' not in rule_params:
                    rule_params['column'] = column_name

                # Build expectation config
                expectation = {
                    "expectation_type": expectation_type,
                    "kwargs": rule_params,
                    "meta": {
                        "rule_id": rule_id,
                        "rule_name": rule_name,
                        "severity": severity
                    }
                }

                if row_condition:
                    expectation['kwargs']['row_condition'] = row_condition

                if group_by:
                    if isinstance(group_by, str):
                        group_by = [col.strip() for col in group_by.split(",")]
                    expectation['kwargs']['group_by'] = group_by

                suite.add_expectation(expectation_configuration=expectation)
                logger.info(f"Added expectation for rule_id: {rule_id}")

            except Exception as rule_error:
                logger.error(f"Error processing rule_id: {row.get('rule_id')} - {rule_error}", exc_info=True)

        context.save_expectation_suite(suite)
        logger.info(f"Saved suite '{suite_name}' with {len(rules_df)} expectations.")

    except GreatExpectationsError as ge_error:
        logger.critical(f"Great Expectations error occurred: {ge_error}", exc_info=True)
    except Exception as e:
        logger.critical(f"Critical error occurred: {e}", exc_info=True)


# Example usage
if __name__ == "__main__":
    try:
        # Load rules from PostgreSQL
        df_rules = load_rules_from_postgres_table(
            host='localhost',
            port='5432',
            database='mydb',
            user='myuser',
            password='mypassword',
            table_name='rules_table'
        )

        context = DataContext()
        add_expectations_from_rules(context, suite_name="customer_data_suite", rules_df=df_rules)

    except Exception as main_error:
        logger.critical(f"Unhandled error in main execution: {main_error}", exc_info=True)
