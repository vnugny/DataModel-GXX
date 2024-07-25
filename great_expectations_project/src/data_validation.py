import great_expectations as ge
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base, Datasource, ExpectationSuite, Expectation, Batch, Validation, ValidationResult
import datetime
import json

def run_and_capture_results(context, batch_kwargs, suite_name, datasource_name, datasource_connection_string):
    # Run the expectation suite
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[context.get_batch(batch_kwargs, suite_name)],
        run_id="customer_data_validation"
    )

    # Connect to PostgreSQL
    engine = create_engine(datasource_connection_string)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Insert datasource if not exists
    datasource = session.query(Datasource).filter_by(name=datasource_name).first()
    if not datasource:
        datasource = Datasource(name=datasource_name, connection_string=datasource_connection_string)
        session.add(datasource)
        session.commit()

    # Insert expectation suite if not exists
    suite = session.query(ExpectationSuite).filter_by(name=suite_name).first()
    if not suite:
        suite = ExpectationSuite(name=suite_name, description="Expectations for customer data validation.")
        session.add(suite)
        session.commit()

    # Insert expectations if not exists
    expectation_configs = context.get_expectation_suite(suite_name).expectations
    for exp in expectation_configs:
        expectation = session.query(Expectation).filter_by(suite_id=suite.id, expectation_type=exp.expectation_type, kwargs=json.dumps(exp.kwargs)).first()
        if not expectation:
            expectation = Expectation(suite_id=suite.id, expectation_type=exp.expectation_type, kwargs=json.dumps(exp.kwargs))
            session.add(expectation)
            session.commit()

    # Insert batch
    batch_identifier = batch_kwargs['path']
    batch = Batch(datasource_id=datasource.id, batch_identifier=batch_identifier)
    session.add(batch)
    session.commit()

    # Insert validation
    validation = Validation(batch_id=batch.id, suite_id=suite.id, success=results["success"])
    session.add(validation)
    session.commit()

    # Insert validation results
    for result in results['results']:
        expectation = session.query(Expectation).filter_by(suite_id=suite.id, expectation_type=result['expectation_config']['expectation_type'], kwargs=json.dumps(result['expectation_config']['kwargs'])).first()
        validation_result = ValidationResult(validation_id=validation.id, expectation_id=expectation.id, success=result['success'], result=json.dumps(result['result']))
        session.add(validation_result)
        session.commit()
