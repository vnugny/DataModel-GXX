import great_expectations as ge
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
import json

# Define SQLAlchemy ORM models (as previously defined)
Base = declarative_base()

class Datasource(Base):
    __tablename__ = 'datasources'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    connection_string = Column(String, nullable=False)

class ExpectationSuite(Base):
    __tablename__ = 'expectation_suites'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)

class Expectation(Base):
    __tablename__ = 'expectations'
    id = Column(Integer, primary_key=True)
    suite_id = Column(Integer, ForeignKey('expectation_suites.id'), nullable=False)
    expectation_type = Column(String, nullable=False)
    kwargs = Column(Text, nullable=False)

class Batch(Base):
    __tablename__ = 'batches'
    id = Column(Integer, primary_key=True)
    datasource_id = Column(Integer, ForeignKey('datasources.id'), nullable=False)
    batch_identifier = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

class Validation(Base):
    __tablename__ = 'validations'
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey('batches.id'), nullable=False)
    suite_id = Column(Integer, ForeignKey('expectation_suites.id'), nullable=False)
    success = Column(Boolean, nullable=False)
    run_time = Column(DateTime, default=datetime.datetime.utcnow)

class ValidationResult(Base):
    __tablename__ = 'validation_results'
    id = Column(Integer, primary_key=True)
    validation_id = Column(Integer, ForeignKey('validations.id'), nullable=False)
    expectation_id = Column(Integer, ForeignKey('expectations.id'), nullable=False)
    success = Column(Boolean, nullable=False)
    result = Column(Text)

# Connect to PostgreSQL
engine = create_engine('postgresql://username:password@localhost/great_expectations')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Function to run validations and capture results
def run_and_capture_results(context, batch_kwargs, suite_name, datasource_name, datasource_connection_string):
    # Run the expectation suite
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[context.get_batch(batch_kwargs, suite_name)],
        run_id="customer_data_validation"
    )

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

# Example usage
context = ge.data_context.DataContext()
batch_kwargs = {"path": "path/to/your/customer_data.csv", "datasource": "your_datasource_name"}
run_and_capture_results(context, batch_kwargs, "customer_data_suite", "customer_datasource", "postgresql://username:password@localhost/customerdb")
