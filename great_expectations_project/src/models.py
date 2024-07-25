from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

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
