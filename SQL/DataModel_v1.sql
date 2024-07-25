CREATE TABLE datasources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    connection_string VARCHAR(255) NOT NULL
);

CREATE TABLE expectation_suites (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE expectations (
    id SERIAL PRIMARY KEY,
    suite_id INTEGER NOT NULL,
    expectation_type VARCHAR(255) NOT NULL,
    kwargs TEXT NOT NULL,
    FOREIGN KEY (suite_id) REFERENCES expectation_suites (id) ON DELETE CASCADE
);

CREATE TABLE batches (
    id SERIAL PRIMARY KEY,
    datasource_id INTEGER NOT NULL,
    batch_identifier VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (datasource_id) REFERENCES datasources (id) ON DELETE CASCADE
);

CREATE TABLE validations (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    suite_id INTEGER NOT NULL,
    success BOOLEAN NOT NULL,
    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (batch_id) REFERENCES batches (id) ON DELETE CASCADE,
    FOREIGN KEY (suite_id) REFERENCES expectation_suites (id) ON DELETE CASCADE
);

CREATE TABLE validation_results (
    id SERIAL PRIMARY KEY,
    validation_id INTEGER NOT NULL,
    expectation_id INTEGER NOT NULL,
    success BOOLEAN NOT NULL,
    result TEXT,
    FOREIGN KEY (validation_id) REFERENCES validations (id) ON DELETE CASCADE,
    FOREIGN KEY (expectation_id) REFERENCES expectations (id) ON DELETE CASCADE
);




INSERT INTO datasources (name, connection_string) VALUES
('customer_datasource', 'postgresql://username:password@localhost/customerdb');

INSERT INTO expectation_suites (name, description) VALUES
('customer_data_suite', 'Expectations for customer data validation.');

INSERT INTO expectations (suite_id, expectation_type, kwargs) VALUES
(1, 'expect_column_to_exist', '{"column": "customer_id"}'),
(1, 'expect_column_values_to_be_unique', '{"column": "customer_id"}'),
(1, 'expect_column_to_exist', '{"column": "name"}'),
(1, 'expect_column_values_to_not_be_null', '{"column": "name"}'),
(1, 'expect_column_to_exist', '{"column": "email"}'),
(1, 'expect_column_values_to_match_regex', '{"column": "email", "regex": "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"}'),
(1, 'expect_column_values_to_be_unique', '{"column": "email"}'),
(1, 'expect_column_to_exist', '{"column": "signup_date"}'),
(1, 'expect_column_values_to_not_be_null', '{"column": "signup_date"}'),
(1, 'expect_column_to_exist', '{"column": "age"}'),
(1, 'expect_column_values_to_be_between', '{"column": "age", "min_value": 18, "max_value": 99}');
