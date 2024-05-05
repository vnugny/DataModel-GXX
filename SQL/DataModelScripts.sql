-- Table to hold suites of expectations
CREATE TABLE ExpectationSuites (
    SuiteID INT PRIMARY KEY AUTO_INCREMENT,
    SuiteName VARCHAR(255) NOT NULL,
    SuiteDescription TEXT
);

-- Table to store individual expectations belonging to specific suites
CREATE TABLE Expectations (
    ExpectationID INT PRIMARY KEY AUTO_INCREMENT,
    SuiteID INT NOT NULL,
    ExpectationName VARCHAR(255) NOT NULL,
    ExpectationType VARCHAR(255) NOT NULL,  -- e.g., "expect_column_values_to_be_unique"
    ColumnName VARCHAR(255),
    Parameters JSON,  -- Store additional parameters as JSON
    FOREIGN KEY (SuiteID) REFERENCES ExpectationSuites(SuiteID)
);

-- Table to hold validation runs for each suite
CREATE TABLE Validations (
    ValidationID INT PRIMARY KEY AUTO_INCREMENT,
    SuiteID INT NOT NULL,
    DatasetName VARCHAR(255),
    ValidationTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    Status ENUM('Success', 'Failed') NOT NULL,
    FOREIGN KEY (SuiteID) REFERENCES ExpectationSuites(SuiteID)
);

-- Table to hold detailed results of each expectation in a validation run
CREATE TABLE ValidationResults (
    ValidationResultID INT PRIMARY KEY AUTO_INCREMENT,
    ValidationID INT NOT NULL,
    ExpectationID INT NOT NULL,
    Status ENUM('Success', 'Failed') NOT NULL,
    ObservedValue JSON,  -- Record observed values for the expectation
    FOREIGN KEY (ValidationID) REFERENCES Validations(ValidationID),
    FOREIGN KEY (ExpectationID) REFERENCES Expectations(ExpectationID)
);


-- Sample data for ExpectationSuites table
INSERT INTO ExpectationSuites (SuiteName, SuiteDescription)
VALUES 
('Customer Data Quality Suite', 'Check basic data quality rules for the customer dataset.'),
('Sales Data Quality Suite', 'Validation rules specific to sales data.');

-- Sample data for Expectations table
INSERT INTO Expectations (SuiteID, ExpectationName, ExpectationType, ColumnName, Parameters)
VALUES 
(1, 'Unique Customer IDs', 'expect_column_values_to_be_unique', 'customer_id', '{"mostly": 1.0}'),
(1, 'Non-empty First Names', 'expect_column_values_to_not_be_null', 'first_name', '{}'),
(2, 'Non-negative Sales Amount', 'expect_column_values_to_be_greater_than', 'sales_amount', '{"value": 0}'),
(2, 'Non-empty Product Codes', 'expect_column_values_to_not_be_null', 'product_code', '{}');

-- Sample data for Validations table
INSERT INTO Validations (SuiteID, DatasetName, ValidationTimestamp, Status)
VALUES 
(1, 'Customer Data January 2024', '2024-01-31 12:00:00', 'Success'),
(1, 'Customer Data February 2024', '2024-02-28 12:00:00', 'Failed'),
(2, 'Sales Data Q1 2024', '2024-04-01 12:30:00', 'Success');

-- Sample data for ValidationResults table
INSERT INTO ValidationResults (ValidationID, ExpectationID, Status, ObservedValue)
VALUES 
(1, 1, 'Success', '{"total": 1000, "unique": 1000}'),
(1, 2, 'Success', '{"total": 1000, "missing": 0}'),
(2, 1, 'Failed', '{"total": 1100, "unique": 1098}'),
(2, 2, 'Success', '{"total": 1100, "missing": 0}'),
(3, 3, 'Success', '{"total": 500, "non_negative": 500}'),
(3, 4, 'Success', '{"total": 500, "missing": 0}');
