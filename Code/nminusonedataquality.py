"""
Data Quality Framework using Great Expectations
==============================================

This module implements a comprehensive data quality checking framework
using the Great Expectations library.

Features:
- Flexible configuration of expectations for different data sources
- Support for CSV, Pandas DataFrame, and SQL data
- Automatic validation report generation
- Configurable notification system
- Extensible architecture through inheritance
"""

import os
import sys
import json
import logging
from datetime import datetime
import numpy as np
from typing import Dict, List, Union, Optional, Any, Tuple

import pandas as pd
import great_expectations as ge
from great_expectations.dataset import PandasDataset
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.util import get_approximate_percentile_disc_sql


class DataQualityConfig:
    """Configuration class for the data quality framework."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize the configuration for data quality checks.
        
        Args:
            config_path: Path to the JSON configuration file
        """
        self.config_path = config_path
        self.config = self._load_config() if config_path else {}
        
    def _load_config(self) -> Dict:
        """Load configuration from JSON file."""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading config file: {e}")
            return {}
    
    def get_datasource_config(self, source_name: str) -> Dict:
        """
        Get configuration for a specific data source.
        
        Args:
            source_name: Name of the data source
            
        Returns:
            Configuration dictionary for the specified source
        """
        return self.config.get('datasources', {}).get(source_name, {})
    
    def get_expectations(self, source_name: str) -> List[Dict]:
        """
        Get list of expectations for a specific data source.
        
        Args:
            source_name: Name of the data source
            
        Returns:
            List of expectation configurations
        """
        return self.config.get('expectations', {}).get(source_name, [])
    
    def get_notification_config(self) -> Dict:
        """Get notification configuration."""
        return self.config.get('notifications', {})


class DataQualityChecker:
    """Core class for data quality checking using Great Expectations."""
    
    def __init__(self, config: DataQualityConfig = None):
        """
        Initialize the data quality checker.
        
        Args:
            config: Configuration object for the checker
        """
        self.config = config or DataQualityConfig()
        self.logger = self._setup_logging()
        self.results = {}
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging for the data quality checker."""
        logger = logging.getLogger("DataQualityChecker")
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
    
    def load_dataframe(self, df: pd.DataFrame, source_name: str) -> PandasDataset:
        """
        Load a pandas DataFrame as a Great Expectations dataset.
        
        Args:
            df: Pandas DataFrame to load
            source_name: Name of the data source
            
        Returns:
            Great Expectations PandasDataset
        """
        self.logger.info(f"Loading DataFrame for source: {source_name}")
        return ge.dataset.PandasDataset(df)
    
    def load_csv(self, file_path: str, source_name: str) -> PandasDataset:
        """
        Load a CSV file as a Great Expectations dataset.
        
        Args:
            file_path: Path to the CSV file
            source_name: Name of the data source
            
        Returns:
            Great Expectations PandasDataset
        """
        self.logger.info(f"Loading CSV from {file_path} for source: {source_name}")
        df = pd.read_csv(file_path)
        return self.load_dataframe(df, source_name)
    
    def apply_expectations(self, dataset: PandasDataset, source_name: str) -> Dict:
        """
        Apply defined expectations to a dataset.
        
        Args:
            dataset: Great Expectations dataset
            source_name: Name of the data source
            
        Returns:
            Dictionary with validation results
        """
        expectations = self.config.get_expectations(source_name)
        
        if not expectations:
            self.logger.warning(f"No expectations defined for source: {source_name}")
            # Auto-profile the dataset if no expectations are defined
            return self.profile_dataset(dataset, source_name)
            
        self.logger.info(f"Applying {len(expectations)} expectations to {source_name}")
        
        for expectation in expectations:
            try:
                expectation_type = expectation.pop('expectation_type')
                method = getattr(dataset, expectation_type)
                method(**expectation)
            except AttributeError:
                self.logger.error(f"Unknown expectation type: {expectation_type}")
            except Exception as e:
                self.logger.error(f"Error applying expectation {expectation_type}: {e}")
        
        validation_result = dataset.validate()
        self.results[source_name] = validation_result
        return validation_result
    
    def profile_dataset(self, dataset: PandasDataset, source_name: str) -> Dict:
        """
        Profile a dataset using Great Expectations profiler.
        
        Args:
            dataset: Great Expectations dataset
            source_name: Name of the data source
            
        Returns:
            Dictionary with profiling results
        """
        self.logger.info(f"Profiling dataset for source: {source_name}")
        profile_result = BasicDatasetProfiler.profile(dataset)
        self.results[source_name] = profile_result
        return profile_result
    
    def validate_dataframe(self, df: pd.DataFrame, source_name: str) -> Dict:
        """
        Validate a pandas DataFrame against expectations.
        
        Args:
            df: Pandas DataFrame to validate
            source_name: Name of the data source
            
        Returns:
            Dictionary with validation results
        """
        dataset = self.load_dataframe(df, source_name)
        return self.apply_expectations(dataset, source_name)
    
    def validate_csv(self, file_path: str, source_name: str) -> Dict:
        """
        Validate a CSV file against expectations.
        
        Args:
            file_path: Path to the CSV file
            source_name: Name of the data source
            
        Returns:
            Dictionary with validation results
        """
        dataset = self.load_csv(file_path, source_name)
        return self.apply_expectations(dataset, source_name)
    
    def generate_report(self, source_name: Optional[str] = None, 
                       output_path: Optional[str] = None) -> str:
        """
        Generate a validation report.
        
        Args:
            source_name: Name of the data source (if None, report on all sources)
            output_path: Path to save the report (if None, return as string)
            
        Returns:
            Report as a string or path to the saved report
        """
        self.logger.info("Generating validation report")
        
        if source_name and source_name in self.results:
            report_data = {source_name: self.results[source_name]}
        else:
            report_data = self.results
            
        report = json.dumps(report_data, indent=2)
        
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(report)
            return output_path
        else:
            return report
    
    def send_notifications(self, report: str) -> None:
        """
        Send notifications based on validation results.
        
        Args:
            report: Validation report to include in notifications
        """
        notification_config = self.config.get_notification_config()
        if not notification_config:
            return
            
        self.logger.info("Sending notifications")
        
        # This is a placeholder for notification implementation
        # You would implement actual notification logic here (email, Slack, etc.)
        if notification_config.get('email'):
            self.logger.info(f"Would send email to {notification_config['email']}")
        
        if notification_config.get('slack'):
            self.logger.info(f"Would send Slack message to {notification_config['slack']}")


class DataAssistant:
    """Class for suggesting data quality expectations based on data analysis."""
    
    def __init__(self, logger=None):
        """
        Initialize the Data Assistant.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger or logging.getLogger("DataAssistant")
        
    def analyze_column(self, df: pd.DataFrame, column: str) -> Dict:
        """
        Analyze a single column and return its characteristics.
        
        Args:
            df: Pandas DataFrame
            column: Column name to analyze
            
        Returns:
            Dictionary with column characteristics
        """
        col_data = df[column]
        result = {
            "name": column,
            "dtype": str(col_data.dtype),
            "null_count": col_data.isnull().sum(),
            "null_percent": round(col_data.isnull().mean() * 100, 2),
        }
        
        # Add numeric metrics
        if np.issubdtype(col_data.dtype, np.number):
            result.update({
                "min": col_data.min(),
                "max": col_data.max(),
                "mean": col_data.mean(),
                "median": col_data.median(),
                "std": col_data.std(),
                "is_numeric": True,
                "unique_count": col_data.nunique(),
                "unique_percent": round(col_data.nunique() / len(col_data) * 100, 2)
            })
        # Add string metrics
        elif col_data.dtype == 'object':
            # Get length stats for strings
            str_lengths = col_data.dropna().astype(str).str.len()
            if not str_lengths.empty:
                result.update({
                    "min_length": str_lengths.min(),
                    "max_length": str_lengths.max(),
                    "mean_length": str_lengths.mean(),
                    "is_string": True,
                    "unique_count": col_data.nunique(),
                    "unique_percent": round(col_data.nunique() / len(col_data) * 100, 2)
                })
            
            # Check if it looks like a categorical column
            if col_data.nunique() < min(20, len(col_data) * 0.1):
                result["probable_category"] = True
                result["value_counts"] = col_data.value_counts().head(10).to_dict()
        
        # Add datetime metrics
        elif pd.api.types.is_datetime64_dtype(col_data):
            result.update({
                "min_date": col_data.min(),
                "max_date": col_data.max(),
                "is_datetime": True,
                "unique_count": col_data.nunique(),
                "unique_percent": round(col_data.nunique() / len(col_data) * 100, 2)
            })
        
        return result
    
    def analyze_dataframe(self, df: pd.DataFrame) -> Dict:
        """
        Analyze all columns in a DataFrame.
        
        Args:
            df: Pandas DataFrame to analyze
            
        Returns:
            Dictionary with analysis for each column
        """
        self.logger.info(f"Analyzing DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
        result = {
            "shape": df.shape,
            "columns": []
        }
        
        for column in df.columns:
            result["columns"].append(self.analyze_column(df, column))
            
        return result
    
    def suggest_expectations(self, df: pd.DataFrame) -> List[Dict]:
        """
        Suggest expectations based on DataFrame analysis.
        
        Args:
            df: Pandas DataFrame to analyze
            
        Returns:
            List of suggested expectation configurations
        """
        analysis = self.analyze_dataframe(df)
        expectations = []
        
        for col_analysis in analysis["columns"]:
            column = col_analysis["name"]
            
            # Always suggest column existence and type
            expectations.append({
                "expectation_type": "expect_column_to_exist",
                "column": column
            })
            
            # For non-empty DataFrames with data, suggest data type expectations
            if df.shape[0] > 0:
                if "is_numeric" in col_analysis:
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_of_type",
                        "column": column,
                        "type_": "number"
                    })
                elif "is_string" in col_analysis:
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_of_type",
                        "column": column,
                        "type_": "string"
                    })
                elif "is_datetime" in col_analysis:
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_of_type",
                        "column": column,
                        "type_": "datetime"
                    })
            
            # Suggest null expectations based on observed data
            if col_analysis["null_count"] == 0:
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": column
                })
            elif col_analysis["null_percent"] < 10:
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": column,
                    "mostly": 0.9  # Allow up to 10% nulls
                })
            
            # Add numeric range expectations
            if "is_numeric" in col_analysis:
                # Add buffer to min/max to account for future data
                buffer = max(0.1 * (col_analysis["max"] - col_analysis["min"]), 1)
                min_val = col_analysis["min"] - buffer if col_analysis["min"] != 0 else 0
                max_val = col_analysis["max"] + buffer
                
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_between",
                    "column": column,
                    "min_value": float(min_val) if min_val != 0 else 0,
                    "max_value": float(max_val)
                })
            
            # Add string length expectations if this is a string column
            if "is_string" in col_analysis:
                if col_analysis.get("max_length", 0) > 0:
                    expectations.append({
                        "expectation_type": "expect_column_value_lengths_to_be_between",
                        "column": column,
                        "min_value": max(0, col_analysis["min_length"] - 5),
                        "max_value": col_analysis["max_length"] + 20  # Add buffer for future data
                    })
            
            # Add categorical expectations
            if col_analysis.get("probable_category", False):
                values = list(col_analysis["value_counts"].keys())
                if len(values) <= 10:  # Only suggest for reasonably sized value sets
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "column": column,
                        "value_set": values
                    })
            
            # Add uniqueness expectations for ID-like columns
            if (col_analysis.get("unique_percent", 0) > 95 and 
                col_analysis.get("null_percent", 100) < 5 and
                (column.lower().endswith('id') or column.lower().endswith('_id') or 
                 column.lower() == 'id')):
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_unique",
                    "column": column
                })
        
        return expectations
    
    def generate_expectation_suite(self, df: pd.DataFrame, suite_name: str) -> Dict:
        """
        Generate a complete expectation suite for a DataFrame.
        
        Args:
            df: Pandas DataFrame to analyze
            suite_name: Name for the expectation suite
            
        Returns:
            Dictionary with a complete expectation suite
        """
        expectations = self.suggest_expectations(df)
        
        suite = {
            "expectation_suite_name": suite_name,
            "expectations": expectations,
            "meta": {
                "generated_by": "DataAssistant",
                "generated_at": datetime.now().isoformat(),
                "row_count": df.shape[0],
                "column_count": df.shape[1]
            }
        }
        
        return suite


class DataQualityPipeline:
    """Pipeline class to orchestrate data quality processes."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the data quality pipeline.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config = DataQualityConfig(config_path)
        self.checker = DataQualityChecker(self.config)
        self.data_assistant = DataAssistant()
        self.logger = logging.getLogger("DataQualityPipeline")
        
    def run_validation(self, data_map: Dict[str, Union[pd.DataFrame, str]]) -> Dict:
        """
        Run validation on multiple data sources.
        
        Args:
            data_map: Dictionary mapping source names to either DataFrames or file paths
            
        Returns:
            Dictionary with validation results for each source
        """
        results = {}
        
        for source_name, data in data_map.items():
            self.logger.info(f"Processing source: {source_name}")
            
            if isinstance(data, pd.DataFrame):
                result = self.checker.validate_dataframe(data, source_name)
            elif isinstance(data, str) and data.endswith('.csv'):
                result = self.checker.validate_csv(data, source_name)
            else:
                self.logger.error(f"Unsupported data type for source: {source_name}")
                continue
                
            results[source_name] = result
            
        return results
    
    def generate_reports(self, output_dir: str) -> List[str]:
        """
        Generate reports for all validated sources.
        
        Args:
            output_dir: Directory to save reports
            
        Returns:
            List of paths to generated reports
        """
        os.makedirs(output_dir, exist_ok=True)
        report_paths = []
        
        for source_name in self.checker.results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"{source_name}_{timestamp}_report.json"
            path = os.path.join(output_dir, file_name)
            self.checker.generate_report(source_name, path)
            report_paths.append(path)
            
        return report_paths
    
    def suggest_expectations(self, source_name: str, data: Union[pd.DataFrame, str]) -> List[Dict]:
        """
        Suggest expectations for a data source.
        
        Args:
            source_name: Name of the data source
            data: Either a DataFrame or a path to a CSV file
            
        Returns:
            List of suggested expectations
        """
        self.logger.info(f"Suggesting expectations for {source_name}")
        
        # Convert to DataFrame if it's a file path
        if isinstance(data, str) and data.endswith('.csv'):
            df = pd.read_csv(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            self.logger.error(f"Unsupported data type for source: {source_name}")
            return []
        
        return self.data_assistant.suggest_expectations(df)
    
    def save_suggested_expectations(self, source_name: str, 
                                   data: Union[pd.DataFrame, str], 
                                   output_path: str) -> str:
        """
        Generate and save suggested expectations for a data source.
        
        Args:
            source_name: Name of the data source
            data: Either a DataFrame or a path to a CSV file
            output_path: Path to save the expectation suite
            
        Returns:
            Path to the saved expectation suite
        """
        self.logger.info(f"Generating expectation suite for {source_name}")
        
        # Convert to DataFrame if it's a file path
        if isinstance(data, str) and data.endswith('.csv'):
            df = pd.read_csv(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            self.logger.error(f"Unsupported data type for source: {source_name}")
            return ""
        
        suite = self.data_assistant.generate_expectation_suite(df, f"{source_name}_suite")
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(suite, f, indent=2)
            
        self.logger.info(f"Saved expectation suite to {output_path}")
        return output_path
    
    def run_pipeline(self, data_map: Dict[str, Union[pd.DataFrame, str]], 
                     output_dir: Optional[str] = None,
                     auto_suggest: bool = False) -> Dict:
        """
        Run the complete data quality pipeline.
        
        Args:
            data_map: Dictionary mapping source names to either DataFrames or file paths
            output_dir: Directory to save reports (if None, no reports are saved)
            auto_suggest: If True, automatically suggest expectations for sources without defined expectations
            
        Returns:
            Dictionary with validation results
        """
        # Auto-suggest expectations if needed
        if auto_suggest and output_dir:
            expectations_dir = os.path.join(output_dir, "expectations")
            os.makedirs(expectations_dir, exist_ok=True)
            
            for source_name, data in data_map.items():
                # Only suggest if no expectations are defined for this source
                if not self.config.get_expectations(source_name):
                    self.logger.info(f"No expectations defined for {source_name}, auto-suggesting")
                    expectation_path = os.path.join(expectations_dir, f"{source_name}_expectations.json")
                    self.save_suggested_expectations(source_name, data, expectation_path)
                    
                    # Update config with suggested expectations
                    # In a real implementation, you might want to load these into the config object
                    self.logger.info(f"Generated expectations for {source_name}")
        
        results = self.run_validation(data_map)
        
        if output_dir:
            report_paths = self.generate_reports(output_dir)
            self.logger.info(f"Generated {len(report_paths)} reports in {output_dir}")
            
        # Generate a summary report
        summary_report = self.checker.generate_report()
        self.checker.send_notifications(summary_report)
        
        return results


# Example usage
if __name__ == "__main__":
    # Sample configuration
    config_json = {
        "datasources": {
            "sales_data": {
                "type": "csv",
                "path": "data/sales.csv"
            }
        },
        "expectations": {
            "sales_data": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "column": "customer_id"
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": "customer_id"
                },
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "column": "total_amount",
                    "min_value": 0,
                    "max_value": 10000
                }
            ]
        },
        "notifications": {
            "email": "data_team@example.com",
            "slack": "#data-quality-alerts"
        }
    }
    
    # Write config to file
    with open("data_quality_config.json", "w") as f:
        json.dump(config_json, f, indent=2)
    
    # Create sample data
    sales_df = pd.DataFrame({
        "customer_id": [101, 102, 103, None, 105],
        "purchase_date": ["2023-01-15", "2023-01-16", "2023-01-17", "2023-01-18", "2023-01-19"],
        "total_amount": [125.50, 250.75, 50.25, 1000.00, 15000.00],
    })
    
    # Create and run the pipeline
    pipeline = DataQualityPipeline("data_quality_config.json")
    results = pipeline.run_pipeline(
        data_map={"sales_data": sales_df},
        output_dir="data_quality_reports"
    )
    
    print("Data quality check completed!")