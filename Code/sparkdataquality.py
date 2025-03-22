"""
Comprehensive Data Quality Framework for Apache Spark
====================================================

This framework provides a scalable approach to data quality validation in Spark environments.
It includes:
- Schema validation
- Data profiling
- Rule-based validations
- Data quality metrics
- Reporting capabilities
- Notification system

Dependencies:
- PySpark
- Great Expectations[spark]
- pandas
- PyYAML
- pydeequ (optional, for rule-based validation)
"""

import os
import json
import yaml
import logging
import datetime
from typing import Dict, List, Any, Optional, Union, Tuple

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# Great Expectations integration
import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

# Optional Deequ integration
try:
    from pydeequ.analyzers import *
    from pydeequ.checks import *
    from pydeequ.verification import *
    DEEQU_AVAILABLE = True
except ImportError:
    DEEQU_AVAILABLE = False
    logging.warning("PyDeequ not available. Some rule-based validations will be disabled.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data_quality.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SparkDataQuality:
    """
    A comprehensive data quality framework for Apache Spark.
    """
    
    def __init__(
        self, 
        spark: SparkSession = None,
        config_path: str = None,
        notification_handlers: List[callable] = None
    ):
        """
        Initialize the data quality framework.
        
        Args:
            spark: An existing SparkSession. If None, a new one will be created.
            config_path: Path to configuration YAML file
            notification_handlers: List of callables to handle notifications
        """
        self.spark = spark or self._create_spark_session()
        self.config = self._load_config(config_path) if config_path else {}
        self.notification_handlers = notification_handlers or []
        self.validation_results = {}
        self.ge_context = None
        
        # Initialize Great Expectations context if configured
        if self.config.get('great_expectations', {}).get('enabled', False):
            self._init_great_expectations()
    
    def _create_spark_session(self) -> SparkSession:
        """Create a new Spark session with appropriate configurations."""
        return (
            SparkSession.builder
            .appName("Data Quality Framework")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.parquet.enableVectorizedReader", "true")
            .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
            .getOrCreate()
        )
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            return {}
    
    def _init_great_expectations(self):
        """Initialize Great Expectations context."""
        try:
            self.ge_context = ge.get_context()
            logger.info("Great Expectations context initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Great Expectations: {str(e)}")
    
    def validate_schema(
        self, 
        df: DataFrame, 
        expected_schema: Union[StructType, Dict, str],
        name: str = "schema_validation"
    ) -> bool:
        """
        Validate the schema of a DataFrame against an expected schema.
        
        Args:
            df: Spark DataFrame to validate
            expected_schema: Can be a StructType, dict representation, or path to JSON schema
            name: Name for this validation
            
        Returns:
            bool: True if schema matches, False otherwise
        """
        # Convert expected_schema to StructType if it's not already
        if isinstance(expected_schema, str) and os.path.exists(expected_schema):
            with open(expected_schema, 'r') as f:
                schema_dict = json.load(f)
            expected_schema = StructType.fromJson(schema_dict)
        elif isinstance(expected_schema, dict):
            expected_schema = StructType.fromJson(expected_schema)
        
        # Compare schemas
        actual_schema = df.schema
        schema_diff = self._compare_schemas(actual_schema, expected_schema)
        
        is_valid = len(schema_diff) == 0
        
        # Store results
        self.validation_results[name] = {
            "type": "schema_validation",
            "timestamp": datetime.datetime.now().isoformat(),
            "is_valid": is_valid,
            "details": {
                "differences": schema_diff,
                "actual_schema": actual_schema.jsonValue(),
                "expected_schema": expected_schema.jsonValue()
            }
        }
        
        if not is_valid:
            self._notify(
                f"Schema validation failed for {name}",
                f"Found {len(schema_diff)} schema differences",
                "schema_validation",
                self.validation_results[name]
            )
        
        return is_valid
    
    def _compare_schemas(self, actual: StructType, expected: StructType) -> List[Dict]:
        """Compare two schemas and return differences."""
        differences = []
        
        # Check for missing fields
        actual_fields = {f.name: f for f in actual.fields}
        expected_fields = {f.name: f for f in expected.fields}
        
        # Fields in expected but not in actual
        for name, field in expected_fields.items():
            if name not in actual_fields:
                differences.append({
                    "type": "missing_field",
                    "field": name,
                    "expected_type": str(field.dataType)
                })
            else:
                # Type mismatch
                actual_field = actual_fields[name]
                if str(actual_field.dataType) != str(field.dataType):
                    differences.append({
                        "type": "type_mismatch",
                        "field": name,
                        "actual_type": str(actual_field.dataType),
                        "expected_type": str(field.dataType)
                    })
        
        # Fields in actual but not in expected
        for name in actual_fields:
            if name not in expected_fields:
                differences.append({
                    "type": "unexpected_field",
                    "field": name,
                    "actual_type": str(actual_fields[name].dataType)
                })
        
        return differences
    
    def profile_data(self, df: DataFrame, name: str = "data_profile") -> Dict:
        """
        Generate a comprehensive data profile for a DataFrame.
        
        Args:
            df: Spark DataFrame to profile
            name: Name for this profile
            
        Returns:
            Dict: Profile results
        """
        logger.info(f"Profiling data for '{name}'")
        
        # Get basic statistics
        profile = {
            "record_count": df.count(),
            "column_count": len(df.columns),
            "timestamp": datetime.datetime.now().isoformat(),
        }
        
        # Collect column-level statistics
        column_stats = []
        
        # Create a profile for sampling if the dataset is large
        sample_ratio = min(1.0, 1000000 / max(profile["record_count"], 1))
        sample_df = df.sample(withReplacement=False, fraction=sample_ratio, seed=42) if sample_ratio < 1.0 else df
        
        # Calculate column statistics
        for column in df.columns:
            try:
                # Basic stats that work for all column types
                stats_df = sample_df.select(
                    F.count(column).alias("count"),
                    F.count(F.when(F.col(column).isNull(), 1)).alias("null_count"),
                    F.countDistinct(column).alias("distinct_count")
                ).collect()[0]
                
                col_stats = {
                    "name": column,
                    "data_type": str(df.schema[column].dataType),
                    "count": stats_df["count"],
                    "null_count": stats_df["null_count"],
                    "null_percentage": round(stats_df["null_count"] / max(stats_df["count"], 1) * 100, 2),
                    "distinct_count": stats_df["distinct_count"],
                    "distinct_percentage": round(stats_df["distinct_count"] / max(stats_df["count"], 1) * 100, 2)
                }
                
                # Type-specific stats
                data_type = str(df.schema[column].dataType).lower()
                
                if "int" in data_type or "double" in data_type or "float" in data_type or "decimal" in data_type:
                    # Numeric column
                    num_stats = sample_df.select(
                        F.min(column).alias("min"),
                        F.max(column).alias("max"),
                        F.mean(column).alias("mean"),
                        F.stddev(column).alias("stddev"),
                        F.expr(f"percentile({column}, array(0.25, 0.5, 0.75))").alias("percentiles")
                    ).collect()[0]
                    
                    col_stats.update({
                        "min": num_stats["min"],
                        "max": num_stats["max"],
                        "mean": num_stats["mean"],
                        "stddev": num_stats["stddev"],
                        "percentiles": {
                            "25%": num_stats["percentiles"][0],
                            "50%": num_stats["percentiles"][1],
                            "75%": num_stats["percentiles"][2]
                        }
                    })
                
                elif "string" in data_type:
                    # String column
                    if col_stats["distinct_count"] <= 100:
                        # For columns with few distinct values, get frequency distribution
                        value_counts = (
                            sample_df
                            .groupBy(column)
                            .count()
                            .orderBy(F.desc("count"))
                            .limit(20)
                            .collect()
                        )
                        col_stats["value_counts"] = {
                            row[column]: row["count"] for row in value_counts if row[column] is not None
                        }
                    
                    # Get length statistics for strings
                    len_stats = sample_df.select(
                        F.min(F.length(column)).alias("min_length"),
                        F.max(F.length(column)).alias("max_length"),
                        F.mean(F.length(column)).alias("avg_length")
                    ).collect()[0]
                    
                    col_stats.update({
                        "min_length": len_stats["min_length"],
                        "max_length": len_stats["max_length"],
                        "avg_length": len_stats["avg_length"]
                    })
                
                column_stats.append(col_stats)
            
            except Exception as e:
                logger.error(f"Error profiling column {column}: {str(e)}")
                column_stats.append({
                    "name": column,
                    "data_type": str(df.schema[column].dataType),
                    "error": str(e)
                })
        
        profile["columns"] = column_stats
        
        # Store results
        self.validation_results[name] = {
            "type": "data_profile",
            "timestamp": datetime.datetime.now().isoformat(),
            "profile": profile
        }
        
        return profile
    
    def run_ge_expectations(
        self, 
        df: DataFrame, 
        expectation_suite_name: str = None,
        expectations: List[Dict] = None,
        name: str = "ge_validation"
    ) -> Dict:
        """
        Validate a DataFrame using Great Expectations.
        
        Args:
            df: Spark DataFrame to validate
            expectation_suite_name: Name of the pre-defined expectation suite
            expectations: List of expectation configurations to apply
            name: Name for this validation
            
        Returns:
            Dict: Validation results
        """
        if not expectation_suite_name and not expectations:
            raise ValueError("Either expectation_suite_name or expectations must be provided")
        
        try:
            # Convert Spark DataFrame to GE DataFrame
            ge_df = SparkDFDataset(df)
            
            if expectation_suite_name:
                # Use pre-defined expectation suite
                validation_result = self.ge_context.run_validation_operator(
                    "action_list_operator",
                    assets_to_validate=[
                        {
                            "batch": {"dataset": ge_df, "batch_kwargs": {}},
                            "expectation_suite_name": expectation_suite_name
                        }
                    ]
                )
                is_valid = validation_result.success
                
            else:
                # Apply expectations dynamically
                for exp in expectations:
                    method_name = exp["expectation_type"]
                    method_args = exp.get("kwargs", {})
                    getattr(ge_df, method_name)(**method_args)
                
                validation_result = ge_df.validate()
                is_valid = validation_result.success
            
            # Store results
            self.validation_results[name] = {
                "type": "great_expectations",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": is_valid,
                "details": validation_result.to_json_dict()
            }
            
            if not is_valid:
                self._notify(
                    f"Great Expectations validation failed for {name}",
                    f"Failed expectations: {len([r for r in validation_result.results if not r['success']])}",
                    "great_expectations",
                    self.validation_results[name]
                )
            
            return validation_result.to_json_dict()
            
        except Exception as e:
            logger.error(f"Error running Great Expectations validation: {str(e)}")
            self.validation_results[name] = {
                "type": "great_expectations",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": False,
                "error": str(e)
            }
            
            self._notify(
                f"Great Expectations validation error for {name}",
                str(e),
                "great_expectations",
                self.validation_results[name]
            )
            
            return {"success": False, "error": str(e)}
    
    def run_deequ_validation(
        self, 
        df: DataFrame, 
        rules: List[Dict],
        name: str = "deequ_validation"
    ) -> Dict:
        """
        Validate a DataFrame using Deequ's rule-based approach.
        
        Args:
            df: Spark DataFrame to validate
            rules: List of rule configurations
            name: Name for this validation
            
        Returns:
            Dict: Validation results
        """
        if not DEEQU_AVAILABLE:
            logger.error("PyDeequ is not available. Cannot run Deequ validation.")
            return {"success": False, "error": "PyDeequ not available"}
        
        try:
            # Initialize verification suite
            check = Check(self.spark, CheckLevel.Error, "Data Quality Check")
            
            # Build checks based on rules
            for rule in rules:
                rule_type = rule["type"]
                column = rule.get("column")
                
                if rule_type == "isComplete":
                    check = check.isComplete(column)
                elif rule_type == "hasCompleteness":
                    check = check.hasCompleteness(column, rule["min"], rule.get("max", 1.0))
                elif rule_type == "hasUniqueness":
                    check = check.hasUniqueness(column, rule["min"], rule.get("max", 1.0))
                elif rule_type == "hasDistinctness":
                    check = check.hasDistinctness(column, rule["min"], rule.get("max", 1.0))
                elif rule_type == "hasUniqueValueRatio":
                    check = check.hasUniqueValueRatio(column, rule["min"], rule.get("max", 1.0))
                elif rule_type == "hasNumberOfDistinctValues":
                    check = check.hasNumberOfDistinctValues(column, rule["min"], rule.get("max"))
                elif rule_type == "isNonNegative":
                    check = check.isNonNegative(column)
                elif rule_type == "isPositive":
                    check = check.isPositive(column)
                elif rule_type == "hasMax":
                    check = check.hasMax(column, rule["max"])
                elif rule_type == "hasMin":
                    check = check.hasMin(column, rule["min"])
                elif rule_type == "hasStandardDeviation":
                    check = check.hasStandardDeviation(column, rule["min"], rule.get("max"))
                elif rule_type == "hasMean":
                    check = check.hasMean(column, rule["min"], rule.get("max"))
                elif rule_type == "hasSum":
                    check = check.hasSum(column, rule["min"], rule.get("max"))
                elif rule_type == "hasEntropy":
                    check = check.hasEntropy(column, rule["min"], rule.get("max"))
                elif rule_type == "hasCorrelation":
                    check = check.hasCorrelation(column, rule["other_column"], rule["min"], rule.get("max", 1.0))
                elif rule_type == "satisfies":
                    check = check.satisfies(rule["constraint"], rule["description"])
                else:
                    logger.warning(f"Unknown rule type: {rule_type}")
            
            # Run validation
            verification_result = VerificationSuite(self.spark) \
                .onData(df) \
                .addCheck(check) \
                .run()
            
            # Convert to a more usable format
            result_df = VerificationResult.checkResultsAsDataFrame(self.spark, verification_result)
            result_pd = result_df.toPandas()
            
            # Check if all constraints are satisfied
            is_valid = all(result_pd["constraint_status"] == "Success")
            
            # Store results
            self.validation_results[name] = {
                "type": "deequ",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": is_valid,
                "details": result_pd.to_dict(orient="records")
            }
            
            if not is_valid:
                self._notify(
                    f"Deequ validation failed for {name}",
                    f"Failed rules: {len(result_pd[result_pd['constraint_status'] != 'Success'])}",
                    "deequ",
                    self.validation_results[name]
                )
            
            return self.validation_results[name]
            
        except Exception as e:
            logger.error(f"Error running Deequ validation: {str(e)}")
            self.validation_results[name] = {
                "type": "deequ",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": False,
                "error": str(e)
            }
            
            self._notify(
                f"Deequ validation error for {name}",
                str(e),
                "deequ",
                self.validation_results[name]
            )
            
            return {"success": False, "error": str(e)}
    
    def custom_validation(
        self, 
        df: DataFrame, 
        validation_fn: callable,
        name: str = "custom_validation"
    ) -> Dict:
        """
        Run a custom validation function on the DataFrame.
        
        Args:
            df: Spark DataFrame to validate
            validation_fn: Function that takes a DataFrame and returns a validation result dict
            name: Name for this validation
            
        Returns:
            Dict: Validation results
        """
        try:
            result = validation_fn(df)
            
            # Ensure result has required fields
            if not isinstance(result, dict):
                result = {"result": result}
            
            if "is_valid" not in result:
                result["is_valid"] = bool(result.get("result", False))
            
            # Store results
            self.validation_results[name] = {
                "type": "custom",
                "timestamp": datetime.datetime.now().isoformat(),
                **result
            }
            
            if not result["is_valid"]:
                self._notify(
                    f"Custom validation failed for {name}",
                    str(result.get("details", "")),
                    "custom",
                    self.validation_results[name]
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error running custom validation: {str(e)}")
            self.validation_results[name] = {
                "type": "custom",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": False,
                "error": str(e)
            }
            
            self._notify(
                f"Custom validation error for {name}",
                str(e),
                "custom",
                self.validation_results[name]
            )
            
            return {"success": False, "error": str(e)}
    
    def check_data_freshness(
        self,
        df: DataFrame,
        timestamp_column: str,
        max_delay_minutes: int = 60,
        name: str = "freshness_check"
    ) -> Dict:
        """
        Check if data is fresh based on timestamp column.
        
        Args:
            df: Spark DataFrame to check
            timestamp_column: Column containing timestamps
            max_delay_minutes: Maximum acceptable delay in minutes
            name: Name for this check
            
        Returns:
            Dict: Check results
        """
        try:
            # Get the max timestamp from the data
            max_ts_row = df.select(F.max(F.col(timestamp_column)).alias("max_ts")).collect()[0]
            max_ts = max_ts_row["max_ts"]
            
            if max_ts is None:
                is_valid = False
                details = "No timestamps found in data"
            else:
                # Convert to datetime if it's a string
                if isinstance(max_ts, str):
                    max_ts = datetime.datetime.strptime(max_ts, "%Y-%m-%d %H:%M:%S")
                
                # Calculate delay
                current_time = datetime.datetime.now()
                delay_minutes = (current_time - max_ts).total_seconds() / 60
                
                is_valid = delay_minutes <= max_delay_minutes
                details = {
                    "max_timestamp": max_ts.isoformat() if hasattr(max_ts, "isoformat") else str(max_ts),
                    "current_time": current_time.isoformat(),
                    "delay_minutes": delay_minutes,
                    "max_allowed_delay": max_delay_minutes
                }
            
            # Store results
            self.validation_results[name] = {
                "type": "freshness_check",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": is_valid,
                "details": details
            }
            
            if not is_valid:
                self._notify(
                    f"Data freshness check failed for {name}",
                    f"Data is {details.get('delay_minutes', 'N/A')} minutes old (max allowed: {max_delay_minutes})",
                    "freshness_check",
                    self.validation_results[name]
                )
            
            return self.validation_results[name]
            
        except Exception as e:
            logger.error(f"Error checking data freshness: {str(e)}")
            self.validation_results[name] = {
                "type": "freshness_check",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": False,
                "error": str(e)
            }
            
            self._notify(
                f"Data freshness check error for {name}",
                str(e),
                "freshness_check",
                self.validation_results[name]
            )
            
            return {"success": False, "error": str(e)}
    
    def check_reference_integrity(
        self, 
        df: DataFrame, 
        reference_df: DataFrame, 
        join_columns: List[str],
        name: str = "ref_integrity_check"
    ) -> Dict:
        """
        Check referential integrity between two DataFrames.
        
        Args:
            df: Primary DataFrame
            reference_df: Reference DataFrame
            join_columns: Columns to join on
            name: Name for this check
            
        Returns:
            Dict: Check results
        """
        try:
            # Count records in primary dataframe
            total_count = df.count()
            
            # Count records that have matching references
            joined_df = df.join(
                reference_df.select(*join_columns),
                on=join_columns,
                how="left_semi"
            )
            matched_count = joined_df.count()
            
            # Check if all records have references
            is_valid = total_count == matched_count
            
            # Find examples of records without references
            if not is_valid:
                missing_refs_df = df.join(
                    reference_df.select(*join_columns),
                    on=join_columns,
                    how="left_anti"
                ).limit(10)
                
                missing_refs = missing_refs_df.collect()
            else:
                missing_refs = []
            
            details = {
                "total_count": total_count,
                "matched_count": matched_count,
                "missing_count": total_count - matched_count,
                "missing_percentage": round((total_count - matched_count) / total_count * 100, 2) if total_count > 0 else 0,
                "examples": [row.asDict() for row in missing_refs]
            }
            
            # Store results
            self.validation_results[name] = {
                "type": "ref_integrity_check",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": is_valid,
                "details": details
            }
            
            if not is_valid:
                self._notify(
                    f"Referential integrity check failed for {name}",
                    f"Missing references: {details['missing_count']} ({details['missing_percentage']}%)",
                    "ref_integrity_check",
                    self.validation_results[name]
                )
            
            return self.validation_results[name]
            
        except Exception as e:
            logger.error(f"Error checking referential integrity: {str(e)}")
            self.validation_results[name] = {
                "type": "ref_integrity_check",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": False,
                "error": str(e)
            }
            
            self._notify(
                f"Referential integrity check error for {name}",
                str(e),
                "ref_integrity_check",
                self.validation_results[name]
            )
            
            return {"success": False, "error": str(e)}
    
    def check_primary_key(
        self, 
        df: DataFrame, 
        key_columns: List[str],
        name: str = "pk_check"
    ) -> Dict:
        """
        Check if columns form a valid primary key (unique, not null).
        
        Args:
            df: DataFrame to check
            key_columns: Columns that should form a primary key
            name: Name for this check
            
        Returns:
            Dict: Check results
        """
        try:
            # Count total records
            total_count = df.count()
            
            # Count distinct key combinations
            distinct_count = df.select(*key_columns).distinct().count()
            
            # Count records with null keys
            null_conditions = [F.col(col).isNull() for col in key_columns]
            null_condition = null_conditions[0]
            for condition in null_conditions[1:]:
                null_condition = null_condition | condition
            
            null_count = df.filter(null_condition).count()
            
            # Is valid if distinct count equals total count and no nulls
            is_valid = (distinct_count == total_count) and (null_count == 0)
            
            # Get examples of duplicates if any
            if distinct_count < total_count:
                # Find duplicate key values
                window_spec = Window.partitionBy(*key_columns).orderBy(*key_columns)
                duplicates_df = (
                    df.select(
                        *key_columns,
                        F.count("*").over(window_spec).alias("count")
                    )
                    .filter(F.col("count") > 1)
                    .select(*key_columns, "count")
                    .distinct()
                    .orderBy(F.col("count").desc())
                    .limit(10)
                )
                
                duplicates = duplicates_df.collect()
            else:
                duplicates = []
            
            details = {
                "total_count": total_count,
                "distinct_key_count": distinct_count,
                "duplicate_count": total_count - distinct_count,
                "null_key_count": null_count,
                "is_unique": distinct_count == total_count,
                "has_nulls": null_count > 0,
                "duplicate_examples": [row.asDict() for row in duplicates]
            }
            
            # Store results
            self.validation_results[name] = {
                "type": "pk_check",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": is_valid,
                "details": details
            }
            
            if not is_valid:
                self._notify(
                    f"Primary key check failed for {name}",
                    f"Duplicates: {details['duplicate_count']}, Nulls: {details['null_key_count']}",
                    "pk_check",
                    self.validation_results[name]
                )
            
            return self.validation_results[name]
            
        except Exception as e:
            logger.error(f"Error checking primary key: {str(e)}")
            self.validation_results[name] = {
                "type": "pk_check",
                "timestamp": datetime.datetime.now().isoformat(),
                "is_valid": False,
                "error": str(e)
            }
            
            self._notify(
                f"Primary key check error for {name}",
                str(e),
                "pk_check",
                self.validation_results[name]
            )
            
            return {"success": False, "error": str(e)}
    
    def detect_anomalies(
        self, 
        df: DataFrame, 
        column: str,
        method: str = "zscore",
        threshold: