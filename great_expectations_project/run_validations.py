from scripts.validation import validate_data

# Paths to configuration files
data_source_path = "configs/data_source_config.json"
expectations_path = "configs/expectations_config.json"

# Run validation
validate_data(data_source_path, expectations_path)
