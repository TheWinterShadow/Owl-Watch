# Test Input Data

This directory contains sample input data files for integration tests.

## File Types

- `sample_input.csv` - Sample CSV data for ETL pipeline testing

## Data Structure

The CSV contains sensor data with the following columns:
- `id` - Unique sensor reading ID
- `timestamp` - ISO 8601 timestamp
- `sensor_type` - Type of sensor (temperature, humidity, pressure)
- `value` - Sensor reading value
- `location` - Sensor location identifier
- `quality_score` - Data quality score (0.0 to 1.0)

TODO: Customize this structure to match your actual data requirements.