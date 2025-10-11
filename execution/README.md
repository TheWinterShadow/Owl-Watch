# Owl-Watch Execution

This package contains all ETL, ML, and utility code for the Owl-Watch data pipeline with a clean, organized structure.

## 🏗️ Architecture

```
execution/
├── core/                    # Core execution logic
│   ├── job_factory.py       # Creates job instances based on type
│   ├── job_runner.py        # Main job execution entry point
│   └── local_runner.py      # Local development runner
├── jobs/                    # All job implementations
│   ├── communication/       # Communication data processing
│   │   ├── email.py         # Email standardization
│   │   ├── slack.py         # Slack message processing
│   │   └── whatsapp.py      # WhatsApp chat processing
│   ├── data/                # Data processing jobs
│   │   ├── cleaning.py      # Data cleaning and validation
│   │   └── transformation.py # Data transformation
│   ├── analytics/           # Analytics and ML jobs
│   │   ├── sentiment.py     # Sentiment analysis
│   │   └── bedrock_ai.py    # AWS Bedrock AI processing
│   └── political/          # Political data specific jobs
│       ├── biden.py         # Biden administration data
│       └── clinton.py       # Clinton administration data
├── utils/                   # Shared utilities and base classes
├── config/                  # Configuration management
│   └── job_config.py        # Centralized job configuration
└── docs/                    # Documentation
```

## 🚀 Pipeline Flow

1. **Raw Data** uploaded to S3 Raw Bucket
2. **Data Cleaning** via `jobs/data/cleaning.py` → S3 Cleaned Bucket
3. **Communication Processing** via `jobs/communication/*` → Standardized format
4. **AI/ML Processing** via `jobs/analytics/*` → S3 Curated Bucket
5. **Political Analysis** via `jobs/political/*` → Specialized insights

## 📋 Available Jobs

### Communication Processing
- `email_communication` - Standardize email data
- `slack_communication` - Process Slack messages  
- `whatsapp_communication` - Parse WhatsApp chats

### Data Processing
- `data_cleaning` - Clean and validate raw data
- `data_transformation` - Transform data schemas

### Analytics & AI
- `sentiment_analysis` - Analyze communication sentiment
- `bedrock_ai_processing` - AWS Bedrock AI analysis

### Political Data
- `biden_data` - Biden administration processing
- `clinton_data` - Clinton administration processing

## 🔧 Usage

### Run in AWS Glue
```python
from core.job_runner import JobRunner

runner = JobRunner()
runner.run()  # Reads job type from command line args
```

### Run Locally for Development
```bash
cd execution
python core/local_runner.py --job-type email_communication --output-path ./output
```

### Using Job Factory
```python
from core.job_factory import JobFactory

factory = JobFactory()
job = factory.create_job('sentiment_analysis')
result = job.run()
```

### Configuration Management
```python
from config.job_config import JobConfig

config = JobConfig.from_job_type('email_communication')
config.input_bucket = 'my-input-bucket'
config.output_bucket = 'my-output-bucket'
```

## 🧪 Local Development

### Requirements
- Python 3.8+
- PySpark (for local testing)
- AWS credentials configured

### Setup
```bash
pip install pyspark  # For local development only
```

### Run Tests
```bash
# From project root
hatch test

# Or run specific test category
python -m pytest tests/execution/
```

### Local Testing Examples
```bash
# Test email processing
python core/local_runner.py --job-type email_communication

# Test sentiment analysis  
python core/local_runner.py --job-type sentiment_analysis

# Test data cleaning
python core/local_runner.py --job-type data_cleaning
```

## 🔄 Migration from Old Structure

The old structure has been reorganized for better maintainability:

**Old → New**
- `etl_class_factory.py` → `core/job_factory.py` (improved)
- `main_etl_running.py` → `core/job_runner.py` (enhanced) 
- `local_etl_runner.py` → `core/local_runner.py` (expanded)
- `etl_jobs/*` → `jobs/communication/*`, `jobs/data/*`, etc. (categorized)
- `enrichment_jobs/bedrock_processor.py` → `jobs/analytics/bedrock_ai.py` (improved)

All import paths have been updated and the new structure maintains backward compatibility where possible.

## 🛠️ Key Improvements

1. **Better Organization** - Jobs grouped by purpose
2. **Enhanced Error Handling** - More robust error handling and logging
3. **Configuration Management** - Centralized config system
4. **Local Testing** - Improved local development experience
5. **Type Safety** - Better type hints and validation
6. **Documentation** - Comprehensive docstrings and examples