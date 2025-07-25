# Crop Suitability Predictor

## Overview

**Crop Suitability Predictor** is a data science and machine learning pipeline designed to predict the suitability of different crops for specific locations, primarily based on soil characteristics, satellite imagery, and agricultural data. The project automates the ingestion, processing, and modeling of large geospatial datasets to help researchers, analysts, and agricultural planners make data-driven decisions.

## Features
- Automated ingestion and transformation of raw geospatial and agricultural data
- Processing of soil rasters, crop yield reports, NDVI satellite imagery, and more
- Machine learning model training and evaluation for crop suitability prediction
- Reproducible environment and workflow using Poetry
- Modular, extensible, and well-organized codebase

## Directory Structure
```
project-root/
│
├── src/
│   └── crop_suitability_predictor/
│       ├── config/
│       ├── modules/
│       ├── scripts/
│       └── ...
├── data/
│   ├── raw/
│   └── processed/
├── tests/
├── setup.py
├── pyproject.toml
├── poetry.lock
└── README.md
```

## Getting Started

### 1. Clone the Repository
```sh
git clone <your-repo-url>
cd crop-suitability-predictor
```

### 2. Install Poetry (if not already installed)
```sh
curl -sSL https://install.python-poetry.org | python3 -
```

### 3. Install Dependencies
```sh
poetry install
```

### 4. Set Up the Data Directory Structure
This project expects a specific data directory structure, as defined in the configuration. To create all necessary directories:

```sh
python setup.py
```
This script will create all required `data/raw` and `data/processed` subdirectories at the location specified by the `PROJECT_PATH` in the config.

### 5. Activate the Poetry Shell (Optional)
```sh
poetry shell
```

## Running the Project

### Data Ingestion
Fetch and prepare raw data:
```sh
poetry run fetch-raw-data
```

### Data Transformation
Process and transform the ingested data:
```sh
poetry run data-transformation
```

### Model Training
Train the crop suitability prediction model:
```sh
poetry run train-model
```

## Testing
Run the test suite with:
```sh
poetry run pytest
```

## Code Quality
Format and lint your code using:
```sh
poetry run black .
poetry run isort .
poetry run flake8
```

## Configuration
- All key paths and settings are managed in `src/crop_suitability_predictor/config/config.py`.
- You can adjust `PROJECT_PATH` and other variables as needed for your environment.

## Contributing
Pull requests and issues are welcome! Please ensure code is well-documented and tested.

## License
This project is licensed under the MIT License.
