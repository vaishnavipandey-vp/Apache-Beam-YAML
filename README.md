# Apache Beam Data Transformation Project

## Problem Statement
The goal of this project is to design a scalable data pipeline using Apache Beam to clean and transform a dataset containing missing or inaccurate values. Key objectives include:
- Replacing missing values in numeric columns
- Reformatting date fields
- Adding custom remarks based on conditions (e.g., high glucose levels)

This pipeline leverages Apache Beam’s distributed processing capabilities to handle large datasets efficiently.

## Work Approach

### 1. Data Exploration and Requirement Gathering
- Analyzed the diabetes dataset to identify missing values and errors.
- Determined columns for processing (e.g., mean imputation for numeric columns, date formatting).

### 2. Exploration of Apache Beam YAML API
- Tested multiple YAML pipeline configurations to understand YAML-based transformations:
  - `calculateColumnMean.yaml` and `calculateRowMean.yaml` for mean calculations.
  - `diabetes_analysis.yaml` for preliminary data transformations.
  - `exceptionHandling.yaml`, `exceptionHandlingMultiple.yaml`, and other YAML files for exception handling, dataset joining, and advanced transformations.
- **Outcome**: This exploration highlighted the capabilities and limitations of YAML in Apache Beam, leading to the decision to implement custom transformations in Python.

### 3. Pipeline Implementation in Python
- Developed the pipeline in Python with Apache Beam, implementing custom transformations:
  - **ReplaceMissingValues**: Replaces missing values in numeric columns with column averages.
  - **ReformatDates**: Standardizes date formats.
  - **GenerateRemarks**: Adds remarks based on specific column values, such as high glucose levels.

### 4. Pipeline Execution and Testing
- Verified the transformations on a sample dataset, logged errors, and ensured output accuracy.

### 5. Output Generation
- Generated a transformed CSV file with cleaned data and added remarks.

## Work Products and Deliverables
- **Data Pipeline Scripts**: Finalized Apache Beam scripts in Python and YAML.
- **Transformed Dataset**: Cleaned output CSV file.
- **Documentation**: Project report, user manual, and technical documentation.
- **GitHub Repository**: Contains all code, documentation, and example datasets.

## User Manual

### Environment Setup
1. Install Apache Beam and dependencies listed in `requirements.txt`.
2. Clone the repository and navigate to the project folder.

### Running the Pipeline
1. Open a terminal and navigate to the `Scripts` folder.
2. Run the script using:
   ```bash
   python data_analysis.py
   ```
   Or, for the YAML pipeline:
   ```bash
   apache-beam-pipeline-runner --config data_analysis.yaml
   ```
3. The output file will be saved in the specified output directory.

### Input and Output Files
- **Input**: Place the CSV file (e.g., `diabetes_data.csv`) in the `Datasets` folder.
- **Output**: Cleaned dataset with remarks, saved in the `Outputs` folder.

## Technical Manual

### Datasets
- **Input**: `diabetes_data.csv` containing health metrics.
- **Output**: `cleaned_diabetes_data.csv` with transformations and remarks.

### Software Libraries
- **Apache Beam**: Core library for data transformation.
- **Pandas**: For data manipulation.
- **NumPy**: For numerical operations.

### Pipeline Components
- **ReplaceMissingValues**: Fills missing values with column averages.
- **ReformatDates**: Formats dates consistently.
- **GenerateRemarks**: Adds remarks based on health metrics.

### Installation and Configuration
- Ensure Python and Apache Beam are installed.
- Install additional dependencies using:
  ```bash
  pip install -r requirements.txt
  ```

### Repository Structure
- **Datasets**: Input and example datasets.
- **Pipelines**: Sample and exploratory YAML scripts.
- **Scripts**: Final transformation scripts in both YAML and Python.
- **Outputs**: Transformed data output files.
