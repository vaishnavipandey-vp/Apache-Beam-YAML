import apache_beam as beam
import pandas as pd
import numpy as np
import csv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReplaceMissingValues(beam.DoFn):
    def __init__(self):
        self.column_means = None
        self.header = None
        self.dtypes = None

    def setup(self):
        df = pd.read_csv("D:\Programs\Apache-Beam-YAML\Datasets\diabetes_data.csv")
        self.header = df.columns.tolist()
        self.dtypes = df.dtypes.to_dict()

        # Calculating means only for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        self.column_means = df[numeric_cols].mean().round(2).to_dict()

        logger.info(f"Numeric column means: {self.column_means}")
        logger.info(f"Data types: {self.dtypes}")

    def process(self, element):
        try:
            # logger.debug(f"Processing element: {element}")
            for key in self.header:
                original_value = element.get(key, "")
                # logger.debug(
                #     f"Column: {key}, Original value: {original_value}, Type: {type(original_value)}"
                # )

                if pd.isna(original_value) or original_value == "":
                    if key in self.column_means:
                        new_value = self.safe_cast(
                            self.column_means[key], self.dtypes[key]
                        )
                        # logger.debug(
                        #     f"Replacing missing value in column {key} with mean: {new_value}"
                        # )
                    else:
                        new_value = ""
                        # logger.debug(
                        #     f"Replacing missing value in non-numeric column {key} with empty string"
                        # )
                else:
                    new_value = self.safe_cast(original_value, self.dtypes[key])

                element[key] = new_value
                # logger.debug(
                #     f"After processing: Column: {key}, New value: {new_value}, Type: {type(new_value)}"
                # )

            # logger.debug(f"Processed element: {element}")
            yield element
        except Exception as e:
            logger.error(f"Error processing element: {element}")
            logger.error(f"Error details: {str(e)}")
            raise

    def safe_cast(self, value, dtype):
        try:
            if pd.api.types.is_numeric_dtype(dtype):
                return dtype.type(value)
            else:
                return str(value)
        except ValueError:
            logger.warning(
                f"Could not convert {value} to {dtype}, using original value"
            )
            return value
        
class ReformatDates(beam.DoFn):
    def __init__(self):
        self.date_column = "Date"

    def setup(self):
        df = pd.read_csv("D:\Programs\Apache-Beam-YAML\Datasets\diabetes_data.csv")
        logger.info(f"Headers to process for date formatting: {df.columns.tolist()}")

    def process(self, element):
        try:
            original_value = element.get(self.date_column, "")

            if pd.isna(original_value) or original_value == "":
                new_value = None 
            else:
                new_value = self.reformat_date(original_value)

            element[self.date_column] = new_value
            logger.debug(f"Processed date in column {self.date_column}: {original_value} -> {new_value}")

            yield element
        except Exception as e:
            logger.error(f"Error processing element: {element}")
            logger.error(f"Error details: {str(e)}")
            raise

    def reformat_date(self, date_str):
        try:
        # Attempt to parse the date with the first format (YYYY-MM-DD)
            parsed_date = pd.to_datetime(date_str, format='%Y-%m-%d', errors='coerce')

            if pd.isna(parsed_date):
                # If parsing fails, try MM/DD/YYYY format
                parsed_date = pd.to_datetime(date_str, format='%m/%d/%Y', errors='coerce')

            if pd.isna(parsed_date):
                # If still NaT, try DD/MM/YYYY format
                parsed_date = pd.to_datetime(date_str, format='%d/%m/%Y', errors='coerce')

            if pd.isna(parsed_date):
                logger.warning(f"Could not parse date: {date_str}")
                return None  
            return parsed_date.strftime('%Y-%m-%d')
        
        except Exception as e:
            logger.error(f"Error formatting date {date_str}: {str(e)}")
            return None     

class GenerateRemarks(beam.DoFn):
    def __init__(self):
        self.date_column = "Date"
        self.pregnancies_column = "Pregnancies"
        self.glucose_column = "Glucose"
        self.bloodPressure_column = "BloodPressure"
        self.skinThickness_column = "SkinThickness"
        self.insulin_column = "Insulin"
        self.bmi_column = "BMI"
        self.diabetes_column = "DiabetesPedigreeFunction"
        self.age_column = "Age"
        self.outcome_column = "Outcome" 
        self.remarks_column = "Remarks"

    def process(self, element):
        try:
                
            outcome_column = element.get(self.outcome_column, "")
            glucose_column = element.get(self.glucose_column, "")
            bmi_column = element.get(self.bmi_column, "")
            bloodPressure_column = element.get(self.bloodPressure_column, "")
            glucose_column = element.get(self.glucose_column, "")

            remarks = []

            if outcome_column == 1:
                remarks.append("Positive Diabetes Test")
            else:
                remarks.append("Negative Diabetes Test")

            if glucose_column > 140:
                remarks.append("High Glucose Level")
            elif glucose_column < 70:
                remarks.append("Low Glucose Level")

            if bmi_column >= 30:
                remarks.append("Obesity")

            if bloodPressure_column > 90:
                remarks.append("High Blood Pressure")

            # Store remarks back into the element
            element[self.remarks_column] = "; ".join(remarks)  # Return remarks as a single string
            
            yield element
        except Exception as e:
            logger.error(f"Error generating remarks for element: {element}")
            logger.error(f"Error details: {str(e)}")
            raise


def parse_csv_line(line, header):
    return dict(zip(header, csv.reader([line]).__next__()))

def format_as_csv(element, header):
    return ",".join(str(element.get(h, "")) for h in header)

def run():
    with beam.Pipeline() as pipeline:

        with open("D:\Programs\Apache-Beam-YAML\Datasets\diabetes_data.csv", "r") as f:
            header = next(csv.reader(f))

        logger.info(f"CSV Header: {header}")
        header.append("Remarks")  # Add 'Remarks' to the output header
        (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText("D:\Programs\Apache-Beam-YAML\Datasets\diabetes_data.csv", skip_header_lines=1)
            | "Parse CSV" >> beam.Map(lambda line: parse_csv_line(line, header))
            | "Replace Missing Values" >> beam.ParDo(ReplaceMissingValues())
            | "Reformat Dates" >> beam.ParDo(ReformatDates())
            | "Generate Remarks" >> beam.ParDo(GenerateRemarks())
            | "Format as CSV" >> beam.Map(lambda d: format_as_csv(d, header))
            | "Write CSV" >> beam.io.WriteToText("D:\Programs\Apache-Beam-YAML\Outputs\cleaned_diabetes_data.csv", header=",".join(header))
        )


if __name__ == "__main__":
    run()

