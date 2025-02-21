#wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip 

#unzip datasource.zip

#python3.11 -m pip install pandas



import pandas as pd
import glob
import xml.etree.ElementTree as ET
from datetime import datetime
import warnings

warnings.filterwarnings('ignore', category=FutureWarning)

log_file = "log_info_file.txt" 
target_file = "transformed_data.csv"
#car_model,year_of_manufacture,price,fuel

#extraction 

def extract_from_csv(file_to_process):
    df=pd.read_csv(file_to_process)
    for col in df.columns:
        if "Unnamed: 0" in col:
            df.drop(columns=[col], inplace=True)
    return df

def extract_from_json(file_to_process):
    df=pd.read_json(file_to_process)
    return df
    
def extract_from_xml(file_to_process):
    df=pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])
    tree=ET.parse(file_to_process)
    root=tree.getroot()
    for car in root:
        car_model=car.find("car_model").text
        year_of_manufacture=car.find("year_of_manufacture").text
        price=float(car.find("price").text)
        fuel=car.find("fuel").text
        extracted_data=pd.concat([df,pd.DataFrame([{"car_model":car_model,"year_of_manufacture":year_of_manufacture,"price":price,"fuel":fuel}])], ignore_index=True)
        return extracted_data

def extract():
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)
    extracted_data.drop_duplicates(inplace=True)

    return extracted_data


#TRANSFORMATION 

#Transform the values under the 'price' header such that they are rounded to 2 decimal places.
def transform(data):
    data['price']=round(data.price,2)
    return data

#loading
#Implement the loading function for the transformed data to a target file, transformed_data.csv.
def load_data(transformed_data,target_file):
    transformed_data.to_csv(target_file)


#logging

#Implement the logging function for the entire process and save it in log_file.txt

def log_progress(message):
    timestamp_format="%y-%h-%d-%H:%M:%S"
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open('log_info_file', 'a')as f:
        f.write(timestamp+','+message +'\n')

#Test the implemented functions and log the events as done in the lab.


log_progress("Etl Process Started")

log_progress("Process of data Extraction")
extracted_data=extract()

log_progress("extraction process ended")


log_progress("Transformation")
transformed_data=transform(extracted_data)

log_progress("Transformation ended")

log_progress("Loading data")
load_data(transformed_data,target_file)
log_progress("Loading is DONE")

log_progress("ETL Process Ended")




