# Project Title: Cities demographics, Immigration and airports
### Data Engineering Capstone Project  

#### Project Summary
Capstone, a startup has grown their data warehouse and want to move their data in a data lake. Their data resides in S3, in a directory of parquet on immigration events, as well as two directories with CSV files on US cities demographics and airport codes respectively .

We are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables and fact table. This will allow their analytics team to continue finding insights such as the date, duration of immigration in terms of number of days, weeks, months, years for example, immigrants data such as the visa type, the mode of immigration and cities demographics and the airports in the respective cities as well.

####The project follows the following steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

#### Data description and Gathering

##### Datasets
* I94 Immigration Data: This data comes from the US National Tourism and Trade Office.In the past all foreign visitors to the U.S. arriving via air or sea were required to complete paper Customs and Border Protection Form I-94 Arrival/Departure Record or Form I-94W Nonimmigrant Visa Waiver Arrival/Departure Record and this dataset comes from this forms.
  Contains SAS format data.

* U.S. City Demographic Data: This data comes from OpenSoft.This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
  Contains CSV format data.

* Airport Code Table: This is a simple table of airport codes and corresponding cities. 
  Contains CSV format data.

##### Tools
* Python: for algorithm
* Pandas: exploratory data analysis on small data set
* AWS S3: data storage
* PySpark: data processing on large data set

###Conceptual Data Model

#### Dimension tables:
##### cities_table
           city_id    PK
           city
           state_name
           state_code
           median_age
           male_population
           female_population
           total_population
           
##### airport_table
           airport_id     PK
           airport_type
           airport_name
           elevation_ft
           state_code
           city
           latitude
           longitude 
           
##### immigrant_table
           imm_id         PK
           age
           birth_year
           gender
           visa_type
           state_code
           
#### Fact table:
##### immigration_table
           imm_id          PK
           airport_id      FK
           city_id         FK
           arrival_date
           departure_date
           mode
           state_code
           immigration_year
           immigration_month
           stay_duration_in_days
           stay_duration_in_weeks
           stay_duration_in_months
           stay_duration_in_years
           
           
#### 3.2 Mapping Out Data Pipelines

The data are modelized by the pipline() function that runs the following functions under the wood: 

    1-  process_cities_data()
    
    2-  process_airport_data()
    
    3-  process_immigrant_data()
    
    4-  process_immigration_data()
    
    
#### 4.2 Data Quality Checks
Performed by the following functions:

check_count_dataset_to_spark(): This function performs data quality check on rows loaded from datasets files to Spark.

check_count_table_to_s3(): This function performs data quality check on rows loaded from dimensions and fact tables to s3.
