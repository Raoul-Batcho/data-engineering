%%spark

import pandas as pd
import configparser
from datetime import datetime, timedelta
import os
import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Read in the immigration data
df_spark_immigration=spark.read.parquet("s3://raoul-bucket/sas_data")

# create temporary table
immigrant_table = df_spark_immigration.createOrReplaceTempView("immigrant_table")

# create temporary table
immigration_table = df_spark_immigration.createOrReplaceTempView("immigration_table")

# Customization of the cities data schema
df_spark_cities_schema=StructType([
                       StructField("city",StringType()),
                       StructField("state_name",StringType()),
                       StructField("median_age",DoubleType()),
                       StructField("male_population",IntegerType()),
                       StructField("female_population",IntegerType()),
                       StructField("total_population",IntegerType()),
                       StructField("number_veterans",IntegerType()),
                       StructField("foreign_born",IntegerType()),
                       StructField("avg_household_size",DoubleType()),
                       StructField("state_code",StringType()),
                       StructField("race",StringType()),
                       StructField("race_count",IntegerType())])

# Read in the cities data
df_spark_cities=spark.read.csv("s3://raoul-bucket/us-cities-demographics.csv", schema=df_spark_cities_schema, sep=";", mode="DROPMALFORMED")

# create temporary table
cities_table=df_spark_cities.createOrReplaceTempView("cities_table")


# Read in the airport data
df_spark_airport = spark.read.csv("s3://raoul-bucket/airport-codes_csv.csv", sep=",", header=True)

# put coordinates column in first normal form
df_spark_airport = df_spark_airport.withColumn("latitude", split(df_spark_airport["coordinates"], ", ").getItem(0).cast("double"))\
                                           .withColumn("longitude", split(df_spark_airport["coordinates"], ", ").getItem(1).cast("double"))\
                                           .withColumn("iso_region", substring(df_spark_airport["iso_region"], 4,2))\
                                           .drop("coordinates")
# create temporary table
airport_table = df_spark_airport.createOrReplaceTempView("airport_table")


output_bucket = "s3a://raoul-bucket/"

# Cleaning tasks for cities data. The steps involved are the following:
# 1- Select valuable columns
# 2- Drop duplicate entries
# 3- Drop lines containing Null value
# 4- Put the table at the first normal form

def process_cities_data():
    """
       The function:
       Extracts data from us-cities-demographic.csv
       Transforms the data and create cities_table
       Loads cities_table back to S3 in parquet format
    """

       # Create cities_table
    cities_table=spark.sql("""SELECT md5(city||state_name) city_id,
                                        city,
                                        state_name,
                                        state_code,
                                        median_age,
                                        male_population,
                                        female_population,
                                        total_population
                                 FROM  cities_table
                                 ORDER BY city
                                      """).dropDuplicates().dropna()

    # load cities_table to s3 in parquet format
    cities_table.write.partitionBy("state_code","city").mode("append").parquet(output_bucket+'us_cities_demographics.parquet')

    cities_table.show(n=3)
    return cities_table.count()


# Cleaning tasks for airport data and creation of airport dimension table
# 1- Select valuable columns
# 2- Drop duplicate entries
# 3- Drop lines containing Null value
# 4- Put the table at the first normal form

def process_airport_data():
        """
            The function:
            Extracts data from airport-codes.csv
            Transforms the data and create airport_table
            Loads airport_table back to S3 in parquet format.
        """
        # Create airport_table

        airport_table = spark.sql("""SELECT ident airport_id,
                                            type airport_type,
                                            name airport_name,
                                            int(elevation_ft) elevation_ft,
                                            iso_region state_code,
                                            municipality city,
                                            latitude,
                                            longitude

                                     FROM   airport_table
                                     WHERE type != "closed"
                                     ORDER BY airport_id

                                  """).dropDuplicates().dropna()

        # load the table in s3 under parquet format
        airport_table.write.partitionBy("state_code","city").mode("append").parquet(output_bucket+'airports_data.parquet')

        airport_table.show(n=3)
        return airport_table.count()

# Cleaning tasks for immigration data and creation of immigrant dimension table
# 1- Select valuable columns
# 2- Drop duplicate entries
# 3- Drop lines containing Null value
# 4- Put the table at the first normal form

def process_immigrant_data():
        """
            The function:
            Extracts data from SAS_data
            Transforms the data and create immigrant_table
            Loads immigrant_table back to S3 in parquet format.
        """
      # Create immigrant_table
        immigrant_table = df_spark_immigration.createOrReplaceTempView("immigrant_table")
        immigrant_table=spark.sql(""" SELECT int(cicid) imm_id,
                                           int(i94bir) age,
                                           biryear birth_year,
                                           gender,
                                           visatype visa_type,
                                           i94addr state_code
                                    FROM immigrant_table
                                 """).dropDuplicates().dropna()

        # load the table in s3 under parquet format
        immigrant_table.write.mode("append").parquet(output_bucket+'immigrants_data.parquet')

        immigrant_table.show(n=3)
        return immigrant_table.count()


# This function converts SAS date format to datetime
def sas_to_datetime(date):
    """This function converts SAS date format to datetime"""
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')

sas_to_datetime_udf = udf(sas_to_datetime, DateType())


# Performing cleaning tasks for immigration data and creation of immigration Fact table
# 1- Select valuable columns
# 2- Drop duplicate entries
# 3- Drop lines containing Null value
# 4- Put the table at the first normal form

def process_immigration_data():
        """
            The function:
            Extracts data from SAS_data
            Transforms the data and create immigration_table
            Loads immigration_table back to S3 in parquet format.
        """

       # Create immigration_table
        immigration_table = spark.sql("""SELECT int(immigration_table.cicid) imm_id,
                                               airport_table.ident airport_id,
                                               md5(city||state_name) city_id,
                                               int(immigration_table.arrdate) arrival_date,
                                               int(immigration_table.depdate) departure_date,
                                               CASE WHEN int(immigration_table.i94mode)=1 THEN "air"
                                                    WHEN int(immigration_table.i94mode)=2 THEN "sea"
                                                    WHEN int(immigration_table.i94mode)=3 THEN "Land"
                                                    ELSE "Not reported"
                                                    END as mode,
                                               immigration_table.i94addr state_code,
                                               int(immigration_table.i94yr) immigration_year,
                                               int(immigration_table.i94mon) immigration_month,
                                               airport_table.latitude,
                                               airport_table.longitude

                                          FROM  immigration_table
                                          JOIN  immigrant_table
                                          ON    immigration_table.cicid=immigrant_table.cicid
                                          JOIN  airport_table
                                          ON    immigrant_table.i94addr=airport_table.iso_region
                                          JOIN  cities_table
                                          ON    airport_table.municipality=cities_table.City AND airport_table.iso_region=cities_table.state_code

                                        """).dropDuplicates().dropna()

       # Convert SAS arrival_date and departure_date columns to Datetime format
        immigration_table=immigration_table.withColumn("arrival_date", sas_to_datetime_udf("arrival_date"))
        immigration_table=immigration_table.withColumn("departure_date", sas_to_datetime_udf("departure_date"))

       # Create the stay_duration_in_days column
        immigration_table=immigration_table.withColumn("stay_duration_in_days", datediff(col("departure_date"),col("arrival_date")))
        immigration_table=immigration_table.withColumn("stay_duration_in_days", round(immigration_table["stay_duration_in_days"], scale=0))

       # Create the stay_duration_in_weeks column
        immigration_table=immigration_table.withColumn("stay_duration_in_weeks", datediff(col("departure_date"),col("arrival_date"))*52/365)
        immigration_table=immigration_table.withColumn("stay_duration_in_weeks", round(immigration_table["stay_duration_in_weeks"], scale=1))

       # Create the stay_duration_in_months column
        immigration_table=immigration_table.withColumn("stay_duration_in_months", months_between(col("departure_date"),col("arrival_date")))
        immigration_table=immigration_table.withColumn("stay_duration_in_months", round(immigration_table["stay_duration_in_months"], scale=1))

       # Create the stay_duration_in_years column
        immigration_table=immigration_table.withColumn("stay_duration_in_years", datediff(col("departure_date"),col("arrival_date"))/365)
        immigration_table=immigration_table.withColumn("stay_duration_in_years", round(immigration_table["stay_duration_in_years"], scale=1))

       # load the table in s3 under parquet format
        immigration_table.write.partitionBy("immigration_year","immigration_month").mode("append").parquet(output_bucket+'immigration_data.parquet')

        immigration_table.show(n=3)
        return immigration_table.count()



# This function triggers the ETL process on the Three datasets, create the star schema data model and load the Fact and dimension tables in to S3

def pipeline():

    """This function triggers the ETL process on the Three datasets,
       creates the star schema data model and load the Fact and dimension tables in S3
       in parquet format
    """
    count_cities_table=process_cities_data()
    print("cities data ETL done.")

    count_airport_table=process_airport_data()
    print("Airports data ETL done.")

    count_immigrant_table=process_immigrant_data()
    print("immigrants data ETL done.")

    count_immigration_table=process_immigration_data()
    print("immigration data ETL done.")

pipeline()


def check_count_dataset_to_spark(df_spark, df_spark_name):

    """ This function checks the number of rows loaded from dataset files/folder to spark dataframe"""

    numrows = df_spark.count()
    if numrows > 0:
        print(f"{numrows} rows loaded to {df_spark_name}")
        return True
    else:
        return False

check_count_dataset_to_spark(df_spark_cities, 'df_spark_cities')
check_count_dataset_to_spark(df_spark_airport, 'df_spark_airport')
check_count_dataset_to_spark(df_spark_immigration, 'df_spark_immigration')

def check_count_table_to_s3(num_rows, table_name):

     """ This function checks the number of rows loaded from tables to s3
         since dropDuplicales() and dropna() functions have been applied
         """
    if num_rows > 0:
        print(f"{num_rows} rows loaded from {table_name} to s3")
        return True
    else:
        return False

check_count_table_to_s3(count_cities_table, 'cities_table')
check_count_table_to_s3(count_airport_table, 'airport_table')
check_count_table_to_s3(count_immigrant_table, 'immigrant_table')
check_count_table_to_s3(count_immigration_table, 'immigration_table')
