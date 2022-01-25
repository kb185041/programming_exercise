#import required python, pyspark and glue libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import boto3
from datetime import date

# Read arguments from Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME", "file_name", "s3_bucket", "input_path", "output_path"])

# Initiate glue context for spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Parse input arguments
input_file_name=args["file_name"]
s3_bucket=args["s3_bucket"]
input_path=args["input_path"]
output_path=args["output_path"]

# Create class with file properties as variables will be used further in the job
class FileProperties:
    def __init__(self,file_format,file_delimeter,header_indicator,file_mode,file_name,file_date,file_path):
        self.file_format = file_format
        self.file_delimeter = file_delimeter
        self.header_indicator = header_indicator
        self.file_mode = file_mode
        self.file_name = file_name
        self.file_date = file_date
        self.file_path = file_path

# Create schema struct type with input file schema
schema = StructType() \
    .add("hit_time_gmt", StringType(), True) \
    .add("date_time", StringType(), True) \
    .add("user_agent", StringType(), True) \
    .add("ip", StringType(), True) \
    .add("event_list", StringType(), True) \
    .add("geo_city", StringType(), True) \
    .add("geo_region", StringType(), True) \
    .add("geo_country", StringType(), True) \
    .add("pagename", StringType(), True) \
    .add("page_url", StringType(), True) \
    .add("product_list", StringType(), True) \
    .add("referrer", StringType(), True)

# Create function to Read input data into glue dynamic frame and written the dynamic frame
def Read_Input_Data(file_path, schema, header_indicator, file_delimeter, file_format):
    try:
        Source_Data_Glue_DF = glueContext.create_dynamic_frame.from_options(
            format_options={"withHeader": header_indicator, "separator": file_delimeter},
            connection_type="s3",
            format=file_format,
            schema=schema,
            connection_options={"paths": [file_path]},
            transformation_ctx="Source_Data_Glue_DF",
        )
    except Exception as e:
        raise e
    return Source_Data_Glue_DF
    
#  Create function to write data to output s3 location
def Write_Output_Data(file_mode, file_delimeter, header_indicator, file_path, file_date, file_format):
    try:
        # Write final dataset to S3 location as tab delimeted single file
        df_final.coalesce(1).write.mode(file_mode).option("delimiter", file_delimeter).option("header", header_indicator)\
                .format(output_file.file_format).save(f"{file_path}/{file_date}")
    except Exception as e:
        raise e

# Job main code starts from here
if __name__ == "__main__":
    try:

        # Instanciate Input_file class object to hold input file properties
        input_file = FileProperties("csv", "\t",True,"read", input_file_name, "", f"s3://{s3_bucket}/{input_path}")

        # Convert Glue Dynamic frame to spark Data frame
        Source_Data_DF = Read_Input_Data(f'{input_file.file_path}/{input_file.file_name}',schema,\
                                        input_file.header_indicator, input_file.file_delimeter,input_file.file_format).toDF()
        
        
        # Flatten event_list field into python list field event_list_new and transpose the data to rows using explode function
        Source_Data_Temp0_DF= Source_Data_DF.withColumn("event_list_new",F.explode(F.split(col("event_list"), ",")))
        
        #Filter data to keep only purchase event as Revenue is actulized only when purchase event occurs
        purchase_event_filtered_DF=Source_Data_Temp0_DF.filter(col("event_list_new") == F.lit("1"))
        
        # Flatten product list into python list field product_list_new and transpose the data to rows using explode function
        transformed_DF= purchase_event_filtered_DF.withColumn("prod_list_new",F.explode(F.split(col("product_list"), ",")))
        
        # Add new fields by parsing prod_list_new as per the business logic mentioned in the document
        expanded_transformed_DF=transformed_DF.withColumn("category",F.element_at(F.split(col("prod_list_new"), ";"),1) )\
                        .withColumn("Product_Name",F.element_at(F.split(col("prod_list_new"), ";"),2) )\
                        .withColumn("Number_of_Items",F.element_at(F.split(col("prod_list_new"), ";"),3) )\
                        .withColumn("Total_Revenue",F.element_at(F.split(col("prod_list_new"), ";"),4) )\
                        .withColumn("Custom_Event",F.element_at(F.split(col("prod_list_new"), ";"),5) )\
                        .withColumn("Domain", F.element_at(F.split(col("referrer"),"/"),3))
        
        #create final data frame with total revenue calculated in domain and search key word group
        df_final=expanded_transformed_DF.select(col("hit_time_gmt"), col("prod_list_new"), col("Product_Name"), col("Total_Revenue"),\
                                    F.regexp_replace(col("Domain"), "www.", "").alias("Domain"))\
                                    .groupBy(col("Domain").alias("Search_Engine_Domain"),col("Product_Name").alias("Search_Key_Word"))\
                                    .agg({"Total_Revenue": "sum"}) \
                                    .withColumnRenamed("sum(Total_Revenue)", "Revenue")\
                                    .orderBy(col("Revenue").desc())

        # Instanciate Output_file class object to hold output file properties
        output_file = FileProperties("csv","\t","true","overwrite","SearchKeywordPerformance",date.today(),\
                                        f"s3://{s3_bucket}/{output_path}")

        # Write final dataset to output S3 location
        Write_Output_Data(output_file.file_mode, output_file.file_delimeter, output_file.header_indicator, \
                            output_file.file_path, output_file.file_date, output_file.file_format)
    except Exception as e:
        raise e
    
    
    try:
        
        # This section is created to rename output file in required output naming standards for customer
        s3 = boto3.resource('s3')
        source_bucket = s3_bucket
        srcPrefix = f'{output_path}/{output_file.file_date}'
        client = boto3.client('s3')
        
        # Get the part file name from output in S3 location
        
        response = client.list_objects( \
                        Bucket = source_bucket, \
                        Prefix = srcPrefix \
                        )
        
        part_file_name = response["Contents"][0]["Key"]
        
        
        # create variables with required target file name
        target_source = {'Bucket': source_bucket, 'Key': part_file_name}
        
        target_key = f'{output_file.file_date}_SearchKeywordPerformance.tab'
        
        
        # Copy old file name into target file name with naming standards as per customer requirements
        client.copy(CopySource=target_source, Bucket=source_bucket, Key=target_key)
        
        # Delete the old file
        client.delete_object(Bucket=source_bucket, Key=part_file_name)
    
    except Exception as e:
        raise e
# commit the job
job.commit()
