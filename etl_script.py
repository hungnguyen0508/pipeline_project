
import pandas as pd
import re
import findspark
findspark.init()
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import * 

spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.hadoop.io.nativeio.enable", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

def read_data_from_path(path):
	df = spark.read.json(path)
	date = str(re.findall(r"\d+", path)[0])
	df = df.withColumn("date",lit(date)) 
	return df 

def union_data(list_of_df:list): 
	df=list_of_df[0]
	for i in list_of_df[1:]: 
		df.union(i)
	return df

def select_fields(df):	
	df = df.select("_source.*","date")
	return df 
	
def calculate_devices(df):
	total_devices = df.select("Contract","Mac").groupBy("Contract").count()
	total_devices = total_devices.withColumnRenamed('count','TotalDevices')
	return total_devices

def active_day_count(df): 
    result = df.select('Contract','date').groupBy('Contract').agg(countDistinct('date').alias('Active_Day_Count'))
    return result 
	
def transform_category(df):
	df = df.withColumn("Type",
		   when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
		  .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
				 (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
		  .when((col("AppName") == 'RELAX'), "Giải Trí")
		  .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
		  .when((col("AppName") == 'SPORT'), "Thể Thao")
		  .otherwise("Error"))
	return df 

def calculate_statistics(df): 
	statistics = df.select('Contract','TotalDuration','Type').groupBy('Contract','Type').sum()
	statistics = statistics.withColumnRenamed('sum(TotalDuration)','TotalDuration')
	statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
	return statistics 

def join_table(statistics, total_devices, day_count): 
	combine = statistics.join(total_devices, "Contract", "inner")
	result = combine.join(day_count, "Contract", "inner")
	return result
	
def finalize_result(result):
    result = result.withColumn("Total Duration",
        col("Giải Trí") + col("Phim Truyện") + col("Thiếu Nhi") + col("Thể Thao") + col("Truyền Hình"))
    result = result.withColumn("Max_Value",greatest(col("Phim Truyện"), col("Giải Trí"), col("Thiếu Nhi"), col("Thể Thao"), col("Truyền Hình")))
    result = result.withColumn("Most Watching",
        when(col("Max_Value") == col("Giải Trí"), "Giải Trí")
        .when(col("Max_Value") == col("Phim Truyện"), "Phim Truyện")
        .when(col("Max_Value") == col("Thiếu Nhi"), "Thiếu Nhi")
        .when(col("Max_Value") == col("Thể Thao"), "Thể Thao")
        .when(col("Max_Value") == col("Truyền Hình"), "Truyền Hình")
        .otherwise("Error"))
    return result

def customer_taste(result): 
	result = result.withColumn("Taste", concat_ws("-", 
									   	 when(col("Giải Trí") != 0,lit("Giải Trí"))
										,when(col("Phim Truyện")!= 0, lit("Phim Truyện"))
										,when(col("Thiếu Nhi")!= 0,lit("Thiếu Nhi"))
										,when(col("Thể Thao")!= 0,lit("Thể Thao"))
										,when(col("Truyền Hình")!= 0,lit("Truyền Hình"))
										))
	return result

def save_data(result,save_path):
	result.repartition(1).write.option("header","true").csv(save_path+"/output.csv")
	return print("Data Saved Successfully")

def main(df,save_path):
	print('-------------Selecting fields--------------')
	df = select_fields(df)
	print('-------------Calculating Devices --------------')
	total_devices = calculate_devices(df)
	print('-------------Day Counting---------------------')
	day_count = active_day_count(df)
	print('-------------Transforming Category --------------')
	df = transform_category(df)
	print('-------------Calculating Statistics --------------')
	statistics = calculate_statistics(df)
	print('--------------Join three tables----------------')
	result = join_table(statistics, total_devices, day_count)
	print('-------------Finalizing results --------------')
	result = finalize_result(result)
	print("--------------Identifying customer taste-------------")
	result = customer_taste(result)
	print('-------------Saving Results --------------')
	save_data(result,save_path)
	print('Task finished')
	return result

def main2(df): 
	print('-------------Selecting fields--------------')
	df = select_fields(df)
	print('-------------Calculating Devices --------------')
	total_devices = calculate_devices(df)
	print('-------------Day Counting---------------------')
	day_count = active_day_count(df)
	print('-------------Transforming Category --------------')
	df = transform_category(df)
	print('-------------Calculating Statistics --------------')
	statistics = calculate_statistics(df)
	print('--------------Join three tables----------------')
	result = join_table(statistics, total_devices, day_count)
	print('-------------Finalizing results --------------')
	result = finalize_result(result)
	print("--------------Identifying customer taste-------------")
	result = customer_taste(result)
	print("finished")
	return result


	

