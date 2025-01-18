import os 
import etl_script_for_method2
from datetime import datetime, timedelta 

def get_input_path(): 
    url=str(input("Please provide your data source folder:"))
    return url 

def get_output_path(): 
    url=str(input("Please provide your destination folder:"))
    return url 

def get_list_files(path): 
    list_files=os.listdir(path)
    print(list_files)
    print("How many files do you want to ETL:")
    return list_files 

def start_date(): 
    start_date=str(input("Please give me your first date to ETL in yyyymmdd format:"))
    return datetime.strptime(start_date,"%Y%m%d").date()

def end_date(): 
    end_date=str(input("Please give me your end date to ETL in yyyymmdd format:"))
    return datetime.strptime(end_date,"%Y%m%d").date()

def get_date_list():
    date_list=[]
    current_date=start_date()
    to_date=end_date()
    while current_date<=to_date: 
        date_list.append(current_date.strftime("%Y%m%d"))
        current_date+=timedelta(days=1)
    return date_list 

#Method 2: Read all files and then ETL: 
def main():
    input_path=get_input_path()
    output_path=get_output_path()
    list_files=get_list_files(input_path)
    date_list=get_date_list()
    list_result=[]
    for i in date_list: 
        data=etl_script_for_method2.read_data_from_path(input_path+i+".json")
        list_result.append(data)
    result = etl_script_for_method2.union_data(list_result)
    result = etl_script_for_method2.select_fields(result)
    result = etl_script_for_method2.active_day_count(result)
    result = etl_script_for_method2.save_data(result, output_path) 
    return result

main()






