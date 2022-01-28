import functools
from functools import reduce
from datetime import date

# create class file properties with outputfile function which uses input file name and creates output file name with required naming standards
class file_properties:
    def output_file(input_file_name):
        input_file_name = input_file_name
        output_file_path_temp = input_file_name.split('/')[0:-1]
        output_file_path = "/".join(output_file_path_temp[:])
        file_date = date.today()
        return f'{output_file_path}/{file_date}'

# Read input file as an input argument
input_file_name = input('Provide input file along with path:')
input_file=open(input_file_name, 'r')

# Read input file into a string variable as lines
file_content = input_file.readlines()
file_content1=[]
# Read each line and split into python list
for i in range(len(file_content)):
    file_content1.append(file_content[i].split("\t"))

# Read split product list field in to multiple field list
product_list_new=[]
for i in range(1,len(file_content1)):
    if file_content1[i][10] != '':
        combined_product_list = functools.reduce(lambda x, y: x + y, file_content1[i][10],).split(",")
        for j in combined_product_list:
            product_list = j.split(";")

            Category = product_list[0]
            Product_Name = product_list[1]
            Number_of_Items = product_list[2]
            Total_Revenue = product_list[3]
            Custom_Event = product_list[4]
            product_list_new.append([product_list, file_content1[i][11].split("/")[2]])
#create an empty dictionary and empty list
dict1={}
list1=[]
#create a dictionary with key value pair and final output sort into Revenue descending order
for i in range(len(product_list_new)):
    if product_list_new[i][1] + ',' + product_list_new[i][0][1]  not in dict1.keys():
        if product_list_new[i][0][3] != '':
            dict1[product_list_new[i][1] + ',' + product_list_new[i][0][1]] = int(product_list_new[i][0][3])
            
        else:
            dict1[product_list_new[i][1] + ',' + product_list_new[i][0][1]] = 0
    else:
        if product_list_new[i][0][3] != '':
            dict1[product_list_new[i][1] + ',' + product_list_new[i][0][1]] = dict1[product_list_new[i][1] + ',' + product_list_new[i][0][1]] + int(product_list_new[i][0][3])
        else:
            dict1[product_list_new[i][1] + ',' + product_list_new[i][0][1]] = dict1[product_list_new[i][1] + ',' + product_list_new[i][0][1]] + 0

dict2=dict(sorted(dict1.items(), key=lambda kv: kv[1], reverse=True))

# call file properties calls function output file to get output file name with path
output_file_name=file_properties.output_file(input_file_name)
#create output file with final dictionary results in required format
fc = open(f'{output_file_name}_SearchKeywordPerformance.tab', "w+")
for key, value in list(dict2.items()):
    fc.writelines((key + ',' + str(value) + '\n').replace('www.', ''))

fc.close()

