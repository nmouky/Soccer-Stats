# -*- coding: utf-8 -*-
"""
Created on Wed Aug  4 14:10:12 2021
 
@author: nmadmin2
"""

#!/usr/bin/env python
# coding: utf-8

#%% Libraries
import pandas as pd
import requests
from requests_negotiate_sspi import HttpNegotiateAuth
import os
import sys
import datetime
import zipfile
import pysftp
import glob
import shutil

# import class from python file
from pyodbc_nova import SQL_Extract_Slips

#%% Set directory to root
os.chdir("F:/Nabih/LOBs/")

#%% DateTime to add to Log file

dt = datetime.datetime.now()
dt_mod = dt.strftime("%d-%m-%y %H%p")

#%% Retrieve slips from RKH and LUX databases

pd.set_option('display.max_colwidth', None)  
Slips = SQL_Extract_Slips()

#RKH & LUX databases
df = Slips.RKH_query
# df_LUX = Slips.LUX_query

# df = df_RKH.append(df_LUX, ignore_index = True)
df_sorted = df.sort_values(['CreateDate'],ascending=True).reset_index()
del df_sorted['index']
df_sorted.head()
df_sorted.groupby(['DepartmentName']).count()

# Column data types
df_sorted.dtypes

df_sorted.DocumentID = df_sorted.DocumentID.apply(lambda x: str(x))
df_sorted.RootObjectID = df_sorted.RootObjectID.apply(lambda x: str(x))
df_sorted.Reference = df_sorted.Reference.apply(lambda x: str(x))
df_sorted["Reference"] = df_sorted["Reference"].astype(str)
df_sorted.dtypes
LOB = df_sorted["DepartmentName"].unique()
LOB.sort()

#%% Change working directory
path = r"G:/Slips/"
os.chdir(path)
os.getcwd()

#%% Number of records
folder_size = 300  # Number of slips per folder
length = len(df_sorted['DocumentFileURL']) #Number of slips per LOB
print("Total number of slips " + str(LOB), length)

#%% Remove all log files from VM

directory = "G:/Log Files/"
files_in_directory = os.listdir(directory)
filtered_files = [file for file in files_in_directory if file.endswith(".txt")]
for file in filtered_files:
	path_to_file = os.path.join(directory, file)
	os.remove(path_to_file)

#%% Download 300 slips per folder per LOB

stdoutOrigin=sys.stdout 
dt_mod
#sys.stdout = open( str(df_RKH_sorted['DepartmentName'].iloc[0]) + dt_mod + ".txt", "w+")
sys.stdout = open( "G:\Log Files\ " +dt_mod + ".txt", "w+") 
for y in LOB:
    x = df_sorted[df_sorted['DepartmentName'] == y]
    links = x['DocumentFileURL']
    length = len(x['DocumentFileURL'])
    print(y,length)
#     print(x['DocumentFileURL'])
    
    folder_size = 300
    length = len(x['DocumentFileURL'])
    for record_num in range(length):
        folder_num = record_num // folder_size
        folder_name = f" {folder_num} " + str(y) 
        if not os.path.exists(folder_name):
            os.mkdir(folder_name)

        url = x['DocumentFileURL'].iloc[record_num]
#         print(url)
        r = requests.get(url, auth=HttpNegotiateAuth())
        Reference = x['Reference'].iloc[record_num]
        UMR = x['UniqueMarketRef'].iloc[record_num]
        DocType = x['DocumentFileType'].iloc[record_num]
        DocID = x['DocumentID'].iloc[record_num]
        RootID = x['RootObjectID'].iloc[record_num]
        DocURL = x['DocumentFileURL'].iloc[record_num]
        filename = DocID.rsplit()[0] + '_' + RootID.rsplit()[0] + '_' + Reference.rsplit()[0] + '.' + DocType.rsplit()[0]
        #print(filename)
        try:
            with open(f"{folder_name}/{record_num} - {filename}", "wb") as fcont:
                fcont.write(r.content)
                print(filename + " , OK " + ", " + UMR)
        except OSError:
                print(filename + " , FILE NOT FOUND  " +  ", " + UMR)
                continue
sys.stdout.close()
sys.stdout=stdoutOrigin


#%% Declare the function to return all file paths in selected directory
def retrieve_file_paths(dirName):
    
    # File paths variable
    filePaths = []
    
    # Read all directory, subdirectories and file lists
    # for root, directories, files in os.walk(dirName):
    #     for dir in directories:
    #         for root, directories, files in os.walk(dir):
    #             for filename in files:
    #                 print(filename)
    #             # Createthe full filepath by using os module
    #                 filePath = os.path.join(dir, filename)
    #                 filePaths.append(filePath)
            
    # # return all paths
    # return filePaths

    for root, dirs, files in os.walk(dirName):
       for dir in dirs:
        zf = zipfile.ZipFile(os.path.join(root+dir, root+dir+'.zip'), "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9)
        files = os.listdir(root+dir)
        print(files)
    
        filePaths.append(files)
        for f in files:
            filepath = root + dir +'/'+ f
            zf.write(filepath, arcname=f)
        zf.close()

retrieve_file_paths(path)



#%% Upload log file to Cerberus (via SFTP) 
logpath = r"G:/Log Files/"
text_files = glob.glob(logpath + "/**/*.txt", recursive = True)
logfiles = ''.join(text_files)

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

sftp = pysftp.Connection("sfp-ne-02", username='int-digitize', password='ProjectNova1!', cnopts=cnopts)

with sftp.cd():
    sftp.chdir("LogFiles_Nova")
    sftp.put(logfiles, preserve_mtime=True)

#%% Remove LogFile

directory = "G:/Log Files/"
files_in_directory = os.listdir(directory)
filtered_files = [file for file in files_in_directory if file.endswith(".txt")]
for file in filtered_files:
	path_to_file = os.path.join(directory, file)
	os.remove(path_to_file)
    
#%% Remove all folders & files (keep zipped folders)

directory = "G:/Slips/"
files_in_directory = os.listdir(directory)
filtered_files = [file for file in files_in_directory if not file.endswith(".zip")]
for file in filtered_files:
	path_to_file = os.path.join(directory, file)
	shutil.rmtree(path_to_file)




#%% Connect to SFTP site and move zipped files 

zipfiles = [file for file in files_in_directory if file.endswith(".zip")]
fullpaths = []
for x in zipfiles:
    fullpaths.append(os.path.abspath(x))
    Zipped = ''.join(fullpaths)
    
    

# Upload files 
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

sftp = pysftp.Connection("sfp-ne-02", username='int-digitize', password='ProjectNova1!', cnopts=cnopts)

with sftp.cd():
    sftp.chdir("LOB_Download_Nova")
    for x in fullpaths:
        sftp.put(x, preserve_mtime=True)
        
        
#%% Remove all folders and files from VM

directory = "G:/Slips/"
files_in_directory = os.listdir(directory)
filtered_files = [file for file in files_in_directory if file.endswith(".zip")]
for file in filtered_files:
	path_to_file = os.path.join(directory, file)
	os.remove(path_to_file)  
    
