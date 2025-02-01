import pandas as pd
import os 
import glob 

"""
#Day 1
#2 Extract the following variables RCFD2170

file_match = "FFIEC CDR Call Schedule RC *.txt" #looking for RC txt file in each sub folder
list_files = []

import re 
def year_extraction(file_matching_expression):
    match = re.search(r"(\d{4})$", file_matching_expression)
    return match.group(1)

for folder in os.listdir("/Users/wgoogle/Desktop/FunAssn"): # going through each folder (one folder per year) in my big Folder that holds them all
    folder_validate = os.path.join("/Users/wgoogle/Desktop/FunAssn", folder)

    matching = glob.glob(os.path.join(folder_validate, file_match)) #forms the search path and makes it recursively match
    if matching:
        yr = year_extraction(folder)
        list_files.append((yr, matching[0])) #store the first match into a list with all the relevant documents, in this case the RC documents

storage = []
for yr, document in list_files:
    df = pd.read_csv(document, delimiter = "\t", header = 0) #taking a look at the files, I see they are in a tsv format
    df = df["RCFD2170"].dropna().drop(0, axis = 0) # extracting the variable requested and dropping NaN values 
    storage.append({"Year": yr, "Variable": "RCFD2170", "Data": df.tolist()}) # appends the data of each variable for each year

df_DF = pd.DataFrame(storage) # making it back to a dataframe to be able to group it with other things later on
df_DF_pivot = df_DF.pivot(index = "Variable", columns = "Year", values = "Data").fillna("") #making it a pivot table so years can be the columns and to obtain the proper format requested
"""
#Day 2
#make the function and extract step 3, 4, 5 variables
#process is the same as a single one (as made in the above code) but generalized into functions so can use it for any variable extraction

import re
def year_extraction(file_matching_expression):
    match = re.search(r"(\d{4})$", file_matching_expression)
    return match.group(1)

def folder_extraction(file_matching_expression, variable_name): #same process as above, but changed variable names and generalized to a function
    list_files_func = []

    for folder2 in os.listdir("/Users/wgoogle/Desktop/FunAssn"):
        folder_validates = os.path.join("/Users/wgoogle/Desktop/FunAssn", folder2)

        matching2 = glob.glob(os.path.join(folder_validates, file_matching_expression)) 
        if matching2:
            yr2 = year_extraction(folder2)
            list_files_func.append((yr2, matching2[0]))

    storaging = []
    for yr2, document in list_files_func:
        df2 = pd.read_csv(document, delimiter = "\t", header = 0) #
        df2 = df2[variable_name].dropna().drop(0, axis = 0)
        storaging.append({"Year": yr2, "Variable": variable_name, "Data": df2.tolist()})

    df2_DF2 = pd.DataFrame(storaging) 
    df2_DF2_pivot = df2_DF2.pivot(index = "Variable", columns = "Year", values = "Data").fillna("")
    return df2_DF2_pivot

"""
#Step 3:
folder_extraction("FFIEC CDR Call Schedule RCK *.txt", "RCON3387")
folder_extraction("FFIEC CDR Call Schedule RCK *.txt", "RCFD3368")

#Step 4
folder_extraction("FFIEC CDR Call Schedule RCCI *.txt", "RCON1763")

#Step 5
folder_extraction("FFIEC CDR Call Schedule RCCII *.txt", "RCON5570")
folder_extraction("FFIEC CDR Call Schedule RCCII *.txt", "RCON5571")
folder_extraction("FFIEC CDR Call Schedule RCCII *.txt", "RCON5572")
folder_extraction("FFIEC CDR Call Schedule RCCII *.txt", "RCON5573")
folder_extraction("FFIEC CDR Call Schedule RCCII *.txt", "RCON5574")
folder_extraction("FFIEC CDR Call Schedule RCCII *.txt", "RCON5575")

"""
#make these into a csv file
df_RC = folder_extraction("FFIEC CDR Call Schedule RC *.txt", "RCFD2170")
df_RCK1 = folder_extraction("FFIEC CDR Call Schedule RCK *.txt", "RCON3387")
df_RCK2 = folder_extraction("FFIEC CDR Call Schedule RCK *.txt", "RCFD3368")
df_RCCI = folder_extraction("FFIEC CDR Call Schedule RCCI *.txt", "RCON1763")

#Day 3 & 4
#finish making it into a csv file, seperating it by the years with columns as the years and variables as the rows
csv_df = pd.concat([df_RC, df_RCK2, df_RCCI, df_RCK1], axis = 0)
csv_df.to_csv("/Users/wgoogle/Desktop/FunAssn/extracted_data3.csv", index = True, encoding="utf-8-sig", quoting=1) #exporting as csv and making excel delimited for proper format
