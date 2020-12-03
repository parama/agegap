import pandas as pd
import sqlite3
from pathlib import Path
import sqlalchemy
from sqlalchemy import create_engine
import time

#------------------------------
# ORIGINAL CSV INPUT FILES
 #- [uniqeSampleID, geneExpressionLevels_subset]
 #- [sampid = uniqueSampleID, smtsd = tissueType]

# TABLES CREATED
# [gene_identifiers, gene_names]
# [individual-sample, [gene-expression-levels-and-age-column tacked on]]
# so one row would be labeled e.g. tissue-subject,
# and there would be a column for each gene, as well
# as a column for age (labeled "age")

# separate db with n tables where it separates out subjects by tissue type (where there are n tissue types)
# [subjects, gene expression levels]
#------------------------------

# 1. Generate megatable

# match unique sample names with indices, then match sample names to age, then append that column by index to table



split_data_pathway = "/Users/parama/github/ageGap/data/"
prefix = "Scaled_GTEX_Final_Batch_"

print("begin age data shaping")
age_df = pd.read_csv(split_data_pathway+"Age_data.csv")
age_df['age_index_col'] = age_df.index
print("number of rows: "+str(len(age_df.index)))
print("final number of columns: "+str(len(age_df.columns)))

labels_df = pd.read_csv(split_data_pathway+"sample_attributes.tsv",sep='\t')
ids_and_tissue_types_df = labels_df[['SAMPID','SMTSD']]
print("number of rows: "+str(len(ids_and_tissue_types_df.index)))
ids_and_tissue_types_df['tissues_index_col']=ids_and_tissue_types_df.index
matched_indices_df = age_df.set_index('SAMPID').join(ids_and_tissue_types_df.set_index('SAMPID'))
print(matched_indices_df.iloc[0:10,:])

#df = pd.DataFrame
list_of_dfs = []
list_construction_start_time = time.time()
for i in range(12):
    file_name = split_data_pathway+prefix+str(i)+".csv"
    additional_df = pd.read_csv(file_name)
    if i==0:
        print(additional_df.iloc[0:5,0:5])
        print("check here")
        print(additional_df.iloc[0, 0])
        print(additional_df['0'])
    # if i=11:
    #     additional_df.set_index('')
    #df = pd.concat([self,additional_df], axis=1)
    append_start_time = time.time()
    list_of_dfs.append(additional_df)
    append_end_time = time.time()
    print("appended df"+str(i))
    print("df length: "+str(len(additional_df.index)))
    print(additional_df.iloc[0:6,0:4])
    print(append_end_time-append_start_time)
    # print("number of columns")
    # print(len(df.columns))

list_construction_end_time = time.time()
print("total time to construct list of dataframes:")
print(list_construction_end_time-list_construction_start_time)
list_concat_start_time = time.time()
df = pd.concat(list_of_dfs)
list_concat_end_time = time.time()
print("total time to concatenate list of dataframes:")
print(list_construction_end_time-list_construction_start_time)
print("final number of rows: "+str(len(df.index)))
print("final number of columns: "+str(len(df.columns)))

# #append age column as last column



# # create new database
# engine = create_engine('sqlite:///dataset.db', echo=True)
# sqlite_connection = engine.connect()
#
# # insert main table
#
# sqlite_table = "all_samples_x_all_genes"
# df.to_sql(sqlite_table, sqlite_connection, if_exists='replace')
#

# connection = sqlite3.connect('dataset.db')
# cursor = connection.cursor()

#construct megatable, save it out, then tables for each tissue type





#TODO: document and generate desired tables
#TODO: generate sqldb file
#TODO: tests! view db in datagripper
#TODO: update database.py file
#TODO: load sqldb to google drive
#TODO: hook up sqldb to colab