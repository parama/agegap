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
#age_df['age_index_col'] = age_df.index
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
    #reordered_age_by_id_df = pd.DataFrame

    # if i==0:

    #     print(additional_df.iloc[0:5,0:5])
    #     print("check here")
    #     additional_df.rename(columns={'Unnamed: 0': 'SAMPID'}, inplace=True)
    #     temp_df = pd.DataFrame(additional_df['SAMPID'])
    #     # reordered_age_by_id_df = temp_df.set_index('SAMPID').join(age_df.set_index('SAMPID'))
    #     # reordered_age_by_id_df.reset_index(drop=True)
    #     # print("updated reordered")
    #     # print(reordered_age_by_id_df.head)
    #     # print("number of rows in new age column match?")
    #     # print("columns in reordered?")
    #     # print(reordered_age_by_id_df.columns)
    #     # print(len(reordered_age_by_id_df)==len(additional_df))
    #     print("breakbreak")

    list_of_dfs.append(additional_df)
    print("appended df"+str(i))
    #print("df length: "+str(len(additional_df.columns)))

list_construction_end_time = time.time()
print("total time to construct list of dataframes:")
print(list_construction_end_time-list_construction_start_time)
list_concat_start_time = time.time()

df = pd.concat(list_of_dfs)
df_with_age = pd.merge(df,age_df,how=left,on='SAMPID')
print("df_with_age:")
print(df_with_age.iloc[0:5,-1:-5])

list_concat_end_time = time.time()
print("total time to concatenate list of dataframes:")
print(list_construction_end_time-list_construction_start_time)
print("final number of rows: "+str(len(df.index)))
print("final number of columns: "+str(len(df.columns)))


# create new database
engine = create_engine('sqlite:///ageGap.db', echo=True)
sqlite_connection = engine.connect()

# insert main table

sqlite_table = "all_samples_x_all_genes_with_age"
df.to_sql(sqlite_table, sqlite_connection, if_exists='replace')

sqlite_connection.close()
# connection = sqlite3.connect('dataset.db')
# cursor = connection.cursor()

#construct megatable, save it out, then tables for each tissue type





#TODO: document and generate desired tables
#TODO: generate sqldb file
#TODO: tests! view db in datagripper
#TODO: update database.py file
#TODO: load sqldb to google drive
#TODO: hook up sqldb to colab