import pandas as pd
import sqlite3
from pathlib import Path
import sqlalchemy
from sqlalchemy import create_engine
import sqlite3
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

split_data_pathway = "/Users/parama/github/ageGap/data/"
prefix = "Scaled_GTEX_Final_Batch_"

age_df = pd.read_csv(split_data_pathway+"Age_data.csv")
test_age_df = age_df.iloc[0:100,:]
print("test_age_df")
print(test_age_df)
# print("number of rows: "+str(len(age_df.index)))
# print("final number of columns: "+str(len(age_df.columns)))

#------------- pull out sample ID and tissue type columns from reference file

labels_df = pd.read_csv(split_data_pathway+"sample_attributes.tsv",sep='\t')
ids_and_tissue_types_df = labels_df[['SAMPID','SMTSD']]
print("number of tissue type rows: "+str(len(ids_and_tissue_types_df.index)))

#------------- construct list of dataframes with patients grouped by tissue type
# produces dataframe list and corresponding list of tissue type strring

matched_indices_df = age_df.set_index('SAMPID').join(ids_and_tissue_types_df.set_index('SAMPID'))
print(matched_indices_df.columns)
print("matched_indices_df after initial join:")
print(matched_indices_df)

matched_indices_df.index.name = 'SAMPID'

#TODO: fix indices
matched_indices_df.reset_index(inplace=True)
#matched_indices_df.drop(matched_indices_df.columns[len(matched_indices_df.columns)-1], axis=1, inplace=True)
print("matched indices after resetting index and stripping tissue type away")
print(matched_indices_df)

sorted_by_tissue_df = matched_indices_df.groupby('SMTSD')
print("sorted by tissue")
tissue_tables_df_list= [sorted_by_tissue_df.get_group(x) for x in sorted_by_tissue_df.groups]
tissue_table_names = []

#TODO: refine/fix row labels
for tissue_df in tissue_tables_df_list:
    tissue_df_copy = tissue_df.copy()
    tissue_df_copy.index.name = 'mega_table_index'
    #tissue_df.reset_index(drop=True)
    # print(tissue_df)
    tissue_name = tissue_df.iloc[0,2]
    indices = list(range(0,len(tissue_df.index)))
    tissue_df_copy = tissue_df.iloc[:,:-1]
    tissue_df = tissue_df_copy.reset_index(drop=True)
    #print("were the indices restored?")
    print("is the last column gone?")
    print(tissue_df.iloc[0:5,0:5])
    print("tissue name is: "+tissue_name)
    tissue_df = tissue_df_copy
    tissue_name_final = ''.join(tissue_name.split())
    tissue_table_names.append(tissue_name_final)
    print("updated table:")
    print(tissue_df)

print("tissue_types")
print(tissue_table_names)



#------------- construct dataframe lists for mega table

list_of_dfs = []
list_of_test_dfs = []

list_construction_start_time = time.time()

for i in range(0,12):
    print("entered mega table for loop!")
    file_name = split_data_pathway+prefix+str(i)+".csv"
    unstripped_df = pd.read_csv(file_name)
    stripped_df = unstripped_df[2:]
    stripped_df.columns = unstripped_df.iloc[1]
    stripped_df.reset_index(drop=True)
    print("intake has been stripped!")
    if i == 0:
        print("tripped the first case!")
        print(stripped_df.iloc[0:5,0:5])
        age_df_copy = age_df.copy()
        age_df_copy = age_df.rename( columns={'SAMPID' : 'Name'})
        stripped_df = pd.merge(age_df_copy, stripped_df, on='Name')
        print(stripped_df.iloc[0:5,0:5])

    list_of_dfs.append(stripped_df)
    list_of_test_dfs.append(stripped_df.iloc[0:100,0:100]) #smaller test dataset
    print("appended df for batch "+str(i))
    #print("df length: "+str(len(additional_df.columns)))

# timer
list_construction_end_time = time.time()
print(f"Constructed list of dataframes in {list_construction_end_time - list_construction_start_time:0.4f} seconds")

#concat massive dataframe and test dataframe
list_concat_start_time = time.time()

df = pd.concat(list_of_dfs)
test_df = pd.concat(list_of_test_dfs)

list_concat_end_time = time.time()
#timer + dimensions
# print time.time() - t0, "seconds wall time"

#print("total time to concatenate list of dataframes:")
print(list_construction_end_time - list_construction_start_time, "seconds to concatenate df list")
print("final dimensions for mega table: ")
print(df.shape)
print("final dimensions for test table: ")
print(test_df.shape)

# create new database
engine = create_engine('sqlite:///ageGap.db', echo=True)
sqlite_connection = engine.connect()
print("created and connected engine")

# insert main table
sqlite_table = "all_samples_x_all_genes_with_age_mega"
df.to_sql(sqlite_table, sqlite_connection, if_exists='replace')
print("made it to conversion of df")
sqlite_connection.close()

# save out as csv

df.to_csv("all_samples_x_all_genes_with_age_mega.csv")

# # insert test table
# sqlite_table = "test_size_samples_x_genes"
# df.to_sql(sqlite_table, sqlite_connection, if_exists='replace')
# sqlite_connection.close()

#TODO: tests! view db in datagripper
#TODO: update database.py file
#TODO: load sqldb to google drive
#TODO: hook up sqldb to colab