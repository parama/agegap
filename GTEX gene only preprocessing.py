#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 20:30:44 2020

@author: michael
"""
""" Gene only GTEX """

"""
IDS rows
Project down number of genes to 20000
Temp: Min max scaler per gene, change later if needed.
Upload data to google drive
Show data processing works on example data
Show data processing works on previous data from paper
"""
import numpy as np
import pandas as pd
import cmapPy
from cmapPy.pandasGEXpress import parse
import os
import matplotlib.pyplot as plt
import sklearn as sklearn
from sklearn.preprocessing import MinMaxScaler

pd.set_option("display.max_columns",200)


filename_test = "example_n50x100.gct.txt"
filename = "GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_tpm.gct"
age_meta = "GTEx_Analysis_2017-06-05_v8_Annotations_SubjectPhenotypesDS.txt"
path = "Users/michael/Downloads"
os.chdir("..")
os.chdir("..")
os.chdir("..")

os.chdir(path)


#reading in whole file using GCT parse too much for my laptop to handle
# header = 2 cuts out some metadata describing data shape
# could try and find a way to pull 10,000 random rows to start
## split data into 12 blocks
# batch = 5000
header = 2
batch = 5000

for i in range(12):
     # Read in batch
     jump = i*batch
     df = pd.read_csv(filename, delimiter='\t', header =header, skiprows = jump, nrows = batch)
     #Perform manipulations on batch
     #if i == 0:
     df.to_csv("UNSCALED_GTEX_Batch_{}".format(i))
     x = df.iloc[:, 2:]
     temp = np.array(x)
     # for some reason, passing whole DF would only properly
     # scale some gene vectors, so i pass them in 1 at a time
     for j in range(len(temp)):
    
        z = np.array(temp[j]).reshape(-1, 1)
        # fit scale for each gene across samples
        scaler = MinMaxScaler()
        scaler.fit(z)
        #scales gene across samples to be [0, 1] 
        scaled = scaler.transform(z)
        # replace unscaled data with scaled data
        df.iloc[j, 2:] = scaled.squeeze() 
     #save? New batch
     df.to_csv("Scaled_GTEX_Batch_{}".format(i))
     # avoid skipping rows as headers after first batch
     header = 0
    
#shape = 56200 X 17382 ? Would that 17382 total samples, but 200k measurements each?
# each column has some unique Gtex barcode GTEX-1117F-0226-SM-5GZZ7, with
##  sample info. 

#test = pd.read_csv(filename, delimiter = '\t', header = 2, usecols=["GTEX-1117F-0226-SM-5GZZ7"])
ENSGs = []
genes = []
filename = "Scaled_GTEX_Batch_0"
df = pd.read_csv(filename)

""" Ignoring all reading in issues for now and using a smaller test set to make
a cleaning pipeline
Cleaning Goals:
    
    Detect all Null cells
        Convert to 0 (maybe not do this? what if a lot of young samples have
                      null values for a particular gene but old dont? we would
                      get a highly predictive artifact rather than real biology)
    Detect all negative cells
        Convert to 0, same concerns as for null cells. could we impute?
Exploration Goals:
    Histogram and summary statistics for expression of each gene across samples
    Variance for genes between tissues and between individuals
    

Quality Check Goals:
    ?
    
    
Baseline model
    Report top 10 "longevity genes" as highest average delta between young and old expression
    maybe split to 10 youthful genes vs 10 aging genes
    """

#find and report overal number of null cells
# could do per sample by only doing one np.sum
null_cell_number = np.sum((np.sum(df.isna())))
print ("number of null cells in data", null_cell_number)

# zero null cells
df.fillna(0)
# check if any negative cells
min_expression = df.min()[3:].min()

# if some are negative in full dataset, ill come back and implement a neg number zero-er


assert(min_expression == 0)
max_expression = df.max()[3:].max()
print ("min transcript expression", min_expression)
print ("max transcript expression", max_expression)
# check max values for unreasonably high TPM


""" Exploration """
# plot expression of single transcript across samples
plt.hist(df.iloc[0, 3:], bins = 50)
    
# plot expression of single sample across transcripts
plt.hist(df.iloc[:, 3], bins = 50)

#test = KNeighborsClassifier.fit(self, ])
""" Baseline Model """

# need to match each sample with its age metadata
#ids = real.columns[2:]

#pd.DataFrame(df.columns[3:]).to_csv("Sample_Ids")
columns = pd.read_csv("Sample_Ids")  
age_df = pd.read_csv(age_meta, delimiter='\t', header = 0)#, skiprows = jump, nrows = batch))
age_sex = age_df.iloc[:, 0:4]

columns['Age'] = pd.Series(dtype=int)
columns['Sex'] = pd.Series(dtype=int)
columns = columns.iloc[:, 1:]

for i in range(len(columns)):
    for j in range(len(age_sex)):
        # if patient id is substring of sample ID
        if age_sex.iloc[j, 0] in columns.iloc[i, 0]:
            columns.iloc[i, 1] = age_sex.iloc[j, 2]
            columns.iloc[i, 2] = age_sex.iloc[j, 3]
            break
columns.to_csv("AgeGap_MetaData")        



