import pandas as pd
import numpy as np

path = '/Users/amigosdadancamooca/Documents/Impacta/Python_for_Data_Engineers/01_Datasets/csv/jcs_2020.csv'
df_jcs = pd.read_csv(path, sep=';', thousands = ',')
print(df_jcs.shape)
print(df_jcs.head())
print(df_jcs.isna().sum())
cols_df_jcs = str.lower(df_jcs.columns).replace(' ','_')
print(cols_df_jcs)