import pandas as pd
import requests, zipfile, io
import glob
import os

path_to_save_csv = './summary_of_deposit/'

isExist = os.path.exists(path_to_save_csv)

# Check to see if folder does not exist
if not isExist:
    # Create folder
    fdic_summary_of_deposit = r'https://www7.fdic.gov/sod/download/all_2020.zip'

    # Extracting the all_2020.zip and creating a folder to place the content.
    r = requests.get(fdic_summary_of_deposit)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path_to_save_csv)

os.chdir(path_to_save_csv)

# Obtain the content within the zipped folder
files = glob.glob('*.csv')

# Combine the content within the zipped folder into one df
combined_csv_df = pd.concat([pd.read_csv(file,  encoding= 'unicode_escape') for file in files ]).reset_index()

# Extract only the necessary columns
summary_of_deposit_df = combined_csv_df[['CERT', 'NAMEBR','CNTRYNAB','ASSET','DEPDOM','DEPSUM','FDICNAME','INSURED']]

## Re-name the columns of the df 
summary_of_deposit_df.rename(columns={'CERT': 'cert', 'NAMEBR': 'branch_name', 
                                    'ASSET': 'asset', 'DEPDOM': 'total_domestics_deposits'
                   , 'DEPSUM': 'total_deposits', 'INSURED': 'insured'}, inplace=True)

# Group data by Cert and branch name
summary_of_deposit_df_grouped = summary_of_deposit_df.groupby(['cert','branch_name'])

# Obtain the first record from each group
summary_of_deposit_df_grouped_by_cert_and_branch_name = summary_of_deposit_df_grouped.first()

# Export into a CSV file within the folder named: FDIC
summary_of_deposit_df_grouped_by_cert_and_branch_name.to_csv('../summary_of_deposit_fact.csv', encoding='utf-8')

