import pandas as pd

# Collect all PPP FOIA data
public_150k_plus_230331_url     = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/dd699b9c-4638-405f-8ba9-07d3d2c92a9d/download/public_150k_plus_230331.csv'
public_up_to_150k_1_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/137436c9-408e-47e9-a7f3-b9a1871c4e11/download/public_up_to_150k_1_230331.csv'
public_up_to_150k_2_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/67b6b208-7116-4e8d-9a56-7168b42cda4a/download/public_up_to_150k_2_230331.csv'
public_up_to_150k_3_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/8765decb-9f66-4d7a-b11f-59c3e57420f7/download/public_up_to_150k_3_230331.csv'
public_up_to_150k_4_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/9c90a564-c63f-4796-bd09-b107aa5559f8/download/public_up_to_150k_4_230331.csv'
public_up_to_150k_5_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/cd2dd797-b78d-40b5-9783-c75afdf140cb/download/public_up_to_150k_5_230331.csv'
public_up_to_150k_6_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/61721d18-2116-4d22-9a3c-287f9bcd6a31/download/public_up_to_150k_6_230331.csv'
public_up_to_150k_7_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/6410069a-3b69-4891-8d4d-1b267749b8cd/download/public_up_to_150k_7_230331.csv'
public_up_to_150k_8_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/fe6d1eab-5617-492a-b05b-1f39ff9b9cd5/download/public_up_to_150k_8_230331.csv'
public_up_to_150k_9_230331_url  = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/1e5bbcee-8db5-4592-8d04-299ba92dc6af/download/public_up_to_150k_9_230331.csv'
public_up_to_150k_10_230331_url = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/0bad5972-6f71-49b6-913f-f2d80934309f/download/public_up_to_150k_10_230331.csv'
public_up_to_150k_11_230331_url = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/cb9b638d-c1dd-4c16-bf42-258b06c52c66/download/public_up_to_150k_11_230331.csv'
public_up_to_150k_12_230331_url = r'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/7371d1ca-57e8-4efb-9993-780cea937449/download/public_up_to_150k_12_230331.csv'

# Combine all PPP FOIA csv into one df
ppp_df = pd.concat(map(pd.read_csv, [   public_150k_plus_230331_url, 
                                        public_up_to_150k_1_230331_url,
                                        public_up_to_150k_2_230331_url,
                                        public_up_to_150k_3_230331_url,
                                        public_up_to_150k_4_230331_url,
                                        public_up_to_150k_5_230331_url,
                                        public_up_to_150k_6_230331_url,
                                        public_up_to_150k_7_230331_url,
                                        public_up_to_150k_8_230331_url,
                                        public_up_to_150k_9_230331_url,
                                        public_up_to_150k_10_230331_url,
                                        public_up_to_150k_11_230331_url,
                                        public_up_to_150k_12_230331_url]),)

# Print the number of rows and columns
print(ppp_df.shape)

# View the column names
print(list(ppp_df))

# Clean Data
## 1. Remove null values
## 2. Change column names
## 3. Remove unnecessary columns
## 4. Convert columns to a specifc type

# Convert df to JSON
                                        
                                