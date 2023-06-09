import pandas as pd

# from ticker_data import search_for_symbol
# Disable Setting WithcopyWarning
pd.options.mode.chained_assignment = None  


# Request the file (https://catalog.data.gov/organization/about/fdic-gov)
fdic_institution_banks = r'https://s3-us-gov-west-1.amazonaws.com/cg-2e5c99a6-e282-42bf-9844-35f5430338a5/downloads/institutions.csv'

# Convert into dataframe
fdic_institution_banks_df = pd.read_csv(fdic_institution_banks, encoding='utf-8')

# Make a copy of the df
df = fdic_institution_banks_df.copy()
 
# Clean the data

## Remove unnecessary columns 
columns_to_drop = ['DOCKET', 'INACTIVE', 'ASSET', 'BKCLASS', 'CHANGEC1', 
                   'CHANGEC2', 'CHANGEC3', 'CHANGEC4', 'CHANGEC5', 'CHANGEC6', 'CHANGEC7', 'CHANGEC8', 
                   'CHANGEC9', 'CHANGEC10', 'CHANGEC11', 'CHANGEC12', 'CHANGEC13', 'CHANGEC14', 'CHANGEC15', 
                   'CHARTER', 'CHRTAGNT', 'CONSERVE', 'CLCODE', 'CMSA_NO', 'CMSA', 'COUNTY', 'DENOVO', 
                   'DEP', 'EFFDATE', 'ENDEFYMD', 'EQ', 'ESTYMD', 'FDICDBS', 
                   'FDICREGN', 'FDICSUPV', 'FED', 'FED_RSSD', 'FEDCHRTR', 'FLDOFF', 'IBA', 
                   'INSAGNT1', 'INSAGNT2', 'INSDATE', 'INSTCRCD', 'INSBIF', 'INSCOML', 'INSDIF', 
                   'INSSAIF', 'INSSAVE', 'MSA_NO', 'MSA', 'NEWCERT', 'OAKAR', 'OTSDIST', 'OTSREGNM', 
                   'PROCDATE', 'QBPRCOML', 'REGAGNT', 'REPDTE', 'RISDATE', 'STCHRTR', 'ROA', 'ROAQ', 'ROE', 
                   'ROEQ', 'RUNDATE', 'SASSER', 'LAW_SASSER_FLG', 'STALP', 'STCNTY', 'STNUM', 'ZIP', 
                   'SUPRV_FD', 'OCCDIST', 'UNINUM', 'ULTCERT', 'CFPBEFFDTE', 'CFPBENDDTE', 'CFPBFLAG', 
                   'REGAGENT2', 'TE01N528', 'TE02N528', 'TE03N528', 'TE04N528', 'TE05N528', 'TE06N528', 
                   'TE07N528', 'TE08N528', 'TE09N528', 'TE10N528', 'TE01N529', 'TE02N529', 'TE03N529', 
                   'TE04N529', 'TE05N529', 'TE06N529', 'WEBADDR', 'CERTCONS', 'PARCERT', 
                   'CITYHCR', 'DEPDOM', 'FORM31', 'HCTMULT', 'INSTAG', 'MUTUAL', 'NAMEHCR', 'NETINC', 
                   'NETINCQ', 'OFFDOM', 'OFFFOR', 'OFFOA', 'RSSDHCR', 'STALPHCR', 'SUBCHAPS', 'ROAPTX', 
                   'ROAPTXQ', 'TRUST', 'SPECGRP', 'SPECGRPN', 'TRACT', 'CSA', 'CSA_NO', 'CSA_FLG', 'CBSA', 
                   'CBSA_NO', 'CBSA_METRO_NAME', 'CBSA_METRO', 'CBSA_METRO_FLG', 'CBSA_MICRO_FLG', 
                   'CBSA_DIV', 'CBSA_DIV_NO', 'CBSA_DIV_FLG', 'CB']
df = df.drop(columns_to_drop, axis=1)

## Re-name the columns of the df with the following ( cert, name, address, state, city, date_updt, insfdic, inactive, offices)
df.rename(columns={'STNAME': 'state', 'CERT': 'cert', 'ADDRESS': 'address', 
                   'CITY': 'city', 'DATEUPDT': 'date_updt', 'ACTIVE': 'active'
                   , 'INSFDIC': 'insfdic', 'NAME': 'name', 'OFFICES': 'offices'}, inplace=True)

df['id'] = df.index

## Re-order the columns (id, cert, ticker, name, address, state, city, date_updt, inactive, offices, insfdic)
df = df.loc[:,['id','cert','name','address', 'state', 'city', 'date_updt', 'active', 'offices', 'insfdic' ]]

## Change the date_updt format to YYYY-MM-DDDD
df['date_updt'] = pd.to_datetime(df["date_updt"], format='mixed')

# Filter only banks that are active and assign it to a new df and new id
active_institution_df = df[df["active"] == 1]
active_institution_df.reset_index(inplace = True, drop = True)
active_institution_df['id'] = active_institution_df.index

# Export into a JSON or CSV file within the folder named: FDIC
active_institution_df.to_csv('financial_institution_dim.csv', encoding='utf-8')