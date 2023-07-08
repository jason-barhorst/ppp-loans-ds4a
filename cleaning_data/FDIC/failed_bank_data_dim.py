import pandas as pd

# Request the file
fdic_failed_bank_list = r'https://www.fdic.gov/resources/resolutions/bank-failures/failed-bank-list/banklist.csv'

def clean_failed_bank_list(failed_bank_csv):
    # Convert into dataframe
    fdic_failed_bank_list_df = pd.read_csv(fdic_failed_bank_list, encoding='windows-1252')

    # Make a copy of the df
    df = fdic_failed_bank_list_df.copy()

    # Clean the data to have the following column name: cert, bank_name, id_closing_date, fund

    ## Remove the following columns (city, state, acquiring institution)
    columns_to_drop = ['City\xa0', 'State\xa0', 'Acquiring Institution\xa0']
    df = df.drop(columns_to_drop, axis=1)

    ## Re-name the columns of the df with the following (cert, bank_name, id_closing_date, and fund)
    df.rename(columns={'Bank Name\xa0': 'bank_name', 'Cert\xa0': 'cert', 'Closing Date\xa0': 'id_closing_date', 'Fund': 'fund'}, inplace=True)

    ## Re-order the columns to the following (cert, bank_name, id_closing_date, and fund)
    df = df.loc[:,['cert','bank_name','id_closing_date', 'fund']]

    ## Change the data format to YYYY-MM-DDDD
    df['id_closing_date'] = pd.to_datetime(df["id_closing_date"], format='mixed')

    ## Only obtain data from 2020
    df = df[df['id_closing_date'].dt.strftime('%Y') == '2020'].reset_index(drop=True)

    return df


df = clean_failed_bank_list(fdic_failed_bank_list)

# Export into a csv file within the folder named: FDIC
df.to_csv('failed_bank_list.csv', encoding='utf-8')
