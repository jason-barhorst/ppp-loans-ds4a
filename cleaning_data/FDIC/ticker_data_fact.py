import pandas as pd
import yfinance as yf

def transform_dataframes():

    ticker_df = pd.read_csv("tiker_symbol.csv")
    Us_ticker_df = pd.read_csv("US_ticker_symbols.csv")

    print(ticker_df)
    #Us_ticker_df.drop("Index", axis='columns')
    print(Us_ticker_df)

    # Select subset of the DataFrame where there is ticker symbol
    subset_ticker_df = ticker_df[ticker_df['id_tiker'] != 'Not Found']
    print(subset_ticker_df)

    # check if the ticker symbol in ticker_df is present in Us_ticker_df
    subset_ticker_df1 = subset_ticker_df[subset_ticker_df['id_tiker'].isin(Us_ticker_df['Ticker'])].reset_index(drop=True)
    print(subset_ticker_df1)
    return subset_ticker_df1

def get_stock_data(ticker, start_date, end_date):
    # Fetch the stock data from Yahoo Finance
    stock_data = yf.download(ticker, start=start_date, end=end_date, interval='1d')

    # Filter the necessary columns including 'Date'
    filtered_data = stock_data[['Open', 'Close', 'High', 'Low', 'Volume']]
    filtered_data.reset_index(inplace=True)
    filtered_data.rename(columns={'index': 'Date'}, inplace=True)


    return filtered_data

def main():
    df = transform_dataframes()
    # Fetch stock data for each unique ticker symbol in the DataFrame
    all_stock_data = []
    for ticker in df['id_tiker'].unique():
        stock_data = get_stock_data(ticker, "2020-01-01", "2020-09-30")
        stock_data['id_ticker'] = ticker  # Add a column with the ticker symbol
        all_stock_data.append(stock_data)

    # Concatenate all stock data into a single DataFrame
    df = pd.concat(all_stock_data)
    #df.to_csv("tiker_data_fact.csv")
    print(df)

# Print the resulting DataFrame
main()