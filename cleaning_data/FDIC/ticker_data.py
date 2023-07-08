# try:
# 	from googlesearch import search
# except ImportError:
# 	print("No module named 'google' found")

# def search_for_symbol(institution_name):
# 	# Query to search for symbol
# 	query = institution_name + " ticker symbol"

# 	# Loop through each query
# 	for link in search(query, tld="co.in", num=20, stop=10, pause=2):
# 		# Only displays the queries that contains cnbc
# 		if "https://www.cnbc.com/quotes/" in link or "https://finance.yahoo.com/quote/" in link:
# 			if "https://finance.yahoo.com/quote/" in link:
# 				stock_symbol = link.split('/')[-2]
# 				return stock_symbol
# 			elif "https://www.cnbc.com/quotes/" in link:
# 				stock_symbol = link.split('/')[-1]
# 				return stock_symbol
# 			else: 
# 				return ""

from pytickersymbols import PyTickerSymbols

stock_data = PyTickerSymbols()
industries = stock_data.get_stocks_by_industry('Financials')
for each_entity in industries: 
	if "Minden Exchange Bank " in each_entity['name']:
		print(each_entity)
		break
