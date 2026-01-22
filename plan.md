# Data Collection
1. collect data automatically based on the input symbol (from yfinance, binance)
2. need to handle all frequency
3. need to handle missing data and abnormal data
4. need to handle timezone conversion (follow pytz conversion)
5. need to handle api request rate limit
6. need to store the data into sqlite
7. need to handle edge case

- while getting the data, check database first. Get from api request if the data doesn't exist in database