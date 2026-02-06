Data Description
  The dataset used in this project is the FAANG Stock Market Data with Technical Indicators from Kaggle.
  1. How to Access
    We use the kagglehub library to load the data directly. This ensures the project can handle large-scale data without storing massive files in the GitHub repository.

      Python
      import kagglehub
      from kagglehub import KaggleDatasetAdapter
      
      # Load the latest version of the dataset
      df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "vishardmehta/faang-stock-market-data-with-technical-indicators",
        ""
      )
  
  2. Dataset Metadata
    Source: Kaggle (vishardmehta/faang-stock-market-data-with-technical-indicators)
    Time Range: 10 years of daily stock data.
    Companies: AAPL, MSFT, AMZN, GOOGL, META, NVDA.
    Volume: Approximately 15,120 records before feature expansion.
    
  3. Data Schema
    The dataset includes the following key features:
      Date: The trading date.
      Ticker: The stock symbol for the company.
      Price Data: Open, High, Low, Close, and Adjusted Close prices. 
      Volume: The number of shares traded.
      Technical Indicators: Pre-calculated values like RSI (Relative Strength Index), MACD, and Moving Averages.
