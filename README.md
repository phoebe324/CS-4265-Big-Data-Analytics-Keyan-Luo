Stock Price Prediction System
  This project uses Apache Spark to predict stock prices for big tech companies. We use Big Data tools because 10 years of stock data and technical indicators are too complex for a single computer to process quickly.
  1. Project Overview
    The system loads 10 years of data for AAPL, MSFT, AMZN, GOOGL, META, and NVDA from distributed storage. It uses Spark to clean the data, calculate features, and train a machine learning model to predict the next day's closing price.
  2. Big Data Stack
    Storage: Data is stored in HDFS or S3 to ensure it can grow easily.
    Syntax: We convert CSV files to Parquet format for faster reading and better compression.
    Processing: Spark DataFrames handle parallel computing to speed up data preparation.
    Analytics: Spark MLlib is used to train independent models for each stock at the same time.
  3. Repository Folders
    /data: Contains data descriptions and small file samples.
    /src: Contains all Python and PySpark code for the project.
    /docs: Contains the project proposal and architecture diagrams.
  4. How to Use
    First, upload your stock CSV files to your distributed storage. Second, run the Spark scripts in the /src folder to transform the data and train the model. Finally, check the output folder for the predicted prices.
  5. Success Metrics
    We check the Root Mean Squared Error (RMSE) to see how accurate the predictions are. We also measure the Processing Time to ensure the Spark cluster is working efficiently.
