import kagglehub
from kagglehub import KaggleDatasetAdapter

# Load the latest version of the dataset
df = kagglehub.load_dataset(
  KaggleDatasetAdapter.PANDAS,
  "vishardmehta/faang-stock-market-data-with-technical-indicators",
  ""
)
