import pandas as pd

# extract csv file using url

data = pd.read_parquet("https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments/resolve/main/nigerian_energy_and_utilities_billing_payments.parquet")

print(data.head(10))
print(data.columns)