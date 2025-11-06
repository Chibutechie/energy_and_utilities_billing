import pandas as pd

# extract csv file using url

df = pd.read_parquet("https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments/resolve/main/nigerian_energy_and_utilities_billing_payments.parquet")

# covert filee from parquet to csv 
df.to_csv("https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments/resolve/main/nigerian_energy_and_utilities_billing_payments.csv")

df.shape

# print top 20 rows in the table and coulumns in the dataframe

print(df. head(20))
print(df.columns)