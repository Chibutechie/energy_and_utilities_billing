import pandas as pd

# load existing data 
df = pd.read_parquet("https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments/resolve/main/nigerian_energy_and_utilities_billing_payments.parquet")

# Split the billing_month column into month and year
df[['year', 'month']] = df['billing_month'].str.split('-', expand=True)

# Reorder columns
cols = ['customer_id', 'disco', 'billing_month', 'year', 'month', 'tariff_band', 'kwh', 'price_ngn_kwh', 'amount_billed_ngn', 'amount_paid_ngn', 'paid_on_time', 'arrears_ngn']
df = df[cols]

print(df.head())