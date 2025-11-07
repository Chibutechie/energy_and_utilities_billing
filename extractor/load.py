from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

connection_string = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

engine = create_engine(connection_string)

df = pd.read_parquet("https://huggingface.co/datasets/electricsheepafrica/nigerian_energy_and_utilities_billing_payments/resolve/main/nigerian_energy_and_utilities_billing_payments.parquet")

df.to_csv("nigerian_energy_and_utilities_billing_payments.csv")

df.to_sql(
    name='energy_billing',
    con=engine,
    if_exists='replace', 
    index=False
)

print(f"Successfully loaded {len(df)} rows!")