#%%
import boto3

def download_parquet_from_s3():
    s3 = boto3.client(
        's3',
        endpoint_url="http://localhost:4566",
        aws_access_key_id="admin",
        aws_secret_access_key="admin"
    )
    bucket_name = 'sales-curated'
    key_name = 'parquet_data/fact_sales.parquet'
    local_path = 'fact_sales.parquet'  # Salvar na pasta atual

    s3.download_file(bucket_name, key_name, local_path)
    print(f"Arquivo {local_path} baixado do S3 com sucesso!")

# Chame essa função antes de carregar o parquet
download_parquet_from_s3()

import pandas as pd
df = pd.read_parquet('fact_sales.parquet')
print(df.head())
# %%
