import boto3
import pandas as pd
import psycopg2
from io import BytesIO

def export_sales_to_parquet():
    print("Iniciando exportação dos dados da tabela sales para Parquet...")

    try:
        # Conexão com o banco PostgreSQL
        conn = psycopg2.connect(
            host='postgres',
            database='salesdb',
            user='airflow',
            password='airflow'
        )

        query = "SELECT * FROM sales;"
        df = pd.read_sql_query(query, conn)
        print(f"Dados extraídos do PostgreSQL: {len(df)} linhas")

        # Salvar dataframe em memória no formato Parquet
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)  # Resetar ponteiro do buffer

        # Configurar cliente S3 (LocalStack)
        s3 = boto3.client(
            's3',
            endpoint_url="http://localstack:4566",
            aws_access_key_id="admin",
            aws_secret_access_key="admin"
        )
        
        bucket_name = 'sales-curated'
        key_name = 'parquet_data/sales_data.parquet'

        # Enviar arquivo Parquet para o bucket
        s3.put_object(Bucket=bucket_name, Key=key_name, Body=buffer.getvalue())
        print(f"Arquivo Parquet enviado para s3://{bucket_name}/{key_name}")

    except Exception as e:
        print("Erro durante exportação para Parquet:", e)

    finally:
        if 'conn' in locals():
            conn.close()
        print("Conexão com PostgreSQL encerrada.")
