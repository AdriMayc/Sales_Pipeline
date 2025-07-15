import boto3
import pandas as pd
import psycopg2  # type: ignore
from io import BytesIO


def load_data_from_csv():
    print("Iniciando extração do CSV do S3...")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="admin",
        aws_secret_access_key="admin",
    )
    bucket = "sales-raw"
    key = "2025/07/08/sales_data.csv"

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read()
        df = pd.read_csv(BytesIO(content))
        print("CSV carregado com sucesso!")

        df = validate_data(df)

        print("Inserindo no PostgreSQL...")
        conn = psycopg2.connect(
            host="postgres", database="salesdb", user="airflow", password="airflow"
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO sales (order_id, order_date, customer_id, product_id, quantity, unit_price, discount, country)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    row["order_id"],
                    row["order_date"],
                    row["customer_id"],
                    row["product_id"],
                    row["quantity"],
                    row["unit_price"],
                    row["discount"],
                    row["country"],
                ),
            )

        conn.commit()
        print("Dados inseridos com sucesso!")
    except Exception as e:
        print("Erro durante o carregamento dos dados:", e)
    finally:
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()
        print("Conexão com o banco de dados encerrada.")


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Validando dados...")
    initial_rows = len(df)
    df = df.dropna(
        subset=[
            "order_id",
            "order_date",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount",
            "country",
        ]
    )
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] > 0]
    df = df[(df["discount"] >= 0) & (df["discount"] <= 1)]
    final_rows = len(df)
    print(f"Validação concluída: {final_rows}/{initial_rows} linhas válidas")
    return df


def export_sales_parquet():
    print("Exportando dados do PostgreSQL para .parquet...")

    conn = psycopg2.connect(
        host="postgres", database="salesdb", user="airflow", password="airflow"
    )
    df = pd.read_sql_query("SELECT * FROM sales", conn)
    conn.close()

    df.to_parquet("/tmp/sales_data.parquet", index=False)
    print("Arquivo Parquet gerado com sucesso!")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="admin",
        aws_secret_access_key="admin",
    )
    with open("/tmp/sales_data.parquet", "rb") as f:
        s3.upload_fileobj(f, "sales-curated", "parquet_data/sales_data.parquet")

    print("Arquivo Parquet enviado para o bucket sales-curated.")


def load_fact_sales():
    print("Iniciando carga na tabela fact_sales...")

    df = pd.read_parquet("/tmp/sales_data.parquet")
    print("Dados do parquet lidos:")
    print(df.head())

    conn = psycopg2.connect(
        host="postgres", database="salesdb", user="airflow", password="airflow"
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_sales (
            order_id VARCHAR(50) PRIMARY KEY,
            order_date DATE,
            customer_id VARCHAR(50),
            product_id VARCHAR(50),
            quantity INT,
            unit_price NUMERIC,
            discount NUMERIC,
            total_price NUMERIC,
            country VARCHAR(100)
        )
        """
    )
    conn.commit()

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_date"] = df["order_date"].dt.strftime("%Y-%m-%d")  # ✅ CORREÇÃO AQUI

    df["total_price"] = df["quantity"] * df["unit_price"] * (1 - df["discount"])

    for _, row in df.iterrows():
        print(
            f"Inserindo order_id {row['order_id']} com total_price {row['total_price']}"
        )
        cursor.execute(
            """
            INSERT INTO fact_sales (
                order_id, order_date, customer_id, product_id,
                quantity, unit_price, discount, total_price, country
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """,
            (
                row["order_id"],
                row["order_date"],
                row["customer_id"],
                row["product_id"],
                row["quantity"],
                row["unit_price"],
                row["discount"],
                row["total_price"],
                row["country"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("Tabela fact_sales carregada com sucesso!")
