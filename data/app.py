
# data/generate_sales_data.py

import csv
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

def generate_data(num_rows=100_000, output_path="data/sales_data.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([
            "order_id", "order_date", "customer_id", "product_id",
            "quantity", "unit_price", "discount", "country"
        ])

        for i in range(num_rows):
            order_id = f"ORD-{datetime.now().strftime('%Y%m%d')}-{i:06d}"
            order_date = fake.date_between(start_date='-180d', end_date='today')
            customer_id = f"CUST-{random.randint(1, 9999):04d}"
            product_id = f"PROD-{random.randint(1, 500):04d}"
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(10, 500), 2)
            discount = round(random.uniform(0, 0.3), 2)
            country = fake.country()

            writer.writerow([
                order_id, order_date, customer_id, product_id,
                quantity, unit_price, discount, country
            ])

    print(f"✅ Gerado com sucesso: {output_path}")


if __name__ == "__main__":
    generate_data()


