from __future__ import annotations
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil

# Constants
OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = "ecommerce_merged"

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
) as dag:

    @task()
    def fetch_orders(output_dir: str = OUTPUT_DIR) -> str:
        import pandas as pd  # Delayed import

        df = pd.read_csv("/opt/airflow/data/olist_orders_dataset.csv")
        df = df[df["order_status"] == "delivered"].copy()
        df = df[["order_id", "order_purchase_timestamp", "order_delivered_customer_date"]].dropna()
        df["purchase_ts"] = pd.to_datetime(df["order_purchase_timestamp"])
        df["delivery_ts"] = pd.to_datetime(df["order_delivered_customer_date"])
        df["delivery_hours"] = (df["delivery_ts"] - df["purchase_ts"]).dt.total_seconds() / 3600
        df = df[df["delivery_hours"] <= 720]  # <= 30 days

        filepath = os.path.join(output_dir, "orders_clean.csv")
        df[["order_id", "delivery_hours"]].to_csv(filepath, index=False)
        print(f"Orders saved to {filepath}")
        return filepath

    @task()
    def fetch_reviews(output_dir: str = OUTPUT_DIR) -> str:
        import pandas as pd  # Delayed import

        df = pd.read_csv("/opt/airflow/data/olist_order_reviews_dataset.csv")
        df = df[["order_id", "review_score"]].dropna()
        df["review_score"] = df["review_score"].astype(int)

        filepath = os.path.join(output_dir, "reviews_clean.csv")
        df.to_csv(filepath, index=False)
        print(f"Reviews saved to {filepath}")
        return filepath

    @task()
    def merge_csvs(orders_path: str, reviews_path: str, output_dir: str = OUTPUT_DIR) -> str:
        import pandas as pd  # Delayed import

        df_orders = pd.read_csv(orders_path)
        df_reviews = pd.read_csv(reviews_path)
        merged = pd.merge(df_orders, df_reviews, on="order_id", how="inner")

        merged_path = os.path.join(output_dir, "merged_data.csv")
        merged.to_csv(merged_path, index=False)
        print(f"Merged data saved to {merged_path}")
        return merged_path

    @task()
    def load_csv_to_pg(conn_id: str, csv_path: str, table: str = "ecommerce_merged", append: bool = False) -> int:
        import pandas as pd

        df = pd.read_csv(csv_path)
        if df.empty:
            print("No rows to insert.")
            return 0

        # Ensure numeric types
        if "delivery_hours" in df.columns:
            df["delivery_hours"] = pd.to_numeric(df["delivery_hours"], errors="coerce")
        if "review_score" in df.columns:
            df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")

        schema = "assignment"
        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        # Explicit column types
        columns_sql = []
        for col in df.columns:
            if col in ["delivery_hours"]:
                columns_sql.append(f'"{col}" DOUBLE PRECISION')
            elif col in ["review_score"]:
                columns_sql.append(f'"{col}" INTEGER')
            else:
                columns_sql.append(f'"{col}" TEXT')
        create_table = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {', '.join(columns_sql)}
            );
        """
        delete_rows = f"DELETE FROM {schema}.{table};" if not append else None

        insert_sql = f"""
            INSERT INTO {schema}.{table} ({', '.join([f'"{col}"' for col in df.columns])})
            VALUES ({', '.join(['%s' for _ in df.columns])});
        """

        rows = [tuple(r) for r in df.to_numpy()]

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)
                if delete_rows:
                    cur.execute(delete_rows)
                cur.executemany(insert_sql, rows)
                conn.commit()
            print(f"Inserted {len(rows)} rows into {schema}.{table}")
            return len(rows)
        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @task()
    def train_simple_model(conn_id: str, table: str = "ecommerce_merged") -> str:
        import pandas as pd
        from sklearn.model_selection import train_test_split
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score
        import joblib
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=conn_id)
        df = hook.get_pandas_df(f'SELECT "review_score", "delivery_hours" FROM assignment."{table}";')

        # Ensure numeric types
        df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")
        df["delivery_hours"] = pd.to_numeric(df["delivery_hours"], errors="coerce")
        df = df.dropna(subset=["review_score", "delivery_hours"])
        df["review_score"] = df["review_score"].astype(int)

        X = df[["delivery_hours"]]
        y = df["review_score"]
        y_binary = (y > 3).astype(int)  # now safe

        X_train, X_test, y_train, y_test = train_test_split(X, y_binary, test_size=0.2, random_state=42)

        model = LogisticRegression()
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        acc = accuracy_score(y_test, preds)

        model_path = "/opt/airflow/data/simple_model.pkl"
        joblib.dump(model, model_path)

        print(f"Model trained with accuracy: {acc:.2f}")
        print(f"Model saved to {model_path}")
        return f"Model accuracy: {acc:.2f}"


    @task()
    def perform_visualization(conn_id: str, table: str = "ecommerce_merged") -> str:
        import pandas as pd
        import matplotlib.pyplot as plt
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=conn_id)
        df = hook.get_pandas_df(f'SELECT "review_score", "delivery_hours" FROM assignment."{table}";')

        # Ensure numeric types
        df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")
        df["delivery_hours"] = pd.to_numeric(df["delivery_hours"], errors="coerce")
        df = df.dropna(subset=["review_score", "delivery_hours"])
        df["review_score"] = df["review_score"].astype(int)

        summary = df.groupby("review_score", as_index=False)["delivery_hours"].mean()
        
        plt.figure(figsize=(8, 5))
        plt.bar(summary["review_score"], summary["delivery_hours"], color="steelblue")
        plt.title("Average Delivery Time by Review Score")
        plt.xlabel("Review Score (1–5)")
        plt.ylabel("Avg Delivery Time (Hours)")
        plt.xticks([1, 2, 3, 4, 5])
        plt.tight_layout()

        img_path = "/opt/airflow/data/analysis_plot.png"
        plt.savefig(img_path)
        plt.close()
        print(f"Visualization saved to {img_path}")
        return img_path


    @task()
    def clear_folder(folder_path: str = "/opt/airflow/data") -> None:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    if "raw" not in file_path:  # keep raw data
                        shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
        print("Clean process completed!")

    # --- Task Orchestration ---
    orders_file = fetch_orders()
    reviews_file = fetch_reviews()
    merged_file = merge_csvs(orders_file, reviews_file)

    load_to_database = load_csv_to_pg(conn_id="Postgres", csv_path=merged_file, table=TARGET_TABLE)
    train_model = train_simple_model(conn_id="Postgres", table=TARGET_TABLE)
    visualization = perform_visualization(conn_id="Postgres", table=TARGET_TABLE)
    clean_folder = clear_folder(folder_path=OUTPUT_DIR)

    load_to_database >> [train_model, visualization] >> clean_folder