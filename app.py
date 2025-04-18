import streamlit as st
import pandas as pd
import boto3
import io
import datetime
import os
import mysql.connector
from dotenv import load_dotenv
from pathlib import Path
import matplotlib.pyplot as plt

def load_env_variables():
    load_dotenv()
    
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
        "s3_bucket_name": os.getenv("S3_BUCKET_NAME"),
        "rds_host": os.getenv("RDS_HOST"),
        "rds_port": os.getenv("RDS_PORT"),
        "rds_user": os.getenv("RDS_USER"),
        "rds_password": os.getenv("RDS_PASSWORD"),
        "rds_db": os.getenv("RDS_DB"),
    }

def upload_to_s3(s3_client, file_path, bucket_name):
    try:
        with open(file_path, "rb") as file:
            s3_client.upload_fileobj(
                file, bucket_name, f"uploads/{Path(file_path).name}"
            )
        print(f"Successfully uploaded {file_path.name} to S3")
    except Exception as e:
        print(f"Error uploading {file_path.name}: {str(e)}")

def connect_mysql(config):
    try:
        conn = mysql.connector.connect(
            host=config["rds_host"],
            user=config["rds_user"],
            password=config["rds_password"],
            database=config["rds_db"]
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"MySQL connection failed: {err}")
        return None

def create_table_if_not_exists(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS summary_imports (
        id INT AUTO_INCREMENT PRIMARY KEY,
        filename VARCHAR(255),
        timestamp DATETIME
        -- Dynamic summary columns will be added later
    )
    """)

def add_missing_columns(cursor, summary_columns):
    # Ensure each summary stat column exists in the table
    cursor.execute("SHOW COLUMNS FROM summary_imports")
    existing_columns = [row[0] for row in cursor.fetchall()]
    
    for col in summary_columns:
        if col not in existing_columns:
            col_safe = f"`{col}`"
            cursor.execute(f"ALTER TABLE summary_imports ADD COLUMN {col_safe} FLOAT")

def insert_summary(cursor, file_name, timestamp, summary_dict):
    columns = ["filename", "timestamp"] + list(summary_dict.keys())
    values = [file_name, timestamp] + list(summary_dict.values())

    col_str = ", ".join([f"`{col}`" for col in columns])
    placeholders = ", ".join(["%s"] * len(values))

    sql = f"INSERT INTO summary_imports ({col_str}) VALUES ({placeholders})"
    cursor.execute(sql, values)

def main():

    st.set_page_config(page_title="Stock Data Uploader", layout="centered")
    st.title("📈 Time Series Analysis Tools")

    # Load AWS credentials from .env
    env = load_env_variables()

    # Validate required environment variables
    if not env["aws_access_key_id"]:
        raise ValueError("No AWS Access key id set")
    if not env["aws_secret_access_key"]:
        raise ValueError("No AWS Secret Access key set")
    if not env["aws_region"]:
        raise ValueError("No AWS Region Set")
    if not env["s3_bucket_name"]:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")

    # Using the boto3 library, initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=env["aws_access_key_id"],
        aws_secret_access_key=env["aws_secret_access_key"],
        region_name=env["aws_region"],
    )

    # MySQL Connection

    conn = connect_mysql(env)
    cursor = conn.cursor() if conn else None
    if cursor:
        create_table_if_not_exists(cursor)

    # Streamlit UI
    st.markdown("""
        Upload a CSV file containing any time series data to view historical price/index trends
        moving averages and summary statistics.
        """)

    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

    if uploaded_file is not None:
        try:
            df_raw = pd.read_csv(uploaded_file)

            if df_raw.shape[1] < 2:
                st.error("Uploaded CSV must have at least two columns (Date, Value)")
                return

            # Rename for consistency
            df = df_raw.iloc[:, :2]
            df.columns = ["Date", "Value"]

            st.write("📋 Preview:")
            st.dataframe(df.head())

            st.write("📊 Summary:")
            st.dataframe(df.describe().transpose())

            # Upload to S3
            uploaded_file.seek(0)
            s3_client.upload_fileobj(
                uploaded_file,
                env["s3_bucket_name"],
                uploaded_file.name
            )
            st.success(f"✅ Uploaded to S3 bucket `{env['s3_bucket_name']}`")

            if cursor:
                # Flatten describe().transpose() output
                summary = df.describe().transpose().to_dict(orient="index")
                flat_summary = {}
                for col, stats in summary.items():
                    for stat, val in stats.items():
                        flat_summary[f"{stat}_{col}"] = float(val)

                add_missing_columns(cursor, flat_summary.keys())

                insert_summary(
                    cursor,
                    uploaded_file.name,
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    flat_summary
                )
                conn.commit()
                st.success("📥 Summary stats saved to MySQL")

            #--------------- Plotting ----------------------
            df.Date = pd.to_datetime(df.Date)
            df = df.sort_values(by='Date')
            value_col = df.columns[1]

            # Calculate 7-day moving average
            df['7_day_mavg'] = df[value_col].rolling(window=7).mean()

            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(df['Date'], df[value_col], label='Original', alpha=0.6)
            ax.plot(df['Date'], df['7_day_mavg'], label='7-Day Moving Avg', linewidth=2)
            ax.set_title(value_col, fontweight='bold')
            ax.set_xlabel("Date")
            ax.set_ylabel("Value")
            ax.legend()
            plt.xticks(rotation=45)
            plt.grid()

            # Display in Streamlit
            st.markdown('##### Original Series with 7-Day Moving Average')
            st.pyplot(fig)

        except Exception as e:
            st.error(f"Upload failed: {e}")

    if conn:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()