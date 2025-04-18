import streamlit as st
import pandas as pd
import boto3
import io
import datetime
import os
from pathlib import Path
import matplotlib.pyplot as plt
from dotenv import load_dotenv

def load_env_variables():
    load_dotenv()
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
        "s3_bucket_name": os.getenv("S3_BUCKET_NAME"),
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


def main():

    st.set_page_config(page_title="Stock Data Uploader", layout="centered")
    st.title("Time Series Analysis Dashboard")

    # Load AWS credentials from .env
    aws_credentials = load_env_variables()

    # Validate required environment variables
    if not aws_credentials["aws_access_key_id"]:
        raise ValueError("No AWS Access key id set")
    if not aws_credentials["aws_secret_access_key"]:
        raise ValueError("No AWS Secret Access key set")
    if not aws_credentials["aws_region"]:
        raise ValueError("No AWS Region Set")
    if not aws_credentials["s3_bucket_name"]:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")

    # Using the boto3 library, initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials["aws_access_key_id"],
        aws_secret_access_key=aws_credentials["aws_secret_access_key"],
        region_name=aws_credentials["aws_region"],
    )

    # Streamlit UI
    st.markdown("""
        Upload a CSV file containing any time series data to view historical price/index trends
        moving averages and summary statistics.
        """)

    uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

    if uploaded_file is not None:
        try:
            # Preview CSV
            df = pd.read_csv(uploaded_file)
            st.markdown("##### 📋 Data Preview:")
            st.dataframe(df.head())

            st.markdown('##### Summary Statistics:')
            st.dataframe(df.describe().transpose())

            # Reset stream position before upload
            uploaded_file.seek(0)

            # Upload to S3
            file_name = uploaded_file.name
            s3_key = f"{file_name}"

            """s3_client.upload_fileobj(
                            uploaded_file,
                            aws_credentials["s3_bucket_name"],
                            s3_key
                        )"""

            st.success(f"✅ Uploaded to S3 bucket `{aws_credentials['s3_bucket_name']}` as `{s3_key}`")

            # Plotting data
            df.Date = pd.to_datetime(df.Date)
            df.sort_values(by='Date')
            value_col = df.columns[1]

            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(df['Date'], df[value_col])
            ax.set_title(value_col, fontweight='bold')
            ax.set_xlabel("Date")
            ax.set_ylabel("Value")
            plt.xticks(rotation=45)
            plt.grid()

            st.markdown('##### Original Series')
            st.pyplot(fig)

        except Exception as e:
            st.error(f"Upload failed: {e}")


if __name__ == "__main__":
    main()