import asyncio
import gradio as gr
from text2sql import create_sql_query, execute_query, generate_response
from read_table import read_table
from utils.logger import AppLog
from google.cloud import bigquery, storage
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def process_question(question):
    """
    Pipeline xử lý từ câu hỏi → SQL query → Kết quả → Câu trả lời.
    """
    try:
        # Kiểm tra câu hỏi
        if not question.strip():
            raise ValueError("Question cannot be empty.")

        AppLog.info(f"User question: {question}")

        # Lấy thông tin BigQuery từ .env
        # group-8-445019
        project_id = "group-8-445019"
        dataset_id = "group-8-445019.visualization"
        table_id = "Vietnam_Airlines"
        if not project_id or not dataset_id or not table_id:
            raise ValueError("PROJECT_ID, DATASET_ID, or TABLE_ID is missing in environment variables.")

        # Tạo full_table_id
        full_table_id = f"{dataset_id}.{table_id}"
        AppLog.info(f"Using table: {full_table_id}")
        client = None

        # Tạo BigQuery client
        try:
            # GCS URI (thay đổi theo bucket và file của bạn)
            gcs_uri = "gs://group-8-445019-us-notebooks/group-8-445019-10957479e54c.json"

            # Tách bucket name và file name từ GCS URI
            local_path = "/app/service-account.json"  # Đường dẫn cục bộ

            # Tách bucket name và file name từ GCS URI
            gcs_uri_parts = gcs_uri.replace("gs://", "").split("/", 1)
            bucket_name = gcs_uri_parts[0]
            file_name = gcs_uri_parts[1]

            # Tải nội dung file JSON từ GCS
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)

            # Tải file từ GCS và lưu vào đường dẫn cục bộ
            blob.download_to_filename(local_path)

            # Sau khi file JSON đã được tải xuống và lưu tại local_path, sử dụng file path này cho BigQuery client
            client = bigquery.Client.from_service_account_json(local_path)

            # Bây giờ bạn có thể sử dụng client để tương tác với BigQuery
            print("BigQuery client created successfully.")
        except Exception as e:
            print(f"Error creating BigQuery client: {e}")

        # 1. Chuyển câu hỏi thành SQL
        sql_query = create_sql_query(question)
        AppLog.info(f"Generated SQL Query: {sql_query}")

        # 2. Thực thi SQL query
        sql_result = execute_query(sql_query, client)
        AppLog.info(f"SQL Result: {sql_result}")

        # 3. Sinh câu trả lời từ kết quả SQL
        response = generate_response(question, sql_result)
        AppLog.info(f"Response: {response}")

        return f"SQL Query:\n{sql_query}\n\nResponse:\n{response}"

    except Exception as e:
        AppLog.error(f"An error occurred: {e}")
        return f"Error: {e}"

# Giao diện Gradio
def gradio_interface():
    """
    Khởi tạo giao diện Gradio cho pipeline Text2SQL.
    """
    title = "Text2SQL with BigQuery and LLM"
    description = "Nhập câu hỏi liên quan đến dữ liệu, nhận truy vấn SQL và câu trả lời từ mô hình LLM."
    inputs = gr.Textbox(label="Your Question", placeholder="Enter your question about the data...")
    outputs = gr.Textbox(label="Response", placeholder="Generated SQL Query and Answer...")

    # Khởi tạo ứng dụng Gradio
    interface = gr.Interface(
        fn=process_question,
        inputs=inputs,
        outputs=outputs,
        title=title,
        description=description
    )
    interface.launch()

if __name__ == "__main__":
    gradio_interface()
