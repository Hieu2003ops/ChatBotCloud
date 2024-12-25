import asyncio
import gradio as gr
from text2sql import create_sql_query, execute_query, generate_response
from read_table import read_table
from utils.logger import AppLog
from google.cloud import bigquery
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
        project_id = os.getenv("PROJECT_ID")
        dataset_id = os.getenv("DATASET_ID")
        table_id = os.getenv("TABLE_ID")
        if not project_id or not dataset_id or not table_id:
            raise ValueError("PROJECT_ID, DATASET_ID, or TABLE_ID is missing in environment variables.")

        # Tạo full_table_id
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        AppLog.info(f"Using table: {full_table_id}")

        # Tạo BigQuery client
        service_account_json = {
            "type": os.getenv("TYPE"),
            "project_id": project_id,
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": os.getenv("PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI"),
            "token_uri": os.getenv("TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
        }
        client = bigquery.Client.from_service_account_info(service_account_json)

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
