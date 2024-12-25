from langchain_together import ChatTogether
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from utils.logger import AppLog
import os
from dotenv import load_dotenv
from google.cloud import bigquery
# from langchain.vectorstores import FAISS
# from langchain.embeddings import OpenAIEmbeddings
# from langchain.document_loaders import BigQueryLoader
from langchain_together import ChatTogether
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from utils.logger import AppLog
import os

# Load environment variables
load_dotenv()

# Khởi tạo LLM (ChatTogether)
api_key = os.getenv("TOGETHERAI_API_KEY")
if not api_key:
    raise ValueError("API key for TogetherAI is not found in the environment variables.")

llm = ChatTogether(
    model="meta-llama/Llama-3-70b-chat-hf",
    api_key=api_key
)

# Hàm tạo câu lệnh SQL từ câu hỏi
def create_sql_query(question):
    """
    Tạo câu lệnh SQL từ câu hỏi người dùng.

    Args:
        question (str): Câu hỏi của người dùng.

    Returns:
        str: Câu lệnh SQL được tạo ra.
    """
    # Lấy các thành phần từ file .env
    project_id = "group-8-445019"
    dataset_id = "group-8-445019.visualization"
    table_id = "Vietnam_Airlines"

    # Kiểm tra nếu bất kỳ biến nào bị thiếu
    if not project_id or not dataset_id or not table_id:
        raise ValueError("PROJECT_ID, DATASET_ID, or TABLE_ID is missing in the environment variables.")

    # Gộp thành `full_table_id`
    full_table_id = f"{dataset_id}.{table_id}"
    AppLog.info(f"Full table ID resolved: {full_table_id}")

    sql_prompt = f"""
    You are an assistant who responds strictly with SQL queries and no additional explanations or comments.
    The table `{full_table_id}` contains the following columns:
    ['date_review', 'day_review', 'month_review', 'month_review_num', 'year_review', 
     'verified', 'name', 'month_fly', 'month_fly_num', 'year_fly', 'month_year_fly', 
     'country', 'aircraft', 'aircraft_1', 'aircraft_2', 'type', 'seat_type', 
     'route', 'origin', 'destination', 'transit', 'seat_comfort', 'cabin_serv', 
     'food', 'ground_service', 'wifi', 'money_value', 'score', 'experience', 
     'recommended', 'review'].

    Always use fully qualified table names (e.g., `group-8-445019.visualization.Vietnam_Airlines`) and proper BigQuery syntax. 
    Make sure the SQL query includes proper backticks (`) around table names and column names.

    Ignore unnecessary columns like 'id'.

    Respond with only the SQL command needed to answer the question. Do not include any explanations or comments.

    Question: {question}
    """
    AppLog.info(f"Generating SQL query for question: {question}")
    try:
        query_response = llm.predict(sql_prompt)  # Gọi LLM để sinh truy vấn SQL
        query = query_response.strip().replace("```", "").strip()  # Xử lý kết quả
        AppLog.info(f"Generated SQL Query: {query}")
        return query
    except Exception as e:
        AppLog.error(f"Failed to generate SQL query: {e}")
        raise

# Hàm thực thi query trên BigQuery
def execute_query(query, client):
    """
    Thực thi câu lệnh SQL trên BigQuery.

    Args:
        query (str): Câu lệnh SQL.
        client (google.cloud.bigquery.Client): Client kết nối với BigQuery.

    Returns:
        list[dict]: Kết quả của truy vấn.
    """
    AppLog.info(f"Executing SQL query: {query}")
    try:
        query_job = client.query(query)
        result = [dict(row) for row in query_job.result()]
        AppLog.info("SQL query executed successfully.")
        return result
    except Exception as e:
        AppLog.error(f"Failed to execute query: {query}\nError: {e}")
        raise RuntimeError(f"Failed to execute query: {e}")

# Hàm tạo câu trả lời từ kết quả SQL
def generate_response(question, sql_result):
    """
    Tạo câu trả lời cuối cùng từ kết quả SQL.

    Args:
        question (str): Câu hỏi của người dùng.
        sql_result (list[dict]): Kết quả SQL.

    Returns:
        str: Câu trả lời cuối cùng.
    """
    answer_prompt = PromptTemplate.from_template(
        """You are a highly skilled data analyst with a flair for storytelling and market analysis. 
        Based on the user question and the result from the database, craft an eloquent, insightful, and engaging response suitable for a senior executive. 
        Highlight key findings and provide meaningful context to the data while avoiding any mention of SQL or raw data.

        User Question: {question}
        Database Result: {result}

        Answer:
        """
    )
    AppLog.info("Generating final response...")
    try:
        final_prompt = answer_prompt.format(
            question=question,
            result=sql_result
        )
        answer = llm.predict(final_prompt)  # Sử dụng LLM để sinh câu trả lời
        AppLog.info("Final response generated successfully.")
        return answer.strip()
    except Exception as e:
        AppLog.error(f"Failed to generate response: {e}")
        raise
