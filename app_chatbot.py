import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

st.set_page_config(page_title="Com2uS Analyst", layout="wide")
st.title("📊 Com2uS 초경량 데이터 분석기")

@st.cache_resource
def get_clients():
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    ai_client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=st.secrets["GROQ_API_KEY"])
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# --- 사이드바: 컬럼 검색 (토큰 절약을 위해 필수) ---
with st.sidebar:
    st.header("🔍 컬럼 검색")
    search_keyword = st.text_input("키워드 입력")
    if search_keyword:
        col_query = f"SELECT column_name FROM `com2us-bigquery.MKT_AI.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'cv_creative_image_features' AND column_name LIKE '%{search_keyword}%' LIMIT 10"
        st.dataframe(client_bq.query(col_query).to_dataframe(), hide_index=True)
    if st.button("대화 기록 초기화"):
        st.session_state.messages = []
        st.rerun()

# --- 시스템 프롬프트 (최대한 짧게 요약) ---
TABLE_ID = "com2us-bigquery.MKT_AI.cv_creative_image_features"
SYSTEM_PROMPT = f"너는 BigQuery 전문가야. SQL 작성 시 한글 주석 금지, ```sql [코드] ``` 형식을 지켜라. 테이블: {TABLE_ID}"

if "messages" not in st.session_state:
    st.session_state.messages = []

# 대화 출력
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- 메인 로직 ---
if prompt := st.chat_input("질문하세요"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # [토큰 최적화] 최근 3개의 메시지만 AI에게 전달
        input_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + st.session_state.messages[-3:]

        try:
            response = client_ai.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=input_messages,
                temperature=0
            )
            ai_answer = response.choices[0].message.content
            
            if "```sql" in ai_answer:
                sql = ai_answer.split("```sql")[1].split("```")[0].strip()
                with st.status("데이터 조회 중..."):
                    # [권한 에러 방지] Storage API 미사용 설정
                    query_job = client_bq.query(sql)
                    df = query_job.result().to_dataframe(create_bqstorage_client=False)
                    st.dataframe(df)

                # [토큰 최적화] 데이터 요약 시 컬럼 550개를 다 보내지 않고 상위 3개 행의 일부만 전달
                summary_data = df.head(3).to_string()
                summary_res = client_ai.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[{"role": "user", "content": f"이 데이터 요약해줘: {summary_data}"}]
                )
                final_text = summary_res.choices[0].message.content
                st.markdown(final_text)
                st.session_state.messages.append({"role": "assistant", "content": f"SQL 실행 결과입니다.\n{final_text}"})
            else:
                st.markdown(ai_answer)
                st.session_state.messages.append({"role": "assistant", "content": ai_answer})

        except Exception as e:
            st.error(f"오류 발생: {e}")