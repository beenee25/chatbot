import streamlit as st
from openai import OpenAI
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI

# 1. BigQuery 클라이언트 설정
@st.cache_resource # 매번 연결하지 않도록 캐싱
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

client_bq = get_bigquery_client()
client_ai = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=st.secrets["GROQ_API_KEY"])

# 2. 데이터 조회 함수
def run_query(query):
    query_job = client_bq.query(query)
    return query_job.to_dataframe()

# --- 채팅 UI 부분 ---
st.title("BigQuery 데이터 챗봇 📊")

if prompt := st.chat_input("질문을 입력하세요"):
    # 예: 사용자가 '데이터 보여줘'라고 하면 특정 쿼리 실행
    if "매출" in prompt:
        df = run_query("SELECT date, sales FROM `your_project.your_dataset.sales_table` LIMIT 10")
        st.write("최근 매출 데이터입니다:", df)
        
        # 데이터를 텍스트로 변환해 AI에게 설명 부탁하기
        prompt = f"다음 데이터프레임 내용을 요약해줘: {df.to_string()}"
    



st.title("Groq 기반 초고속 챗봇 ⚡")

# 1. API 키 및 Base URL 설정
# Streamlit Secrets에 GROQ_API_KEY라는 이름으로 키를 저장하세요.
client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=st.secrets["GROQ_API_KEY"]
)

# 2. 대화 기록 초기화
if "messages" not in st.session_state:
    st.session_state.messages = []

# 3. 저장된 대화 기록 출력
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 4. 사용자 입력 처리
if prompt := st.chat_input("메시지를 입력하세요"):
    # 사용자 메시지 표시 및 저장
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 5. AI 응답 생성 및 표시
    with st.chat_message("assistant"):
        # Groq의 Llama 3 모델 사용
        stream = client.chat.completions.create(
            model="llama-3.3-70b-versatile", 
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        )
        response = st.write_stream(stream)
    
    # AI 응답 저장
    st.session_state.messages.append({"role": "assistant", "content": response})