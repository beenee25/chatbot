import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI

st.set_page_config(page_title="BigQuery AI Assistant", layout="wide")
st.title("BigQuery 데이터 챗봇 📊")

# 1. 클라이언트 초기화 (캐싱 처리)
@st.cache_resource
def get_clients():
    # BigQuery 설정
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    # Groq 설정 (OpenAI 호환 라이브러리 사용)
    ai_client = OpenAI(
        base_url="https://api.groq.com/openai/v1",
        api_key=st.secrets["GROQ_API_KEY"]
    )
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# 2. 대화 기록 초기화
if "messages" not in st.session_state:
    st.session_state.messages = []

# 3. 이전 대화 출력
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 4. 사용자 입력 처리
if prompt := st.chat_input("질문을 입력하세요"):
    # 사용자 메시지 표시 및 저장
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 5. 특수 로직: BigQuery 조회가 필요한 경우
    context_data = ""
    if "매출" in prompt or "데이터" in prompt:
        with st.status("BigQuery에서 데이터를 조회하는 중..."):
            try:
                # 쿼리 실행
                query = "SELECT * FROM `com2us-bigquery.MKT_AI.cv_creative_image_features`"
                df = client_bq.query(query).to_dataframe()
                
                st.write("조회된 데이터 샘플:", df)
                # AI에게 전달할 데이터 텍스트화
                context_data = f"\n\n참고 데이터 (BigQuery): \n{df.to_string(index=False)}"
            except Exception as e:
                st.error(f"BigQuery 에러: {e}")

    # 6. AI 응답 생성 (데이터가 있으면 포함해서 질문)
    with st.chat_message("assistant"):
        # 마지막 유저 질문에 데이터 정보 추가 (필요한 경우만)
        current_messages = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages]
        if context_data:
            current_messages[-1]["content"] += context_data

        stream = client_ai.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=current_messages,
            stream=True,
        )
        response = st.write_stream(stream)
    
    # AI 응답 저장
    st.session_state.messages.append({"role": "assistant", "content": response})