import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

# 1. 페이지 설정
st.set_page_config(page_title="Com2uS BigData Analyst", layout="wide")
st.title("📊 Com2uS 대규모 피처 분석기")

# 2. 클라이언트 설정 (캐싱)
@st.cache_resource
def get_clients():
    # Secrets에 gcp_service_account와 GROQ_API_KEY가 등록되어 있어야 합니다.
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    ai_client = OpenAI(
        base_url="https://api.groq.com/openai/v1",
        api_key=st.secrets["GROQ_API_KEY"]
    )
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# 3. 사이드바: 550개 컬럼 중 원하는 컬럼 찾기 기능
with st.sidebar:
    st.header("🔍 컬럼 사전 검색")
    st.info("550개의 컬럼 중 정확한 이름을 확인하세요.")
    search_keyword = st.text_input("찾고 싶은 키워드 (예: ratio, score, dark)")
    
    if search_keyword:
        col_search_query = f"""
            SELECT column_name, data_type 
            FROM `com2us-bigquery.MKT_AI.INFORMATION_SCHEMA.COLUMNS` 
            WHERE table_name = 'cv_creative_image_features' 
            AND (column_name LIKE '%{search_keyword}%')
            LIMIT 20
        """
        try:
            found_cols = client_bq.query(col_search_query).to_dataframe()
            if not found_cols.empty:
                st.dataframe(found_cols, hide_index=True)
            else:
                st.warning("해당 키워드가 포함된 컬럼이 없습니다.")
        except Exception as e:
            st.error(f"검색 오류: {e}")

# 4. 시스템 프롬프트 (전략적 지침)
TABLE_ID = "com2us-bigquery.MKT_AI.cv_creative_image_features"
SYSTEM_MESSAGE = {
    "role": "system",
    "content": f"""너는 BigQuery SQL 생성 전문가이다. 
    이 테이블은 컬럼이 550개이므로, 존재하지 않는 컬럼명을 추측하지 마라.

    [핵심 규칙]
    1. 사용자가 언급한 단어와 가장 유사한 영문 컬럼명을 사용하여 SQL을 작성해라.
    2. 만약 컬럼명이 확실하지 않다면, 사용자에게 사이드바에서 컬럼을 검색해달라고 요청하거나, 
       아래 쿼리를 통해 직접 컬럼 목록을 확인하라고 답변해라.
       ```sql
       SELECT column_name FROM `com2us-bigquery.MKT_AI.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'cv_creative_image_features' AND column_name LIKE '%키워드%'
       ```
    3. 결과는 반드시 ```sql [코드] ``` 블록을 사용하고, 한글 주석은 절대 달지 마라.
    4. 테이블명: `{TABLE_ID}`
    """
}

# 대화 기록 관리
if "messages" not in st.session_state:
    st.session_state.messages = [SYSTEM_MESSAGE]

for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# 5. 메인 채팅 로직
if prompt := st.chat_input("질문을 입력하세요 (예: tone_dark_ratio가 높은 이미지 5개)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # AI에게 SQL 생성 요청
        response = client_ai.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=st.session_state.messages,
            temperature=0
        )
        ai_answer = response.choices[0].message.content
        
        # SQL 블록이 포함되어 있는지 확인
        if "```sql" in ai_answer:
            sql = ai_answer.split("```sql")[1].split("```")[0].strip()
            
            with st.status("BigQuery 실행 중..."):
                try:
                    df = client_bq.query(sql).to_dataframe()
                    st.dataframe(df)
                    
                    # 데이터 기반 요약 요청
                    summary_prompt = f"조회된 데이터 샘플: {df.head(5).to_string()}\n\n이 데이터를 바탕으로 한글로 요약 답변해줘."
                    summary_res = client_ai.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[{"role": "user", "content": summary_prompt}]
                    )
                    final_text = summary_res.choices[0].message.content
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": f"{ai_answer}\n\n{final_text}"})
                except Exception as e:
                    st.error(f"SQL 에러 발생: {e}")
                    st.info("왼쪽 사이드바에서 정확한 컬럼명을 검색해 보세요.")
                    st.code(sql)
        else:
            # SQL이 없는 일반 대화
            st.markdown(ai_answer)
            st.session_state.messages.append({"role": "assistant", "content": ai_answer})