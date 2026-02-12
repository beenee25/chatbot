import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

st.set_page_config(page_title="Com2uS AI Analyst", layout="wide")
st.title("🎨 이미지 피처 분석 챗봇")

# 1. 클라이언트 설정
@st.cache_resource
def get_clients():
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    ai_client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=st.secrets["GROQ_API_KEY"])
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# 2. 실제 테이블 스키마 정의 (정확한 분석의 핵심!)
TABLE_ID = "com2us-bigquery.MKT_AI.cv_creative_image_features"
TABLE_SCHEMA = f"""
- 대상 테이블: `{TABLE_ID}`
- 주요 컬럼:
    * image_name (STRING): 이미지 파일명
    * tone_dark_ratio (FLOAT64): 이미지의 어두운 톤 비율 (0~1 사이)
    * performance_score (FLOAT64): 이미지 성과 점수
    * upload_date (DATE): 업로드 날짜
    * (기타 이미지 특징 관련 컬럼들이 포함되어 있음)
"""

# 시스템 메시지: 거절 방지 및 SQL 규칙 강화
if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "system", 
        "content": f"""너는 BigQuery SQL 전문가야. 
        사용자가 데이터에 대해 물어보면 '데이터가 없다'는 말을 절대 하지 마. 
        너의 역할은 제공된 스키마를 바탕으로 BigQuery SQL 코드를 생성하는 것이며, 
        실제 실행은 시스템이 담당한다.

        [규칙]
        1. 결과에 반드시 ```sql [쿼리] ``` 형식의 코드 블록을 포함할 것.
        2. SQL 내부에는 한글 주석을 절대 달지 말 것.
        3. 테이블명은 반드시 `{TABLE_ID}`를 사용할 것.
        4. 스키마 정보: {TABLE_SCHEMA}"""
    }]

# 채팅 기록 출력
for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# 3. 사용자 입력 처리
if prompt := st.chat_input("예: tone_dark_ratio가 높은 순으로 5개 이미지 알려줘"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # 1단계: AI에게 SQL 생성 요청
        response = client_ai.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=st.session_state.messages
        )
        ai_answer = response.choices[0].message.content
        
        # 2단계: SQL 추출 및 BigQuery 실행
        if "```sql" in ai_answer:
            sql = ai_answer.split("```sql")[1].split("```")[0].strip()
            
            with st.status("BigQuery에서 데이터를 가져오는 중..."):
                try:
                    df = client_bq.query(sql).to_dataframe()
                    st.dataframe(df) # 결과 표 출력
                    
                    # 3단계: 결과 요약 요청
                    analysis_prompt = f"위 데이터 결과({df.head(10).to_string()})를 바탕으로 사용자의 질문에 답변해줘."
                    temp_messages = st.session_state.messages + [{"role": "assistant", "content": ai_answer}, {"role": "user", "content": analysis_prompt}]
                    
                    final_res = client_ai.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=temp_messages
                    )
                    final_text = final_res.choices[0].message.content
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                    
                except Exception as e:
                    st.error(f"SQL 실행 에러: {e}")
                    st.code(sql)
        else:
            st.markdown(ai_answer)
            st.session_state.messages.append({"role": "assistant", "content": ai_answer})