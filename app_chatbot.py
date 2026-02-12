import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

st.set_page_config(page_title="BigQuery AI Analyst", layout="wide")
st.title("지능형 데이터 분석 챗봇 🤖📊")

# 1. 클라이언트 설정
@st.cache_resource
def get_clients():
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    ai_client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=st.secrets["GROQ_API_KEY"])
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# 2. 분석할 테이블 정보 (AI가 쿼리를 짤 수 있게 가이드를 줍니다)
TABLE_SCHEMA = """
Table Name: `com2us-bigquery.MKT_AI.cv_creative_image_features`
Columns:
- image_name: 이미지 고유 ID
- tone_dark_ratio: 어두운 톤 비율 (0~1 사이 수치))
"""

if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system", "content": f"너는 BigQuery 전문가야. 다음 테이블 스키마를 참고해서 사용자의 질문을 SQL로 변환하고 분석해줘. SQL을 작성할 때는 반드시 마크다운 코드 블록(```sql ... ```)을 사용해. \n\n스키마 정보: {TABLE_SCHEMA}"}]

# 대화 출력 (시스템 메시지 제외)
for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# 3. 사용자 입력 및 처리
if prompt := st.chat_input("데이터에 대해 궁금한 점을 물어보세요!"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # STEP 1: AI에게 SQL 생성을 요청
        response = client_ai.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=st.session_state.messages
        )
        ai_answer = response.choices[0].message.content
        
        # STEP 2: 생성된 답변에서 SQL 추출 및 실행
        if "```sql" in ai_answer:
            sql = ai_answer.split("```sql")[1].split("```")[0].strip()
            
            with st.status("BigQuery 쿼리 실행 중..."):
                try:
                    df = client_bq.query(sql).to_dataframe()
                    st.dataframe(df) # 데이터 결과 표로 보여주기
                    
                    # STEP 3: 데이터를 바탕으로 최종 해석 요청
                    analysis_prompt = f"위 데이터 결과({df.to_string(index=False)})를 바탕으로 질문에 대한 최종 결론을 한글로 요약해줘."
                    st.session_state.messages.append({"role": "assistant", "content": ai_answer})
                    st.session_state.messages.append({"role": "user", "content": analysis_prompt})
                    
                    final_res = client_ai.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=st.session_state.messages
                    )
                    final_text = final_res.choices[0].message.content
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                    
                except Exception as e:
                    st.error(f"SQL 실행 중 오류가 발생했습니다: {e}")
                    st.code(sql)
        else:
            # SQL이 필요 없는 일반 대화인 경우
            st.markdown(ai_answer)
            st.session_state.messages.append({"role": "assistant", "content": ai_answer})