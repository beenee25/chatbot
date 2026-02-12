import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

# 1. 페이지 설정
st.set_page_config(page_title="BigQuery Analyst with Charts", layout="wide")
st.title("📊 매출 분석 AI 챗봇 (그래프 모드)")

# 2. 클라이언트 설정 (캐싱)
@st.cache_resource
def get_clients():
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    ai_client = OpenAI(
        base_url="https://api.groq.com/openai/v1",
        api_key=st.secrets["GROQ_API_KEY"]
    )
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# 3. 사이드바 관리
with st.sidebar:
    st.header("⚙️ 관리 도구")
    if st.button("대화 기록 초기화"):
        st.session_state.messages = []
        st.rerun()
    st.info("대상 테이블: `dummy_sales_data` (date, title, sales, pu)")

# 4. 시스템 프롬프트 (그래프 지침 추가)
SYSTEM_PROMPT = """너는 BigQuery 전문가야.
1. 테이블: `com2us-bigquery.MKT_AI.dummy_sales_data`
2. SQL 생성 시 반드시 ```sql [코드] ``` 형식을 지켜라.
3. 사용자가 추이나 변화를 물어보면 반드시 date 컬럼을 포함하여 쿼리를 짜라.
"""

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 5. 메인 로직
if prompt := st.chat_input("질문하세요 (예: 2025년 월별 매출 그래프 그려줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # 토큰 최적화 호출
        input_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + st.session_state.messages[-2:]

        try:
            # 1단계: SQL 생성
            response = client_ai.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=input_messages,
                temperature=0
            )
            ai_answer = response.choices[0].message.content
            
            if "```sql" in ai_answer:
                sql = ai_answer.split("```sql")[1].split("```")[0].strip()
                
                with st.status("데이터 분석 및 시각화 중..."):
                    query_job = client_bq.query(sql)
                    df = query_job.result().to_dataframe(create_bqstorage_client=False)
                    
                    if not df.empty:
                        # --- 그래프 자동 출력 로직 ---
                        # 1. 시계열 데이터(date)가 포함된 경우 선 그래프
                        if 'date' in df.columns:
                            df['date'] = pd.to_datetime(df['date'])
                            df_chart = df.set_index('date')
                            st.line_chart(df_chart[['sales']] if 'sales' in df.columns else df_chart)
                        
                        # 2. 카테고리(title)별 데이터인 경우 바 차트
                        elif 'title' in df.columns and 'sales' in df.columns:
                            st.bar_chart(data=df, x='title', y='sales')
                        
                        # 표도 함께 출력
                        st.dataframe(df, use_container_width=True)
                        # ----------------------------

                        # 3단계: 결과 요약
                        summary_res = client_ai.chat.completions.create(
                            model="llama-3.3-70b-versatile",
                            messages=[{"role": "user", "content": f"이 데이터 요약해줘: {df.head(5).to_string()}"}]
                        )
                        final_text = summary_res.choices[0].message.content
                        st.markdown(final_text)
                        st.session_state.messages.append({"role": "assistant", "content": final_text})
            else:
                st.markdown(ai_answer)
                st.session_state.messages.append({"role": "assistant", "content": ai_answer})

        except Exception as e:
            st.error(f"오류 발생: {e}")