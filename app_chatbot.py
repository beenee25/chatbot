import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

st.set_page_config(page_title="BigQuery Analyst", layout="wide")
st.title("📊 매출 분석 AI 챗봇 (그래프 + 표)")

@st.cache_resource
def get_clients():
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    ai_client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=st.secrets["GROQ_API_KEY"])
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# --- 사이드바 ---
with st.sidebar:
    if st.button("대화 기록 초기화"):
        st.session_state.messages = []
        st.rerun()
    st.info("대상: `dummy_sales_data` (date, title, sales, pu)")

# --- 시스템 프롬프트 ---
SYSTEM_PROMPT = "너는 BigQuery 전문가야. 테이블: `com2us-bigquery.MKT_AI.dummy_sales_data`. 반드시 ```sql [코드] ``` 형식으로 SQL만 생성해."

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- 메인 로직 ---
if prompt := st.chat_input("질문하세요"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        input_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + st.session_state.messages[-2:]

        try:
            response = client_ai.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=input_messages,
                temperature=0
            )
            ai_answer = response.choices[0].message.content
            
            if "```sql" in ai_answer:
                sql = ai_answer.split("```sql")[1].split("```")[0].strip()
                
                with st.status("데이터 분석 중..."):
                    query_job = client_bq.query(sql)
                    df = query_job.result().to_dataframe(create_bqstorage_client=False)
                
                if not df.empty:
                    # 탭 생성: 그래프와 표를 동시에 혹은 선택해서 볼 수 있게 함
                    tab1, tab2 = st.tabs(["📈 시각화 그래프", "📄 데이터 표"])
                    
                    with tab1:
                        # 1. 시계열 선 그래프 (date 컬럼이 있을 때)
                        if 'date' in df.columns:
                            df['date'] = pd.to_datetime(df['date'])
                            st.line_chart(df.set_index('date')[['sales' if 'sales' in df.columns else df.columns[1]]])
                        # 2. 범주형 바 차트 (title 컬럼이 있을 때)
                        elif 'title' in df.columns:
                            st.bar_chart(data=df, x='title', y=df.columns[1])
                        else:
                            st.write("그래프를 그릴 수 있는 형태의 데이터가 아닙니다. 표 탭을 확인하세요.")

                    with tab2:
                        # 표는 어떤 조건에서도 무조건 출력
                        st.dataframe(df, use_container_width=True)

                    # 결과 요약
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