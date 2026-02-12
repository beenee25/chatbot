import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

st.set_page_config(page_title="Com2uS Data Analyst", layout="wide")
st.title("📊 매출 데이터 통합 분석기")

@st.cache_resource
def get_clients():
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    ai_client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=st.secrets["GROQ_API_KEY"])
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# --- 사이드바 ---
with st.sidebar:
    if st.button("🔄 대화 초기화"):
        st.session_state.messages = []
        st.rerun()
    st.info("대상: `dummy_sales_data` (date, title, sales, pu)")

# --- 시스템 프롬프트 (시각화 최적화 쿼리 유도) ---
SYSTEM_PROMPT = """너는 BigQuery 전문가야. 
- 테이블: `com2us-bigquery.MKT_AI.dummy_sales_data`
- 사용자가 추이나 비교를 물어보면 반드시 X축으로 쓸 컬럼(date 혹은 title)과 Y축으로 쓸 수치 컬럼(sales, pu)을 함께 조회해라.
- SQL만 생성하고 한글 주석은 달지 마라."""

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- 메인 로직 ---
if prompt := st.chat_input("질문을 입력하세요"):
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
                    df = client_bq.query(sql).result().to_dataframe(create_bqstorage_client=False)
                
                if not df.empty:
                    st.subheader("📈 분석 결과 시각화")
                    
                    # 1. 시각화 시도
                    try:
                        # 날짜 컬럼이 있으면 시계열로 변환
                        date_cols = [c for c in df.columns if 'date' in c.lower() or 'dt' in c.lower()]
                        if date_cols:
                            df[date_cols[0]] = pd.to_datetime(df[date_cols[0]])
                            chart_df = df.set_index(date_cols[0])
                            st.line_chart(chart_df)
                        # 문자열(title)과 숫자 컬럼이 있으면 바 차트
                        elif len(df.columns) >= 2:
                            st.bar_chart(data=df, x=df.columns[0], y=df.columns[1:])
                    except Exception:
                        st.info("데이터 특성상 그래프 생성이 건너뛰어졌습니다.")

                    # 2. 데이터 표 무조건 출력 (그래프 바로 아래)
                    st.subheader("📄 상세 데이터 리스트")
                    st.dataframe(df, use_container_width=True)

                    # 3. 요약 답변
                    summary_res = client_ai.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[{"role": "user", "content": f"이 데이터 결과({df.head(5).to_string()})를 바탕으로 답변 요약해줘."}]
                    )
                    st.markdown("---")
                    st.markdown(summary_res.choices[0].message.content)
                    st.session_state.messages.append({"role": "assistant", "content": summary_res.choices[0].message.content})
            else:
                st.markdown(ai_answer)
                st.session_state.messages.append({"role": "assistant", "content": ai_answer})

        except Exception as e:
            st.error(f"오류 발생: {e}")