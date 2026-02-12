import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

# 1. 페이지 설정
st.set_page_config(page_title="BigQuery Dummy Analyst", layout="wide")
st.title("📊 매출 데이터 분석 AI 챗봇")

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
    st.info("대상 테이블: `com2us-bigquery.MKT_AI.dummy_sales_data`")
    st.write("컬럼: date, title, sales, pu")

# 4. 시스템 프롬프트 (토큰 다이어트 버전)
# AI가 딴소리하지 않고 SQL만 짜도록 강력하게 지시
SYSTEM_PROMPT = """너는 BigQuery 전문가야. 다음 규칙을 지켜:
1. 테이블: `com2us-bigquery.MKT_AI.dummy_sales_data`
2. 컬럼: date(날짜), title(제목), sales(매출), pu(유료사용자)
3. 반드시 ```sql [코드] ``` 형식으로만 SQL을 생성해라.
4. SQL 내부에 한글 주석 금지.
5. 데이터가 없다는 핑계 대지 말고 쿼리를 짜라.
"""

if "messages" not in st.session_state:
    st.session_state.messages = []

# 대화 출력
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 5. 메인 로직
if prompt := st.chat_input("예: 2025년 전체 매출 합계 알려줘"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # [토큰 최적화] 최근 대화 2개만 참조하여 토큰 초과 방지
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
                
                with st.status("BigQuery 실행 중..."):
                    # 2단계: 데이터 조회 (403 권한 에러 방지 옵션 적용)
                    query_job = client_bq.query(sql)
                    df = query_job.result().to_dataframe(create_bqstorage_client=False)
                    st.dataframe(df)

                if not df.empty:
                    # 3단계: 결과 요약 (토큰 절약을 위해 최소 데이터만 전달)
                    summary_prompt = f"이 데이터 결과({df.head(5).to_string()})를 바탕으로 질문에 답해줘."
                    summary_res = client_ai.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[{"role": "user", "content": summary_prompt}]
                    )
                    final_text = summary_res.choices[0].message.content
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": f"SQL 결과입니다.\n{final_text}"})
                else:
                    st.warning("조회된 데이터가 없습니다.")
            else:
                st.markdown(ai_answer)
                st.session_state.messages.append({"role": "assistant", "content": ai_answer})

        except Exception as e:
            st.error(f"오류가 발생했습니다: {e}")
            if "rate_limit" in str(e).lower():
                st.info("잠시 후 다시 시도해 주세요. (무료 티어 토큰 제한)")