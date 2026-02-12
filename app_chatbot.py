import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

# 1. 페이지 설정
st.set_page_config(page_title="MKT Performance AI", layout="wide")
st.title("🚀 마케팅 성과 분석기 (DATE 타입 최적화)")

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
    if st.button("🔄 대화 기록 초기화"):
        st.session_state.messages = []
        st.rerun()
    st.info("대상 테이블: `com2us-bigquery.MKT_AI.marketing_performance`")
    st.write("참고: `ymdkst` 컬럼은 이미 DATE/TIMESTAMP 형식이므로 별도 변환이 필요 없습니다.")

# 4. 시스템 프롬프트 (DATE 타입 대응 및 문법 고정)
TABLE_ID = "com2us-bigquery.MKT_AI.marketing_performance"

SYSTEM_PROMPT = f"""너는 BigQuery 전문가야.
[필수 규칙]
1. 테이블명: `{TABLE_ID}`
2. 컬럼명: `ymdkst` (이 컬럼은 이미 DATE 혹은 TIMESTAMP 타입이다).
3. **중요**: `ymdkst`에 `PARSE_TIMESTAMP` 함수를 절대 사용하지 마라. 이미 날짜 형식이므로 그대로 사용하거나 필요한 경우 `CAST(ymdkst AS TIMESTAMP)`만 사용해라.
4. 테이블/컬럼명을 감쌀 때 절대 대괄호([])를 쓰지 말고 백틱(`)을 사용해라.
5. SQL 내부에 한글 주석을 달지 마라.
6. 결과는 반드시 ```sql [코드] ``` 형식으로 출력해라.
"""

if "messages" not in st.session_state:
    st.session_state.messages = []

# 대화 기록 출력
for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# 5. 메인 로직
if prompt := st.chat_input("질문을 입력하세요 (예: 날짜별 spend와 click 추이를 그래프로 보여줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # 토큰 최적화: 최근 2개의 메시지만 참조
        input_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + st.session_state.messages[-2:]

        try:
            # 1단계: AI에게 SQL 생성 요청
            response = client_ai.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=input_messages,
                temperature=0
            )
            ai_answer = response.choices[0].message.content
            
            if "```sql" in ai_answer:
                sql = ai_answer.split("```sql")[1].split("```")[0].strip()
                
                with st.status("BigQuery 분석 중..."):
                    # 2단계: 데이터 조회 (Storage API 미사용 옵션으로 권한 에러 방지)
                    query_job = client_bq.query(sql)
                    df = query_job.result().to_dataframe(create_bqstorage_client=False)
                
                if not df.empty:
                    st.subheader("📈 시각화 분석")
                    
                    # 시계열 그래프 로직 (날짜 형식 유연하게 처리)
                    try:
                        # ymdkst 또는 시간 관련 컬럼 자동 감지
                        time_cols = [c for c in df.columns if any(k in c.lower() for k in ['ymdkst', 'time', 'date', 'dt'])]
                        if time_cols:
                            t_col = time_cols[0]
                            df[t_col] = pd.to_datetime(df[t_col], errors='coerce')
                            df = df.dropna(subset=[t_col]).sort_values(t_col)
                            # 숫자형 데이터만 그래프로 표시
                            st.line_chart(df.set_index(t_col).select_dtypes(include=['number']))
                        elif len(df.columns) >= 2:
                            st.bar_chart(data=df, x=df.columns[0], y=df.columns[1:])
                    except Exception as chart_err:
                        st.info("데이터 구조상 자동 그래프 생성이 어렵습니다. 표 데이터를 확인해 주세요.")

                    # 상세 데이터 표 출력
                    st.subheader("📄 상세 데이터")
                    st.dataframe(df, use_container_width=True)

                    # 3단계: AI 요약
                    # 토큰 절약을 위해 head(5)만 전달
                    summary_res = client_ai.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[{"role": "user", "content": f"데이터 결과 요약: {df.head(5).to_string()}"}]
                    )
                    final_text = summary_res.choices[0].message.content
                    st.markdown("---")
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                else:
                    st.warning("조회된 데이터가 없습니다.")
            else:
                st.markdown(ai_answer)
                st.session_state.messages.append({"role": "assistant", "content": ai_answer})

        except Exception as e:
            st.error(f"오류가 발생했습니다: {e}")
            if "sql" in locals():
                st.code(sql, language="sql")