import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

# 1. 페이지 설정
st.set_page_config(page_title="MKT Performance AI Analyst", layout="wide")
st.title("🚀 마케팅 성과 상세 분석기")

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
    st.write("💡 팁: 컬럼명 뒤에 '0'이 붙는 경우가 많으니 확인 후 질문해 주세요.")

# 4. 시스템 프롬프트 (정확한 컬럼 매핑 추가)
TABLE_ID = "com2us-bigquery.MKT_AI.marketing_performance"

SYSTEM_PROMPT = f"""너는 BigQuery 전문가이자 마케팅 분석가야.
[필수 SQL 규칙]
1. 테이블명: `{TABLE_ID}`
2. **중요 컬럼 매핑**:
   - 매출/수익(Revenue)은 revenue0, revenue7, revenue14 등이 있으며 cohort_date로부터 n일차의 누적된 매출이다.
   - ymdkst는 cohort_date이고, revenue은 해당 날짜의 매출을 의미해.
   - 비용(Spend/Cost)은 `cost_cohort` 컬럼을 사용해라.
   - 시간 데이터는 `ymdkst` (DATE 타입)를 사용해라.
   - 캠페인은 campaign 컬럼을 사용하라.
3. 절대 대괄호([])를 쓰지 말고 백틱(`)을 사용해라.
4. SQL 내부에 한글 주석을 달지 마라.
5. 결과는 반드시 ```sql [코드] ``` 형식으로 출력해라.
6. 모든 응답의 언어는 기본적으로 한국어를 사용하며, 고유 명사의 경우 영어를 허용한다.
7. 절대 한자(漢字)를 섞어서 사용하지 마라.
8. 숫자는 지수로 아래와 같이 표기하며, 소숫점은 버린다.
  - 1,000 = 1천,
  - 10,000 = 1만
  - 150,000,000 = 1.5억
9. **나눗셈 오류 방지**: ROAS, CTR 등 모든 나눗셈 연산 시 반드시 `SAFE_DIVIDE(분자, 분모)` 함수를 사용해라. 절대 `/` 기호를 직접 쓰지 마라.
   - 예: `SAFE_DIVIDE(SUM(revenue0), SUM(spend0))`
"""

if "messages" not in st.session_state:
    st.session_state.messages = []

# 대화 기록 출력
for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# 5. 메인 로직
if prompt := st.chat_input("질문을 입력하세요 (예: 날짜별 revenue0 추이를 그래프로 보여줘)"):
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
                    query_job = client_bq.query(sql)
                    df = query_job.result().to_dataframe(create_bqstorage_client=False)
                
                if not df.empty:
                    st.subheader("📈 시각화 분석")
                    
                    try:
                        # 시계열 감지 및 그래프 생성
                        time_cols = [c for c in df.columns if any(k in c.lower() for k in ['ymdkst', 'time', 'date', 'dt'])]
                        if time_cols:
                            t_col = time_cols[0]
                            df[t_col] = pd.to_datetime(df[t_col], errors='coerce')
                            df = df.dropna(subset=[t_col]).sort_values(t_col)
                            st.line_chart(df.set_index(t_col).select_dtypes(include=['number']))
                        elif len(df.columns) >= 2:
                            st.bar_chart(data=df, x=df.columns[0], y=df.columns[1:])
                    except Exception:
                        st.info("데이터 구조상 자동 그래프 생성이 어렵습니다. 표 데이터를 확인해 주세요.")

                    # 상세 데이터 표 출력
                    st.subheader("📄 상세 데이터")
                    st.dataframe(df, use_container_width=True)

                    # 3단계: AI 요약
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