import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

# 1. 페이지 설정
st.set_page_config(page_title="MKT Performance AI Analyst", layout="wide")
st.title("🚀 마케팅 성과 상세 분석기 (ymdkst)")

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
    st.info("대상 테이블: `com2us-bigquery.MKT_AI.marketing_performance` (예시)")
    st.write("컬럼 구성: ymdkst, title, spend, click, conversion 등")

# 4. 시스템 프롬프트 (BigQuery 문법 및 ymdkst 처리 강화)
# TABLE_ID는 실제 환경에 맞춰 수정하세요.
TABLE_ID = "com2us-bigquery.MKT_AI.dummy_sales_data"

SYSTEM_PROMPT = f"""너는 BigQuery SQL 전문가이자 마케팅 데이터 분석가야.
[필수 SQL 규칙]
1. 테이블과 컬럼명에 절대 대괄호([])를 사용하지 마라. 대신 백틱(`)을 사용해라.
   - 잘못된 예: [project.dataset.table]
   - 올바른 예: `{TABLE_ID}`
2. SQL 내부에 한글 주석을 달지 마라.
3. 결과는 반드시 ```sql [코드] ``` 형식으로 출력해라.

[데이터 가이드]
- 테이블명: `{TABLE_ID}`
- 시간 컬럼: `ymdkst` (형식: YYYYMMDDHHMMSS)
- 시계열 분석 시 `PARSE_TIMESTAMP('%Y%m%d%H%M%S', ymdkst)`를 사용하여 시간을 처리해라.
"""

if "messages" not in st.session_state:
    st.session_state.messages = []

# 대화 기록 출력 (최근 대화 위주)
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 5. 메인 로직
if prompt := st.chat_input("질문을 입력하세요 (예: 최근 24시간 동안의 클릭 수 추이 보여줘)"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # 토큰 최적화: 최근 2개의 대화만 참조
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
                
                with st.status("BigQuery 데이터 분석 중..."):
                    # 2단계: 데이터 조회 (Storage API 권한 에러 방지)
                    df = client_bq.query(sql).result().to_dataframe(create_bqstorage_client=False)
                
                if not df.empty:
                    st.subheader("📈 시각화 분석")
                    
                    # 시계열 처리 로직
                    try:
                        # ymdkst 또는 시간 관련 컬럼 자동 감지 및 변환
                        time_cols = [c for c in df.columns if 'time' in c.lower() or 'ymdkst' in c.lower()]
                        if time_cols:
                            t_col = time_cols[0]
                            # ymdkst 문자열 형식을 datetime으로 변환 시도
                            df[t_col] = pd.to_datetime(df[t_col], format='%Y%m%d%H%M%S', errors='coerce').fillna(pd.to_datetime(df[t_col], errors='coerce'))
                            df = df.dropna(subset=[t_col]).sort_values(t_col)
                            st.line_chart(df.set_index(t_col).select_dtypes(include=['number']))
                        elif len(df.columns) >= 2:
                            st.bar_chart(data=df, x=df.columns[0], y=df.columns[1:])
                    except Exception as e:
                        st.info("기본 그래프 생성이 불가능한 데이터 구조입니다. 아래 표를 확인하세요.")

                    # 표 출력
                    st.subheader("📄 상세 데이터")
                    st.dataframe(df, use_container_width=True)

                    # 3단계: 요약 답변
                    summary_res = client_ai.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[{"role": "user", "content": f"이 데이터 요약해줘: {df.head(5).to_string()}"}]
                    )
                    final_text = summary_res.choices[0].message.content
                    st.markdown("---")
                    st.markdown(final_text)
                    st.session_state.messages.append({"role": "assistant", "content": final_text})
                else:
                    st.warning("조회된 결과 데이터가 없습니다.")
            else:
                st.markdown(ai_answer)
                st.session_state.messages.append({"role": "assistant", "content": ai_answer})

        except Exception as e:
            st.error(f"분석 오류 발생: {e}")
            if "sql" in locals():
                st.code(sql, language="sql")