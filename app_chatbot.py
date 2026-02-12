import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

st.set_page_config(page_title="MKT Performance Analyst", layout="wide")
st.title("🚀 마케팅 성과 상세 분석기 (ymdkst)")

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
    st.info("대상 테이블: `com2us-bigquery.MKT_AI.marketing_performance` (예시)")
    st.write("핵심 컬럼: ymdkst (시간), title, spend, click, conversion")

# --- 시스템 프롬프트 (ymdkst 분석 최적화) ---
SYSTEM_PROMPT = """너는 마케팅 데이터 분석 전문가야. 
- 테이블 명은 사용자가 지정한 테이블을 사용하되, 시간 컬럼은 'ymdkst'이다.
- 'ymdkst'는 'YYYYMMDDHHMMSS' 형식이거나 TIMESTAMP일 수 있으니 이를 고려하여 SQL을 짜라.
- 시간대별 추이를 물어보면 ymdkst를 기준으로 그룹화하여 성과(CTR, ROAS 등)를 계산해라.
- 반드시 ```sql [코드] ``` 형식을 사용하고 한글 주석은 달지 마라."""

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- 메인 로직 ---
if prompt := st.chat_input("질문을 입력하세요 (예: 시간대별 클릭률 추이 보여줘)"):
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
                
                with st.status("마케팅 데이터 분석 중..."):
                    df = client_bq.query(sql).result().to_dataframe(create_bqstorage_client=False)
                
                if not df.empty:
                    st.subheader("📊 성과 분석 시각화")
                    
                    # 1. 시계열 처리 (ymdkst 자동 감지)
                    time_col = 'ymdkst' if 'ymdkst' in df.columns else None
                    if not time_col: # ymdkst라는 이름이 없으면 첫 번째 컬럼 시도
                        time_col = df.columns[0]
                    
                    try:
                        # ymdkst가 숫자/문자열인 경우를 대비해 datetime 변환
                        df[time_col] = pd.to_datetime(df[time_col], format='%Y%m%d%H%M%S', errors='coerce')
                        df = df.dropna(subset=[time_col]).sort_values(time_col)
                        
                        chart_df = df.set_index(time_col)
                        st.line_chart(chart_df.select_dtypes(include=['number']))
                    except:
                        st.info("데이터를 그래프로 표시하기 위해 변환 중입니다. 아래 표를 참조하세요.")
                    
                    # 2. 데이터 표 출력
                    st.subheader("📄 상세 성과 데이터")
                    st.dataframe(df, use_container_width=True)

                    # 3. 데이터 기반 요약
                    summary_res = client_ai.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[{"role": "user", "content": f"마케팅 성과 데이터 요약: {df.head(5).to_string()}"}]
                    )
                    st.markdown("---")
                    st.markdown(summary_res.choices[0].message.content)
                    st.session_state.messages.append({"role": "assistant", "content": summary_res.choices[0].message.content})
            else:
                st.markdown(ai_answer)
                st.session_state.messages.append({"role": "assistant", "content": ai_answer})

        except Exception as e:
            st.error(f"분석 오류: {e}")