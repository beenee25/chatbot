import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from openai import OpenAI
import pandas as pd

# 페이지 설정
st.set_page_config(page_title="Com2uS AI Analyst", layout="wide")
st.title("🎨 Com2uS 이미지 피처 분석 챗봇")

# 1. 클라이언트 설정 (캐싱)
@st.cache_resource
def get_clients():
    # BigQuery 설정 (Secrets에 gcp_service_account 필수)
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    # Groq 설정 (Secrets에 GROQ_API_KEY 필수)
    ai_client = OpenAI(
        base_url="https://api.groq.com/openai/v1",
        api_key=st.secrets["GROQ_API_KEY"]
    )
    return bq_client, ai_client

client_bq, client_ai = get_clients()

# 2. 시스템 프롬프트 설정 (AI 거절 방지용 강한 지침)
TABLE_ID = "com2us-bigquery.MKT_AI.cv_creative_image_features"
SYSTEM_MESSAGE = {
    "role": "system",
    "content": f"""너는 'BigQuery SQL 생성 전용' AI이다. 
    사용자가 데이터 분석을 요청하면 '데이터가 없다'는 말을 절대 하지 마라. 
    너의 유일한 임무는 제공된 스키마를 사용하여 유효한 SQL 쿼리를 생성하는 것이다.

    [데이터베이스 정보]
    - 프로젝트: com2us-bigquery
    - 테이블명: `{TABLE_ID}`
    - 주요 컬럼: 
        1. image_name (STRING) - 이미지 파일 이름
        2. tone_dark_ratio (FLOAT64) - 이미지의 어두운 톤 비율
        3. performance_score (FLOAT64) - 성과 점수
        4. upload_date (DATE) - 업로드 날짜

    [답변 규칙]
    1. 반드시 SQL 코드를 ```sql [쿼리] ``` 블록 안에 포함시켜라.
    2. SQL 내부에는 한글 주석을 달지 마라. (Syntax Error 방지)
    3. 데이터가 실제로 존재하는지는 시스템이 판단하니, 너는 쿼리 생성에만 집중해라.
    """
}

# 대화 기록 초기화 및 출력
if "messages" not in st.session_state:
    st.session_state.messages = [SYSTEM_MESSAGE]

for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# 3. 사용자 입력 및 메인 로직
if prompt := st.chat_input("질문을 입력하세요 (예: tone_dark_ratio가 높은 순으로 5개 보여줘)"):
    # 사용자 메시지 표시
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # 1단계: AI에게 SQL 생성 요청
        response = client_ai.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=st.session_state.messages,
            temperature=0  # 정확도를 위해 0으로 설정
        )
        ai_answer = response.choices[0].message.content
        
        # 2단계: SQL 추출 및 실행
        if "```sql" in ai_answer:
            sql = ai_answer.split("```sql")[1].split("```")[0].strip()
            
            with st.status("BigQuery 분석 중..."):
                try:
                    # 실제 쿼리 실행
                    df = client_bq.query(sql).to_dataframe()
                    
                    if not df.empty:
                        st.dataframe(df) # 결과 표 출력
                        
                        # 3단계: 결과를 바탕으로 최종 요약
                        analysis_prompt = f"조회된 데이터 결과입니다:\n{df.head(10).to_string()}\n위 데이터를 바탕으로 질문에 대한 최종 답변을 한글로 작성해줘."
                        
                        # 요약을 위한 임시 메시지 구성
                        summary_res = client_ai.chat.completions.create(
                            model="llama-3.3-70b-versatile",
                            messages=[
                                SYSTEM_MESSAGE,
                                {"role": "user", "content": prompt},
                                {"role": "assistant", "content": ai_answer},
                                {"role": "user", "content": analysis_prompt}
                            ]
                        )
                        final_text = summary_res.choices[0].message.content
                        st.markdown("---")
                        st.markdown(final_text)
                        st.session_state.messages.append({"role": "assistant", "content": f"{ai_answer}\n\n{final_text}"})
                    else:
                        st.warning("쿼리 결과 데이터가 없습니다.")
                        st.session_state.messages.append({"role": "assistant", "content": ai_answer})
                        
                except Exception as e:
                    st.error(f"SQL 실행 중 오류 발생: {e}")
                    st.code(sql) # 에러 난 쿼리 확인용
        else:
            # SQL이 생성되지 않은 일반 답변인 경우
            st.markdown(ai_answer)
            st.session_state.messages.append({"role": "assistant", "content": ai_answer})