import streamlit as st
from openai import OpenAI

st.title("Groq 기반 초고속 챗봇 ⚡")

# 1. API 키 및 Base URL 설정
# Streamlit Secrets에 GROQ_API_KEY라는 이름으로 키를 저장하세요.
client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=st.secrets["GROQ_API_KEY"]
)

# 2. 대화 기록 초기화
if "messages" not in st.session_state:
    st.session_state.messages = []

# 3. 저장된 대화 기록 출력
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 4. 사용자 입력 처리
if prompt := st.chat_input("메시지를 입력하세요"):
    # 사용자 메시지 표시 및 저장
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 5. AI 응답 생성 및 표시
    with st.chat_message("assistant"):
        # Groq의 Llama 3 모델 사용
        stream = client.chat.completions.create(
            model="llama-3.3-70b-versatile", 
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        )
        response = st.write_stream(stream)
    
    # AI 응답 저장
    st.session_state.messages.append({"role": "assistant", "content": response})