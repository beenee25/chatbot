import streamlit as st
from openai import OpenAI

st.title("나의 AI 챗봇 🤖")

# 1. API 키 설정 (Streamlit Secrets에서 불러오기)
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

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
        # 스트리밍 효과 구현
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        )
        response = st.write_stream(stream)
    
    # AI 응답 저장
    st.session_state.messages.append({"role": "assistant", "content": response})