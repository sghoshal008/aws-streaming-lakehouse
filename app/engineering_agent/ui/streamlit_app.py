from __future__ import annotations

import os
import uuid

import requests
import streamlit as st

API = os.getenv("AGENT_API_BASE_URL", "http://localhost:8000").rstrip("/")

st.set_page_config(page_title="Engineering Copilot", page_icon="🛠️", layout="wide")
st.title("Streaming Lakehouse Engineering Copilot")
st.caption("OpenAI ReAct agent · FastAPI · MCP tools · human-approved pytest generation")

if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []
if "proposal" not in st.session_state:
    st.session_state.proposal = None

with st.sidebar:
    selected_file = st.text_input(
        "Selected repository file",
        "app/glue/bronze-to-silver/yt_sales_bronze_to_silver.py",
    )
    st.caption(f"Thread: {st.session_state.thread_id[:8]}")
    if st.button("New conversation"):
        st.session_state.thread_id = str(uuid.uuid4())
        st.session_state.messages = []
        st.session_state.proposal = None
        st.rerun()
    if st.button("Check backend"):
        try:
            response = requests.get(f"{API}/health", timeout=10)
            response.raise_for_status()
            st.json(response.json())
        except requests.RequestException as exc:
            st.error(str(exc))

for item in st.session_state.messages:
    with st.chat_message(item["role"]):
        st.markdown(item["content"])
        if item.get("tool_trace"):
            st.caption("Tools used: " + " → ".join(item["tool_trace"]))

prompt = st.chat_input("Ask the agent to review code, generate tests, run tests, or inspect read-only AWS")
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Agent is choosing engineering tools..."):
            try:
                response = requests.post(
                    f"{API}/api/agent/invoke",
                    json={
                        "message": prompt,
                        "thread_id": st.session_state.thread_id,
                        "selected_file": selected_file or None,
                        "mode": "auto",
                    },
                    timeout=240,
                )
                response.raise_for_status()
                data = response.json()
                st.markdown(data["answer"])
                tool_trace = data.get("tool_trace", [])
                if tool_trace:
                    st.caption("Tools used: " + " → ".join(tool_trace))
                st.session_state.messages.append(
                    {"role": "assistant", "content": data["answer"], "tool_trace": tool_trace}
                )
                if data.get("proposed_filename") and data.get("proposed_content"):
                    st.session_state.proposal = {
                        "filename": data["proposed_filename"],
                        "content": data["proposed_content"],
                        "test_plan": data.get("test_plan", []),
                    }
            except requests.RequestException as exc:
                detail = exc.response.text if getattr(exc, "response", None) is not None else str(exc)
                st.error(detail)

proposal = st.session_state.proposal
if proposal:
    st.divider()
    st.subheader("Generated pytest proposal")
    if proposal.get("test_plan"):
        st.json(proposal["test_plan"])
    st.code(proposal["content"], language="python")
    approve_col, reject_col = st.columns(2)
    with approve_col:
        if st.button("Approve and write test", type="primary"):
            try:
                response = requests.post(
                    f"{API}/api/generated-tests/approve",
                    json={
                        "thread_id": st.session_state.thread_id,
                        "filename": proposal["filename"],
                        "content": proposal["content"],
                        "approved": True,
                    },
                    timeout=30,
                )
                response.raise_for_status()
                st.success(f"Written: {response.json()['path']}")
                st.session_state.proposal = None
            except requests.RequestException as exc:
                st.error(exc.response.text if exc.response is not None else str(exc))
    with reject_col:
        if st.button("Reject proposal"):
            st.session_state.proposal = None
            st.info("Proposal discarded.")
