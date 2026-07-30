# Engineering Copilot: simple ReAct + FastAPI + MCP

This is a small follow-up demo built around the existing streaming-lakehouse repository.

## Flow

```text
Streamlit UI
    -> FastAPI
    -> OpenAI ReAct agent
    -> engineering tools
       - repository analysis
       - pytest generation
       - pytest execution
       - optional read-only AWS checks

The same engineering tool layer is also exposed through an MCP server at /mcp.
```

## What the generated tests do

The generated tests are lightweight repository-contract tests. They validate that a selected
Glue/Python job:

- remains valid Python;
- still contains the Spark operations discovered during analysis;
- still declares the discovered top-level functions;
- has an explicit Glue/Spark runtime boundary.

They do not start Spark, so Java and PySpark are not required. This keeps the showcase easy to
clone and run. Full DataFrame behaviour tests can be added later after transformation logic is
extracted from the Glue entry point.

## Human approval

The ReAct agent may propose a pytest file, but it cannot write production files. The Streamlit UI
shows the proposal and requires an explicit **Approve and write test** action. Approved files are
written only under `generated_tests/`.

## Setup

```bash
python3 -m venv .agent-venv
source .agent-venv/bin/activate
python -m pip install -r requirements-agent.txt
cp .env.agent.example .env.agent
```

Add your OpenAI API key to `.env.agent`, then load it:

```bash
set -a
source .env.agent
set +a
```

Start FastAPI:

```bash
uvicorn app.engineering_agent.api.main:app --reload --port 8000
```

Start Streamlit in another terminal:

```bash
streamlit run app/engineering_agent/ui/streamlit_app.py --server.port 8501
```

Open `http://localhost:8501`.

## Demo prompts

```text
Review the selected Glue job and explain the main engineering risks.
```

```text
Generate executable pytest tests for the selected file.
```

Approve the proposal in the UI, then ask:

```text
Run the generated tests and report passed, failed, skipped and errors exactly.
```

```text
Explain the repository architecture.
```

Optional AWS example:

```text
Get Glue job run <run-id> for job <job-name>.
```

AWS tools are read-only and remain disabled until `AGENT_ENABLE_AWS=true`.

## MCP

The FastMCP application is mounted under:

```text
http://localhost:8000/mcp
```

It exposes repository analysis, test proposal, approval-gated test writing, pytest execution and
optional read-only AWS tools.
