from __future__ import annotations

from mcp.server.fastmcp import FastMCP
from app.engineering_agent.tools import aws_tools, repository_tools, test_tools

mcp = FastMCP("Streaming Lakehouse Engineering Tools")

mcp.tool()(repository_tools.list_repository_files)
mcp.tool()(repository_tools.read_repository_file)
mcp.tool()(repository_tools.search_repository)
mcp.tool()(repository_tools.analyse_python_file)
mcp.tool()(test_tools.propose_generated_test)
mcp.tool()(test_tools.validate_generated_test)
mcp.tool()(test_tools.write_generated_test)
mcp.tool()(test_tools.run_pytest)
mcp.tool()(aws_tools.get_glue_job_run)
mcp.tool()(aws_tools.get_control_item)
mcp.tool()(aws_tools.read_s3_text)
