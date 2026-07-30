# Engineering Copilot tools

## ReAct tools

- `review_glue_or_spark_job`: reviews one selected Python/Glue file.
- `propose_pytest_tests`: generates executable lightweight pytest contract tests.
- `run_engineering_copilot_tests`: runs the Copilot's own tests.
- `run_generated_tests`: runs approved tests under `generated_tests/`.
- `explain_pipeline_architecture`: reads repository documentation.
- `find_deduplication_logic`: finds deduplication/window code.
- repository helpers: list, read, search and AST analysis.
- optional AWS helpers: Glue job run, DynamoDB item and bounded S3 text read.

## MCP tools

The MCP server exposes repository inspection, test proposal, validation, approval-gated writing,
pytest execution and optional read-only AWS investigation.
