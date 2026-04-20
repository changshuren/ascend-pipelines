Yes — I can help you create the paths and show the contents for these shared folders/files.

Because there are many files, I’ll organize them by folder and start with the most important ones we already pulled:

automations/
connections/
macros/
src/
profiles/
otto/ starter files
1. automations/
Create folder
Cmd
1 lines

mkdir C:\Users\ISBE\ascend-pipelines\automations
File
Text
1 lines

automations/README.md
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\automations\README.md
Contents
Markdown
47 lines

# Automations

Event-driven and scheduled workflow triggers for Otto's Expeditions.

## Automation Categories

### Extract-Load Automations

Scheduled triggers for data ingestion flows:

- #automation:extract-load-bigquery
- #automation:extract-load-databricks
- #automation:extract-load-duckdb
- #automation:extract-load-duckdb-postgres
- #automation:extract-load-motherduck
- #automation:extract-load-snowflake

### Transform Automations

Triggered after extract-load completes:

- #automation:transform-bigquery
- #automation:transform-databricks
- #automation:transform-duckdb
- #automation:transform-duckdb-postgres
- #automation:transform-motherduck
- #automation:transform-snowflake

### Downstream Automations

Triggered after transform completes:

- #automation:downstreams-bigquery
- #automation:downstreams-databricks
- #automation:downstreams-duckdb
- #automation:downstreams-duckdb-postgres
- #automation:downstreams-motherduck
- #automation:downstreams-snowflake

### Alerting

- #automation:email-on-failure - Sends email alerts on flow failures (disabled by default)
- #automation:otto-email - Otto-powered email notifications

## Configuration

Automation schedules are controlled via profile parameters. See `profiles/` for schedule configuration.
File
Text
1 lines

automations/peach_frost_alerting.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\automations\peach_frost_alerting.yaml
Contents
YAML
27 lines

automation:
  enabled: true
  name: peach_frost_alerting
  triggers:
    sensors:
      - type: timer
        name: every-3-hours
        config:
          schedule:
            cron: '0 */3 * * *'
    events:
      - types:
          - FlowRunError
        sql_filter: json_extract_string(event, '$.data.flow') = 'peach_frost_alerting'
  actions:
    - type: run_flow
      name: run-peach-frost-alerting
      config:
        flow: peach_frost_alerting
    - type: run_otto
      name: diagnose-peach-frost-alerting-failure
      config:
        agent_name: frost_pipeline_monitor
        prompt: |
          Analyze the latest peach_frost_alerting flow failure.
          Summarize likely root causes, impacted components, and safe next fixes.
          Write a concise diagnostic summary suitable for a markdown report.
File
Text
1 lines

automations/sales.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\automations\sales.yaml
Contents
YAML
15 lines

automation:
  enabled: true
  name: sales
  triggers:
    sensors:
      - type: timer
        name: cron-timer
        config:
          schedule:
            cron: 'R(0-59) */4 * * *'
  actions:
    - type: run_flow
      name: run-sales
      config:
        flow: sales
File
Text
1 lines

automations/sangamon_county_frost_alerting.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\automations\sangamon_county_frost_alerting.yaml
Contents
YAML
32 lines

automation:
  enabled: true
  name: sangamon_county_frost_alerting
  triggers:
    sensors:
      - type: timer
        name: pre-7am-frost-alert-run
        config:
          schedule:
            cron: '0 6 * * *'
      - type: timer
        name: pre-noon-daily-report-run
        config:
          schedule:
            cron: '0 11 * * *'
    events:
      - types:
          - FlowRunError
        sql_filter: json_extract_string(event, '$.data.flow') = 'sangamon_county_frost_alerting'
  actions:
    - type: run_flow
      name: run-sangamon-county-frost-alerting
      config:
        flow: sangamon_county_frost_alerting
    - type: run_otto
      name: diagnose-sangamon-county-frost-alerting-failure
      config:
        agent_name: sangamon_frost_pipeline_monitor
        prompt: |
          Analyze the latest sangamon_county_frost_alerting flow failure.
          Summarize likely root causes, impacted components, whether alerts should be paused, and the safest next fix.
          Produce the output as a concise markdown diagnostic summary.
2. connections/
Create folder
Cmd
1 lines

mkdir C:\Users\ISBE\ascend-pipelines\connections
File
Text
1 lines

connections/README.md
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\connections\README.md
Contents
Markdown
23 lines

# Connections

Data plane and source connections for Otto's Expeditions.

## Data Plane Connections

Each platform has a dedicated data plane connection:

- #connection:data_plane_bigquery - Google BigQuery
- #connection:data_plane_databricks - Databricks Unity Catalog
- #connection:data_plane_duckdb - DuckDB with DuckLake
- #connection:data_plane_duckdb_postgres - DuckDB with Postgres metadata
- #connection:data_plane_motherduck - MotherDuck cloud
- #connection:data_plane_snowflake - Snowflake

## Source Connections

- #connection:read_gcs_lake - GCS bucket for source data
- #connection:read_local_files - Local files in `data/`

## Configuration

Connection parameters are defined in profiles. Each workspace/deployment profile specifies the appropriate catalog, schema, or dataset for data isolation.
File
Text
1 lines

connections/data_plane_fabric.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\connections\data_plane_fabric.yaml
Contents
YAML
3 lines

connection:
  fabric:
    $<: $parameters.fabric
File
Text
1 lines

connections/read_gcs_lake.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\connections\read_gcs_lake.yaml
Contents
YAML
3 lines

connection:
  gcs:
    root: gs://ascend-ottos-expeditions/
File
Text
1 lines

connections/read_local_files.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\connections\read_local_files.yaml
Contents
YAML
3 lines

connection:
  local_file:
    root: data/
3. macros/
Create folder
Cmd
1 lines

mkdir C:\Users\ISBE\ascend-pipelines\macros
File
Text
1 lines

macros/README.md
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\macros\README.md
Contents
Markdown
22 lines

# SQL Macros

Reusable Jinja SQL templates for cross-platform compatibility.

## Available Macros

### `utils.sql`

| Macro | Description | Example |
|-------|-------------|---------|
| `col(col)` | Column reference wrapper | `{{ col('name') }}` |
| `standardize(col_name)` | Lowercase and trim a column | `{{ standardize('email') }}` → `LOWER(TRIM(email))` |

## Usage

Macros are automatically available in SQL components:

```sql
SELECT
    id,
    {{ standardize('customer_name') }} AS customer_name
FROM {{ ref('raw_customers') }}
6 lines


---

## File
```text
macros/utils.sql
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\macros\utils.sql
Contents
SQL
8 lines

{% macro col(col) %}
    {{ col }}
{% endmacro %}

--Standardize macro to lowercase and trim data
{% macro standardize(col_name) %}
    LOWER(TRIM({{ col_name }}))
{% endmacro %}
4. src/
Create folders
Cmd
2 lines

mkdir C:\Users\ISBE\ascend-pipelines\src
mkdir C:\Users\ISBE\ascend-pipelines\src\ascend_project_code
File
Text
1 lines

src/README.md
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\src\README.md
Contents
Markdown
23 lines

# Python Source Code

Shared Python modules automatically added to the Python path.

## Modules

### `ascend_project_code/transform.py`

Reusable transformation utilities:

- `clean(t: ibis.Table)` - Standardize column names and deduplicate rows

### `nps_analysis.py`

Ascend Module for generating NPS analysis compound components. Configurable categories and thresholds for customer satisfaction analysis.

## Usage

Import modules directly in Python components:

```python
from ascend_project_code.transform import clean
from nps_analysis import NPSAnalysis
Adding New Modules
Create a .py file in src/ or a subdirectory
Import it in your Python components
Note
For code shared across multiple projects, distribute as a Python package via pyproject.toml.

6 lines


---

## File
```text
src/ascend_project_code/transform.py
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\src\ascend_project_code\transform.py
Contents
Python
15 lines

"""
Python code for re-use within an Ascend Project.

To re-use Python code across multiple Ascend Projects, it should
be distributed as a Python package and installed through the
Project or Flow configuration.
"""

import ibis


def clean(t: ibis.Table) -> ibis.Table:
    if ibis.get_backend(t).name == "snowflake":
        return t.rename("ALL_CAPS").distinct()
    return t.rename("snake_case").distinct()
5. profiles/
Create folder
Cmd
1 lines

mkdir C:\Users\ISBE\ascend-pipelines\profiles
File
Text
1 lines

profiles/workspace_template.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\profiles\workspace_template.yaml
Contents
YAML
4 lines

profile:
  parameters:
    fabric:
      $<: $ascend_managed.fabric
File
Text
1 lines

profiles/deployment_template.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\profiles\deployment_template.yaml
Contents
YAML
4 lines

profile:
  parameters:
    fabric:
      $<: $ascend_managed.fabric
6. otto/ starter files
Create folders
Cmd
4 lines

mkdir C:\Users\ISBE\ascend-pipelines\otto
mkdir C:\Users\ISBE\ascend-pipelines\otto\agents
mkdir C:\Users\ISBE\ascend-pipelines\otto\commands
mkdir C:\Users\ISBE\ascend-pipelines\otto\rules
File
Text
1 lines

otto/README.md
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\otto\README.md
Contents
Markdown
39 lines

# Otto Configuration

AI agent customization for this project.

## Structure

| File/Directory | Description |
|----------------|-------------|
| `otto.yaml` | Main Otto configuration (MCP server access) |
| `mcp.yaml` | MCP server definitions (GitHub, MotherDuck) |
| `agents/` | Custom agent personalities |
| `commands/` | Slash command definitions |
| `rules/` | Project-specific rules for Otto |

## Custom Agents

- **Professor Otto** (`agents/professor_otto.md`) - Extended agent for new users with detailed explanations
- **Code Reviewer** (`agents/code_reviewer.md`) - Reviews code changes and provides actionable feedback

## Commands

- `/audit_rules` - Audit project rules against learning principles

## Project Rules

| Rule | Description |
|------|-------------|
| `visualizations` | Guidelines for data visualizations in artifacts |
| `learning` | Guidelines for capturing learnings and improving rules |
| `readme_maintenance` | Guidelines for maintaining README files |
| `commands` | Guidelines for Otto commands |
| `migration` | Guidelines for migrating ETL logic from other platforms |

## MCP Servers

Available MCP servers (configure access in `otto.yaml`):

- **github** - GitHub Copilot MCP (requires `GITHUB_TOKEN`)
- **motherduck** - MotherDuck MCP (requires `MOTHERDUCK_API_KEY`)
File
Text
1 lines

otto/otto.yaml
Save as
Text
1 lines

C:\Users\ISBE\ascend-pipelines\otto\otto.yaml
Contents
YAML
8 lines

otto:
  agents:
    # Otto is that "chat" agent
    chat:
      # Adjust to enable MCP servers Otto has access to
      mcp_servers: []
        # - github
        # - motherduck
Next
There are still more otto/ files available, including:

otto/mcp.yaml
otto/commands/audit_rules.md
otto/agents/*.md
otto/rules/*.md
If you want, I can continue with Part 2 and give you the path + contents of the remaining otto/ files in the same format.