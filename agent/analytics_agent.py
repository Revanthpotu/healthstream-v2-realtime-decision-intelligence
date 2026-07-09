"""
============================================================================
HealthStream v2 — Analytics Agent (Snowflake Decision Intelligence)
============================================================================
An LLM-powered agent that answers natural language analytics questions
against the Snowflake healthcare lakehouse:
  - "Which conditions drive the most readmissions?"
  - "What's the readmission risk by age group?"
  - "Show me the highest-risk patients right now."
  - "How many patients are in each risk level?"

The agent generates Snowflake SQL from English, runs it (READ-ONLY),
and explains the result in business terms.

Same GPT-4o + tools pattern as the Clinical, Integration, and DLQ agents.

USAGE:
  export OPENAI_API_KEY="sk-..."
  export SNOWFLAKE_PASSWORD="..."
  python agent/analytics_agent.py
============================================================================
"""

import os
import sys
import json
import logging
from dotenv import load_dotenv
from openai import OpenAI
import snowflake.connector

# ============================================================================
# CONFIGURATION
# ============================================================================
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("ERROR: Set OPENAI_API_KEY in .env file or environment")
    sys.exit(1)

# Snowflake connection details
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "SHPBEPY-VN05293")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "POTUREVANTH666")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "HEALTHSTREAM")

if not SNOWFLAKE_PASSWORD:
    print("ERROR: Set SNOWFLAKE_PASSWORD in .env file or environment")
    print("  Add to .env:  SNOWFLAKE_PASSWORD=your_password")
    sys.exit(1)

MODEL = "gpt-4o"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("analytics_agent")

# ============================================================================
# SCHEMA CONTEXT — tells the LLM what tables/columns exist
# ============================================================================
SCHEMA_CONTEXT = """
DATABASE: HEALTHSTREAM

SCHEMA ANALYTICS:
  READMISSIONS (one row per inpatient stay)
    - encounter_id, patient_id
    - admit_time, discharge_time (TIMESTAMP)
    - total_claim_cost (FLOAT)
    - reasondescription (STRING)
    - days_to_next_admit (INT)
    - readmitted_30d (INT: 1 = readmitted within 30 days, 0 = not)

  INPATIENT_STAYS (raw inpatient encounters)
    - encounter_id, patient_id, admit_time, discharge_time, total_claim_cost

SCHEMA ML:
  PATIENT_RISK_LEVELS (model output: risk per stay)
    - encounter_id, patient_id
    - age_at_admit (INT)
    - prior_admission_count (INT)
    - condition_count (INT)
    - readmit_probability (FLOAT 0-1)
    - risk_pct (FLOAT 0-100)
    - risk_level (STRING: 'LOW','MEDIUM','HIGH','CRITICAL')
    - actual_label (INT: 1 = actually readmitted)

  TRAINING_DATA (features used to train the model)
    - encounter_id, patient_id, age_at_admit, gender, marital, race
    - length_of_stay_days, prior_admission_count, condition_count
    - medication_count, total_claim_cost, readmitted_30d

SCHEMA RAW (source data):
  PATIENTS (id, birthdate, gender, race, marital, city, state, ...)
  ENCOUNTERS (id, "START", "STOP", patient, encounterclass, description,
              total_claim_cost, reasondescription)
              NOTE: START and STOP are reserved words, always quote as "START"/"STOP"
  CONDITIONS (patient, encounter, code, description)
  MEDICATIONS (patient, encounter, code, description, totalcost)
  OBSERVATIONS (patient, encounter, code, description, value, units, date)

JOIN KEYS:
  - READMISSIONS.patient_id = RAW.CONDITIONS.patient
  - READMISSIONS.patient_id = RAW.PATIENTS.id
  - PATIENT_RISK_LEVELS.patient_id = RAW.PATIENTS.id
"""

# ============================================================================
# SYSTEM PROMPT
# ============================================================================
SYSTEM_PROMPT = f"""You are the HealthStream Analytics Agent — an AI assistant that answers
healthcare analytics questions by querying a Snowflake lakehouse.

You translate plain-English business questions into Snowflake SQL, run the query,
and explain the result in clear business terms for hospital administrators and clinicians.

You have access to this schema:
{SCHEMA_CONTEXT}

HOW YOU WORK:
1. Understand the user's question.
2. If unsure what tables exist, use list_tables or describe_table.
3. Write a single Snowflake SELECT query to answer it, then call run_query.
4. Read the result and explain it in plain business language — lead with the insight,
   not the raw numbers.

CRITICAL RULES:
- ONLY generate SELECT queries. Never INSERT, UPDATE, DELETE, DROP, CREATE, ALTER.
- Always fully qualify tables: HEALTHSTREAM.ANALYTICS.READMISSIONS, etc.
- START and STOP in RAW.ENCOUNTERS are reserved words — always write them as "START" and "STOP".
- Keep results readable — use LIMIT when listing rows (e.g. top 10).
- When a question is about cost, note that this is synthetic Synthea data so absolute
  dollar values are not realistic, but the analysis pattern is production-ready.
- Explain WHY a result matters clinically or operationally, not just what the numbers are.
"""

# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
    )

# ============================================================================
# TOOL IMPLEMENTATIONS
# ============================================================================
def _is_read_only(sql: str) -> bool:
    """Guard: only allow SELECT / WITH queries."""
    stripped = sql.strip().lower()
    # Block anything that isn't a pure read
    forbidden = ("insert", "update", "delete", "drop", "create",
                 "alter", "truncate", "merge", "grant", "revoke")
    first_word = stripped.split()[0] if stripped.split() else ""
    if first_word not in ("select", "with"):
        return False
    # Extra safety: block forbidden keywords as statements (semicolon-separated)
    for stmt in stripped.split(";"):
        s = stmt.strip()
        if s and s.split()[0] in forbidden:
            return False
    return True


def run_query(sql: str) -> str:
    """Execute a read-only SQL query against Snowflake and return rows as JSON."""
    if not _is_read_only(sql):
        return json.dumps({
            "error": "Blocked: only SELECT queries are allowed. "
                     "This agent is read-only."
        })
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(sql)
        columns = [c[0] for c in cur.description]
        rows = cur.fetchmany(100)  # cap at 100 rows
        cur.close()
        conn.close()
        result = [dict(zip(columns, row)) for row in rows]
        # Convert non-serializable types to strings
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


def list_tables() -> str:
    """List all tables across the HEALTHSTREAM schemas."""
    sql = """
        SELECT table_schema, table_name
        FROM HEALTHSTREAM.INFORMATION_SCHEMA.TABLES
        WHERE table_schema IN ('RAW','ANALYTICS','ML')
        ORDER BY table_schema, table_name
    """
    return run_query(sql)


def describe_table(schema: str, table: str) -> str:
    """Show the columns of a given table."""
    sql = f"""
        SELECT column_name, data_type
        FROM HEALTHSTREAM.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema.upper()}'
          AND table_name = '{table.upper()}'
        ORDER BY ordinal_position
    """
    return run_query(sql)


# ============================================================================
# TOOL DEFINITIONS (OpenAI function-calling schema)
# ============================================================================
TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "run_query",
            "description": "Execute a read-only Snowflake SELECT query and return rows. "
                           "Use this to answer analytics questions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A single Snowflake SELECT query, fully qualified "
                                       "with HEALTHSTREAM.<schema>.<table>."
                    }
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List all available tables in the HEALTHSTREAM lakehouse.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "describe_table",
            "description": "Show the columns and types of a specific table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name: RAW, ANALYTICS, or ML"},
                    "table": {"type": "string", "description": "Table name"},
                },
                "required": ["schema", "table"],
            },
        },
    },
]

TOOL_FUNCTIONS = {
    "run_query": run_query,
    "list_tables": list_tables,
    "describe_table": describe_table,
}

# ============================================================================
# ANALYTICS AGENT CLASS  (matches HealthStreamAgent pattern)
# ============================================================================
class AnalyticsAgent:
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]
        logger.info("Analytics Agent initialized with GPT-4o + Snowflake")

    def process_tool_calls(self, response):
        tool_results = []
        for tool_call in response.choices[0].message.tool_calls:
            fn_name = tool_call.function.name
            fn_args = json.loads(tool_call.function.arguments)

            # Log the generated SQL so it's visible in the demo
            if fn_name == "run_query":
                logger.info(f"  Generated SQL: {fn_args.get('sql','')}")
            else:
                logger.info(f"  Tool call: {fn_name}({fn_args})")

            if fn_name in TOOL_FUNCTIONS:
                result = TOOL_FUNCTIONS[fn_name](**fn_args)
            else:
                result = json.dumps({"error": f"Unknown tool: {fn_name}"})

            tool_results.append({
                "tool_call_id": tool_call.id,
                "role": "tool",
                "content": result,
            })
        return tool_results

    def chat(self, user_message: str) -> str:
        self.conversation_history.append({"role": "user", "content": user_message})

        response = self.client.chat.completions.create(
            model=MODEL,
            messages=self.conversation_history,
            tools=TOOL_DEFINITIONS,
            tool_choice="auto",
        )

        max_iterations = 5
        iteration = 0
        while (
            response.choices[0].message.tool_calls
            and iteration < max_iterations
        ):
            iteration += 1
            self.conversation_history.append(response.choices[0].message)
            tool_results = self.process_tool_calls(response)
            self.conversation_history.extend(tool_results)
            response = self.client.chat.completions.create(
                model=MODEL,
                messages=self.conversation_history,
                tools=TOOL_DEFINITIONS,
                tool_choice="auto",
            )

        assistant_message = response.choices[0].message.content or ""
        self.conversation_history.append(
            {"role": "assistant", "content": assistant_message}
        )

        if len(self.conversation_history) > 22:
            self.conversation_history = (
                self.conversation_history[:1] + self.conversation_history[-20:]
            )

        return assistant_message

    def run_interactive(self):
        print("=" * 60)
        print("HealthStream v2 — Analytics Agent (Snowflake)")
        print("=" * 60)
        print("Ask analytics questions in plain English. I generate Snowflake SQL,")
        print("run it against the lakehouse, and explain the result.")
        print()
        print("Example questions:")
        print("  'Which conditions drive the most readmissions?'")
        print("  'What is the readmission risk by age group?'")
        print("  'Show me the 10 highest-risk patients.'")
        print("  'How many patients are in each risk level?'")
        print()
        print("Type 'quit' or 'exit' to stop.")
        print("=" * 60)
        print()

        while True:
            try:
                user_input = input("You: ").strip()
                if not user_input:
                    continue
                if user_input.lower() in ("quit", "exit", "q"):
                    print("Goodbye!")
                    break
                print()
                response = self.chat(user_input)
                print(f"Agent: {response}")
                print()
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
                logger.error(f"Agent error: {e}")


# ============================================================================
# DEMO MODE
# ============================================================================
def run_demo():
    agent = AnalyticsAgent()
    demo_questions = [
        "Which conditions drive the most 30-day readmissions?",
        "What is the readmission risk by age group?",
        "How many patients are in each risk level?",
        "Show me the 10 highest-risk patients.",
    ]
    print("=" * 60)
    print("HealthStream v2 — Analytics Agent DEMO MODE")
    print("=" * 60)
    print()
    for q in demo_questions:
        print(f"{'='*60}")
        print(f"DEMO Q: {q}")
        print(f"{'='*60}")
        response = agent.chat(q)
        print(f"\nAgent: {response}\n")
        input("Press Enter for next question...")
        print()


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="HealthStream Analytics Agent")
    parser.add_argument("--demo", action="store_true", help="Run demo mode")
    args = parser.parse_args()

    if args.demo:
        run_demo()
    else:
        agent = AnalyticsAgent()
        agent.run_interactive()
