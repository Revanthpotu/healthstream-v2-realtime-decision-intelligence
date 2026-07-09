"""
============================================================================
HealthStream v2 — AI Agent (Step 8)
============================================================================
An LLM-powered agent that answers natural language questions about:
  - Patient health: "Tell me about patient X" / "Who needs attention?"
  - Pipeline operations: "How is my pipeline doing?" / "Any DLQ issues?"
  - Impact analysis: "What if Source C goes down?"

Inspired by Confluent's Real-Time Context Engine (MCP pattern).
Instead of Confluent Cloud + Flink SQL, we use:
  ksqlDB (stream processing) → Redis (context serving) → AI Agent (tools)

USAGE:
  export OPENAI_API_KEY="sk-..."
  python agent/agent.py
============================================================================
"""

import os
import sys
import json
import logging
from dotenv import load_dotenv
from openai import OpenAI

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent.tools.agent_tools import TOOL_DEFINITIONS, TOOL_FUNCTIONS

# ============================================================================
# CONFIGURATION
# ============================================================================
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("ERROR: Set OPENAI_API_KEY in .env file or environment")
    print("  export OPENAI_API_KEY='sk-...'")
    print("  OR create a .env file with: OPENAI_API_KEY=sk-...")
    sys.exit(1)

MODEL = "gpt-4o"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ai_agent")

# ============================================================================
# SYSTEM PROMPT
# ============================================================================
SYSTEM_PROMPT = """You are the HealthStream AI Agent — an intelligent assistant that monitors a real-time patient data integration pipeline.

You have access to real-time streaming data from 3 hospital systems:
- Source A (CSV Lab Export): Patient vital signs (heart rate, blood pressure, O2, temperature) via a custom Python producer
- Source B (PostgreSQL EHR): Patient conditions/diagnoses via Kafka Connect JDBC
- Source C (MySQL Pharmacy): Patient medications via Debezium CDC (Change Data Capture)

All data flows through Apache Kafka (3 brokers, replication factor 3), is processed by ksqlDB for joins and trend calculations, materialized into Redis for sub-millisecond lookups, and served to you through tools.

You operate in TWO MODES:

**CLINICAL MODE** — When asked about patients:
- Use get_patient_context to look up specific patients
- Use get_patients_needing_attention to find urgent cases
- Explain vital signs in clinical context (age, conditions, medications matter)
- Highlight TRENDS, not just current values — a rising heart rate is more concerning than a static one
- Always mention relevant conditions and medications that affect interpretation

**OPERATIONAL MODE** — When asked about the pipeline:
- Use get_pipeline_health to check overall system status
- Use get_dlq_analysis to investigate data quality issues
- Use get_source_impact to assess failure scenarios
- Report in BUSINESS terms: "142 patients affected" not just "lag: 2847"
- Provide specific recovery recommendations

IMPORTANT GUIDELINES:
- Always use tools to get real-time data — never guess or make up patient data
- When reporting trends, explain WHY they matter clinically
- When reporting pipeline issues, estimate PATIENT IMPACT
- Be concise but thorough
- If a tool returns an error, explain what it means and suggest next steps
"""

# ============================================================================
# AI AGENT CLASS
# ============================================================================
class HealthStreamAgent:
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]
        logger.info("AI Agent initialized with GPT-4o")

    def process_tool_calls(self, response):
        """Execute tool calls from the LLM and return results."""
        tool_results = []
        for tool_call in response.choices[0].message.tool_calls:
            fn_name = tool_call.function.name
            fn_args = json.loads(tool_call.function.arguments)

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
        """Send a message and get a response, with tool calling."""
        self.conversation_history.append({"role": "user", "content": user_message})

        # Call GPT-4 with tools
        response = self.client.chat.completions.create(
            model=MODEL,
            messages=self.conversation_history,
            tools=TOOL_DEFINITIONS,
            tool_choice="auto",
        )

        # Handle tool calls (may need multiple rounds)
        max_iterations = 5
        iteration = 0
        while (
            response.choices[0].message.tool_calls
            and iteration < max_iterations
        ):
            iteration += 1
            # Add assistant's tool call message
            self.conversation_history.append(response.choices[0].message)

            # Execute tools and add results
            tool_results = self.process_tool_calls(response)
            self.conversation_history.extend(tool_results)

            # Call GPT-4 again with tool results
            response = self.client.chat.completions.create(
                model=MODEL,
                messages=self.conversation_history,
                tools=TOOL_DEFINITIONS,
                tool_choice="auto",
            )

        # Get final text response
        assistant_message = response.choices[0].message.content or ""
        self.conversation_history.append(
            {"role": "assistant", "content": assistant_message}
        )

        # Keep conversation history manageable (last 20 messages)
        if len(self.conversation_history) > 22:
            self.conversation_history = (
                self.conversation_history[:1] + self.conversation_history[-20:]
            )

        return assistant_message

    def run_interactive(self):
        """Run interactive chat loop."""
        print("=" * 60)
        print("HealthStream v2 — AI Agent")
        print("=" * 60)
        print("I can answer questions about patients and the pipeline.")
        print()
        print("Example questions:")
        print("  Clinical:    'Which patients need attention?'")
        print("  Clinical:    'Tell me about patient <UUID>'")
        print("  Operational: 'How is my pipeline doing?'")
        print("  Operational: 'Show me DLQ errors'")
        print("  Impact:      'What if the MySQL source goes down?'")
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
# DEMO MODE — Pre-scripted questions for presentations
# ============================================================================
def run_demo():
    """Run pre-scripted demo questions for presentations."""
    agent = HealthStreamAgent()

    demo_questions = [
        "Which patients currently need clinical attention?",
        "How is my data pipeline performing right now?",
        "Show me any dead letter queue issues and what's causing them.",
        "What would happen if the MySQL pharmacy source goes down?",
    ]

    print("=" * 60)
    print("HealthStream v2 — AI Agent DEMO MODE")
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

    parser = argparse.ArgumentParser(description="HealthStream AI Agent")
    parser.add_argument("--demo", action="store_true", help="Run demo mode")
    args = parser.parse_args()

    if args.demo:
        run_demo()
    else:
        agent = HealthStreamAgent()
        agent.run_interactive()
