"""
============================================================================
HealthStream v2 — DLQ Monitor Agent
============================================================================
An AI-powered agent that monitors the Dead Letter Queue and helps
Kafka engineers diagnose and resolve data quality issues.

Instead of a Kafka engineer manually:
  1. Opening the console to check DLQ messages
  2. Reading raw error messages and trying to understand them
  3. Searching logs for related connector failures
  4. Spending 15-30 minutes on root cause analysis

The agent does all of this in seconds and generates a report.

USAGE:
  export OPENAI_API_KEY="sk-..."
  python agent/dlq_monitor_agent.py              # Interactive mode
  python agent/dlq_monitor_agent.py --scan       # One-time scan + report

EXAMPLE:
  You: Scan the DLQ and tell me what's going on
  Agent: Found 15 errors in the DLQ. 12 are OUT_OF_RANGE errors from
         the CSV lab export. Root cause: heart rate values above the
         configured maximum. Recommended fix: update validation range
         or check the source sensor...
============================================================================
"""

import os
import sys
import json
import logging
from dotenv import load_dotenv
from openai import OpenAI

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent.tools.dlq_tools import DLQ_TOOL_DEFINITIONS, DLQ_TOOL_FUNCTIONS

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("ERROR: Set OPENAI_API_KEY in .env file or environment")
    sys.exit(1)

MODEL = "gpt-4o"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dlq_monitor")

SYSTEM_PROMPT = """You are the HealthStream DLQ Monitor Agent — an AI assistant that monitors the Dead Letter Queue and helps Kafka engineers diagnose data quality issues.

Your job is to SAVE the Kafka engineer's time by automating what they normally do manually:
- Scanning the DLQ for new error messages
- Categorizing errors by type and source
- Performing root cause analysis
- Recommending specific fixes
- Generating alert reports

WORKFLOW when asked to scan or monitor:
1. Call scan_dlq_topic to check for errors
2. If errors found, call analyze_error_patterns to categorize them
3. Call diagnose_root_cause for the top error category
4. Call check_connector_health to see if connectors are related
5. Call generate_alert_report to create the full report

IMPORTANT:
- Always quantify the impact: "15 errors affecting 8 patients" not just "some errors"
- Always provide specific fix steps, not vague suggestions
- Always mention the reprocessing strategy (how to recover the failed messages)
- Always estimate time saved: "This analysis would normally take 15-30 minutes manually"
- When generating reports, mention that in production this would be sent via email/Slack

You help Kafka engineers focus on architecture and planning instead of spending time on manual DLQ monitoring.

Be concise and direct. Engineers want answers, not essays.
"""


class DLQMonitorAgent:
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]
        logger.info("DLQ Monitor Agent initialized")

    def process_tool_calls(self, response):
        tool_results = []
        for tool_call in response.choices[0].message.tool_calls:
            fn_name = tool_call.function.name
            fn_args = json.loads(tool_call.function.arguments)

            logger.info(f"  Tool: {fn_name}")

            if fn_name in DLQ_TOOL_FUNCTIONS:
                result = DLQ_TOOL_FUNCTIONS[fn_name](**fn_args)
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
            tools=DLQ_TOOL_DEFINITIONS,
            tool_choice="auto",
        )

        max_iterations = 6
        iteration = 0
        while response.choices[0].message.tool_calls and iteration < max_iterations:
            iteration += 1
            self.conversation_history.append(response.choices[0].message)
            tool_results = self.process_tool_calls(response)
            self.conversation_history.extend(tool_results)

            response = self.client.chat.completions.create(
                model=MODEL,
                messages=self.conversation_history,
                tools=DLQ_TOOL_DEFINITIONS,
                tool_choice="auto",
            )

        assistant_message = response.choices[0].message.content or ""
        self.conversation_history.append({"role": "assistant", "content": assistant_message})

        if len(self.conversation_history) > 22:
            self.conversation_history = self.conversation_history[:1] + self.conversation_history[-20:]

        return assistant_message

    def run_scan(self):
        """Run a one-time DLQ scan and print the report."""
        print("=" * 60)
        print("HealthStream v2 — DLQ Monitor (Auto-Scan)")
        print("=" * 60)
        print()
        print("Scanning Dead Letter Queue...")
        print()

        response = self.chat(
            "Scan the DLQ, analyze any errors, diagnose the root cause, "
            "check connector health, and generate a full alert report. "
            "Give me a concise summary with priority level and recommended actions."
        )
        print(f"Report:\n{response}")
        print()
        print("=" * 60)

    def run_interactive(self):
        print("=" * 60)
        print("HealthStream v2 — DLQ Monitor Agent")
        print("=" * 60)
        print()
        print("I monitor the Dead Letter Queue and help you diagnose issues.")
        print()
        print("Examples:")
        print("  'Scan the DLQ and tell me what's going on'")
        print("  'Why are messages failing?'")
        print("  'Generate an alert report'")
        print("  'Check if any connectors are down'")
        print("  'What's the reprocessing strategy for these errors?'")
        print()
        print("Type 'quit' to exit.")
        print("=" * 60)
        print()

        while True:
            try:
                user_input = input("Engineer: ").strip()
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HealthStream DLQ Monitor Agent")
    parser.add_argument("--scan", action="store_true", help="Run one-time scan and report")
    args = parser.parse_args()

    agent = DLQMonitorAgent()
    if args.scan:
        agent.run_scan()
    else:
        agent.run_interactive()
