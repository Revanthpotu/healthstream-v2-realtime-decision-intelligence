"""
============================================================================
HealthStream v2 — Kafka Integration Agent
============================================================================
An AI-powered agent that helps Kafka engineers connect new data sources.

Instead of manually:
  1. Deciding which integration method (JDBC vs CDC vs Producer)
  2. Writing connector JSON configs or Python code
  3. Deploying via REST API
  4. Debugging connector failures

The engineer describes the source, and the agent handles the rest.

USAGE:
  export OPENAI_API_KEY="sk-..."
  python agent/integration_agent.py

EXAMPLE:
  You: I have a MySQL database with patient medications. Connect it to Kafka.
  Agent: I recommend Debezium CDC for real-time capture...
         What's the hostname, port, database name, and table?
  You: hostname mysql, port 3306, database healthstream_pharmacy, table patient_medications
  Agent: Generated config and deployed. Connector is RUNNING. ✓
============================================================================
"""

import os
import sys
import json
import logging
from dotenv import load_dotenv
from openai import OpenAI

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent.tools.integration_tools import INTEGRATION_TOOL_DEFINITIONS, INTEGRATION_TOOL_FUNCTIONS

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
logger = logging.getLogger("integration_agent")

SYSTEM_PROMPT = """You are the HealthStream Integration Agent — an AI assistant that helps Kafka engineers connect new data sources to the Kafka pipeline.

You guide the engineer through a conversational flow:

1. UNDERSTAND THE SOURCE: Ask what type of data source they have (database, file, API)
2. RECOMMEND METHOD: Use recommend_integration_method to suggest the best approach
3. COLLECT DETAILS: Ask for connection details (host, port, database, table, credentials)
4. GENERATE CONFIG: Use generate_connector_config or generate_producer_code
5. SHOW PREVIEW & WAIT FOR APPROVAL: After generating the config, present it to the engineer and ask explicitly: "Should I deploy this connector? Type 'yes' to deploy, 'no' to cancel." DO NOT call deploy_connector in the same turn. Wait for the engineer's reply.
6. DEPLOY (ONLY AFTER 'yes'): Once the engineer replies 'yes', call deploy_connector
7. VERIFY: Use check_connector_status to confirm it's running

IMPORTANT RULES:
- Always call recommend_integration_method FIRST before generating configs
- For database sources, ask if they need real-time capture or delete detection
- Always explain WHY you chose a particular method
- After deploying, always verify the connector status
- If deployment fails, read the error and suggest fixes
- Use list_active_connectors to show what's already running
- CRITICAL: NEVER call deploy_connector in the same turn as generate_connector_config. ALWAYS pause, show the config, and wait for the engineer to type 'yes' in a separate message. This human-in-the-loop approval is mandatory.

DECISION LOGIC:
- CSV/File → Custom Python Producer (needs transformation control)
- Database + no real-time needs → JDBC Source Connector (simpler)
- Database + real-time OR delete detection → Debezium CDC (reads transaction log)
- MySQL → Debezium reads binlog
- PostgreSQL → Debezium reads WAL (Write-Ahead Log)

You save the Kafka engineer significant time by automating what normally takes 30-60 minutes of reading docs, writing configs, and debugging. The engineer can instead spend that time on architecture planning and requirements analysis.

Be concise, professional, and focused. You are a tool for Kafka engineers, not a chatbot.
"""


class IntegrationAgent:
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.conversation_history = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]
        # Store generated configs for deployment
        self.pending_configs = {}
        logger.info("Integration Agent initialized")

    def process_tool_calls(self, response):
        tool_results = []
        for tool_call in response.choices[0].message.tool_calls:
            fn_name = tool_call.function.name
            fn_args = json.loads(tool_call.function.arguments)

            # === HUMAN-IN-THE-LOOP GUARD ===
            # Hard gate: deploy_connector requires explicit engineer approval at runtime
            if fn_name == "deploy_connector":
                print()
                print("=" * 60)
                print("  AWAITING ENGINEER APPROVAL")
                print("=" * 60)
                print(f"  Connector to deploy: {fn_args.get('connector_name', 'unknown')}")
                print(f"  Arguments: {json.dumps(fn_args, indent=2)}")
                print("=" * 60)
                approval = input("  Type 'yes' to deploy, anything else to cancel: ").strip().lower()
                if approval != "yes":
                    print("  -> Deployment CANCELLED by engineer\n")
                    results.append({
                        "tool_call_id": tool_call.id,
                        "role": "tool",
                        "name": fn_name,
                        "content": json.dumps({
                            "status": "cancelled",
                            "reason": "Engineer did not approve deployment. Connector was NOT deployed."
                        })
                    })
                    continue
                print("  -> Engineer approved. Deploying...\n")
            # === END GUARD ===

            logger.info(f"  Tool: {fn_name}({json.dumps(fn_args)[:100]}...)")

            if fn_name in INTEGRATION_TOOL_FUNCTIONS:
                result = INTEGRATION_TOOL_FUNCTIONS[fn_name](**fn_args)

                # If config was generated, save it for deployment
                if fn_name == "generate_connector_config":
                    try:
                        parsed = json.loads(result)
                        if "config" in parsed:
                            cname = parsed["config"]["name"]
                            self.pending_configs[cname] = parsed["config"]
                            # Also save to /tmp for deploy_connector to find
                            with open(f"/tmp/connector_{cname}.json", "w") as f:
                                json.dump(parsed["config"], f)
                    except (json.JSONDecodeError, KeyError):
                        pass
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
            tools=INTEGRATION_TOOL_DEFINITIONS,
            tool_choice="auto",
        )

        max_iterations = 5
        iteration = 0
        while response.choices[0].message.tool_calls and iteration < max_iterations:
            iteration += 1
            self.conversation_history.append(response.choices[0].message)
            tool_results = self.process_tool_calls(response)
            self.conversation_history.extend(tool_results)

            response = self.client.chat.completions.create(
                model=MODEL,
                messages=self.conversation_history,
                tools=INTEGRATION_TOOL_DEFINITIONS,
                tool_choice="auto",
            )

        assistant_message = response.choices[0].message.content or ""
        self.conversation_history.append({"role": "assistant", "content": assistant_message})

        if len(self.conversation_history) > 22:
            self.conversation_history = self.conversation_history[:1] + self.conversation_history[-20:]

        return assistant_message

    def run_interactive(self):
        print("=" * 60)
        print("HealthStream v2 — Kafka Integration Agent")
        print("=" * 60)
        print()
        print("I help you connect new data sources to Kafka.")
        print("Tell me about your source and I'll handle the rest.")
        print()
        print("Examples:")
        print("  'I have a MySQL database with patient records'")
        print("  'Connect PostgreSQL orders table to Kafka'")
        print("  'I need to ingest CSV lab results'")
        print("  'Show me all active connectors'")
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
    agent = IntegrationAgent()
    agent.run_interactive()
