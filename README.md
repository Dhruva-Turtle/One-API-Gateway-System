OneAPI Gateway System Documentation

Welcome to the official developer documentation for the OneAPI Gateway (Filtered State Engine). This system serves as a robust, resilient middleware bridge between conversational AI models (using LangChain and GitHub-hosted GPT-4o-mini) and a local Model Context Protocol (MCP) gateway running on Port 7002.

It is designed to cleanly handle API parameter matching, structural type coercion, and persistent transaction logging without experiencing data-type crashes.

🏗️ Architectural Overview

The gateway is built on a modular stack designed for resilience, self-healing, and loose data-structure coupling.

       [ Web Chat Interface ] (index2.html)
                 │
                 ▼ (FastAPI Chat Port: 8005)
       ┌────────────────────────────────────────────────────────┐
       │                 ONEAPI GATEWAY ENGINE                  │
       │                                                        │
       │  ┌───────────────────┐        ┌─────────────────────┐  │
       │  │  GitHub LLM Core  │◄──────►│  LangChain Tools    │  │
       │  │  (gpt-4o-mini)    │        │  - Catalog Inspect  │  │
       │  └───────────────────┘        │  - Operation Exec   │  │
       │                               └──────────┬──────────┘  │
       │                                          │             │
       │  ┌───────────────────┐                   ▼             │
       │  │  Doc Fallback RAG │        ┌─────────────────────┐  │
       │  │  (Chroma + Embed) │        │ Dynamic Coercion &  │  │
       │  └───────────────────┘        │ Retry Engine        │  │
       │                               └──────────┬──────────┘  │
       └──────────────────────────────────────────┼─────────────┘
                                                  ▼ (JSON-RPC)
                                       ┌─────────────────────┐
                                       │   MCP Port 7002     │
                                       │  (one-api service)  │
                                       └──────────┬──────────┘
                                                  │
                                                  ▼ (Saves Payload)
                                       ┌─────────────────────┐
                                       │   PostgreSQL DB     │
                                       │ (JSONB response_log)│
                                       └─────────────────────┘


Core Components

FastAPI Web Server (Port 8005): Accepts conversational queries from web interfaces, maintaining state across a rolling conversational buffer.

PostgreSQL JSONB Storage Layer: Standardizes historical transaction payloads on disk using the native Postgres binary JSON format (JSONB). This shields the database from typing mismatched constraints (strings vs. integers).

Dynamic Parameter Harvester: Automatically scans past backend payloads chronologically to gather relevant values (like sessionId or mobileNumber) and automatically populate fields for subsequent tool executions.

Schema Blueprint Extractor: Hits the MCP server dynamically (tools/list) to analyze which fields are strictly required for any target operational tool.

Dynamic Type Coercion Utility: Translates raw database string logs into the exact data types (integer, boolean, number, string) specified by the target MCP schema metadata on-the-fly in RAM.

Combinatorial Self-Healing Retry Engine: Automatically intercepts upstream gateway failures ($500$ internal server errors) caused by schema typos, recursively attempting execution with alternative permutations of string/integer inputs.

Adaptive Response Synthesizer: Processes raw tool results back into clean, scannable Markdown layouts, dropping empty, null, or placeholder fields ('NA', None) to show only actionable telemetry metrics.

💾 Database Architecture

The system utilizes PostgreSQL configured with a schemaless database model designed for high performance and zero constraint-mismatch crashes.

The response_logs Table

The database logs are stored in a single table designed with a persistent binary JSON column (JSONB):

CREATE TABLE IF NOT EXISTS response_logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB
);


Why JSONB?

Zero Typing Mismatches: Staged systems regularly fluctuate typing structures between string and numerical IDs. A native JSONB schema accepts any key-value pattern dynamically.

Efficient Internal Querying: Allows on-disk JSON structural lookups, avoiding complex relational joins or rigid structural column mappings.

🛠️ Installation & Local Setup

Follow these instructions to establish the active local environment on your machine.

Prerequisites

macOS with Homebrew or Docker Desktop installed.

Python 3.11+ installed.

Postico 2 (or any Postgres graphical viewer client).

Step 1: Install & Boot PostgreSQL

If you are using Homebrew on your Mac, execute the following commands in your terminal:

# Install Postgres via Homebrew
brew install postgresql

# Start the background service
brew services start postgresql


Step 2: Configure Database Users & Roles

Create your dedicated development database credentials:

# Create the administrative user role
createuser -s gateway_user

# Create the target relational database
createdb oneapi_db -O gateway_user


Verify your connection by opening Postico 2, clicking New Connection, and filling out the parameters:

Host: localhost

User: gateway_user

Database: oneapi_db

Port: 5432

Step 3: Configure Environment Variables

Create an .env file in your root workspace folder and populate your active credentials:

GITHUB_TOKEN="your_github_inference_ai_key_here"
DATABASE_URL="postgresql://gateway_user:mysecretpassword@localhost:5432/oneapi_db"


Step 4: Install Python Dependencies

Set up your virtual environment and install the required dependencies:

# Activate your virtual environment (if using one)
source env/bin/activate

# Install the PostgreSQL Python adapter and other tools
pip install psycopg2-binary numpy fastapi uvicorn requests langchain-openai langchain-chroma langchain-huggingface python-dotenv


Step 5: Start the OneAPI Gateway

Boot the FastAPI bridge controller:

python3 app2.py


Upon startup, the terminal will print:
💾 [DATABASE LOG] PostgreSQL schemaless JSONB table verified successfully.

⚙️ Core Logic Implementations

1. Dynamic Parameter Harvesting & Inheritance

Instead of relying on the client interface to pass active session credentials repeatedly, app2.py scans backward through the local Postgres JSON history to retrieve keys required by the upcoming tool.

def harvest_from_postgres_logs(expected_keys: list) -> dict:
    harvested = {}
    logs = get_all_raw_logs() # Queries PG sorted by id DESC
    
    for log in logs:
        if len(harvested) == len(expected_keys):
            break
            
        def search_dict(d):
            if not isinstance(d, dict):
                return
            for k, v in d.items():
                if k in expected_keys and k not in harvested:
                    if v is not None and not isinstance(v, (dict, list)):
                        # Safely extract stringified JSON variables
                        if isinstance(v, str) and v.strip().startswith("{") and v.strip().endswith("}"):
                            try: search_dict(json.loads(v))
                            except: harvested[k] = str(v)
                        else:
                            harvested[k] = str(v)
                # Recurse through arrays and sub-objects
                if isinstance(v, dict):
                    search_dict(v)
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict): search_dict(item)
            
        search_dict(log)
    return harvested


2. Type Coercion Layer

When values are harvested from logs, they are loaded into RAM as string variables. This function checks the MCP schema blueprints to coerce them into matching primitive types just before dispatching them:

expected_type = expected_params_with_types.get(k, "string")
if expected_type == "integer":
    try: final_arguments[k] = int(v)
    except: final_arguments[k] = v
elif expected_type == "boolean":
    if isinstance(v, str): final_arguments[k] = v.lower() in ("true", "1", "yes")
    else: final_arguments[k] = bool(v)


3. Combinatorial Self-Healing Engine

If the target staging system experiences an unexpected signature type mismatch (e.g., throwing a $500$ error on mobileNumber because it wanted a string instead of an integer), the engine intercepts the error, automatically flags candidate parameters, and recursively tests alternative value types:

if "500" in str(raw_res) or "API call failed" in str(raw_res):
    # Dynamically find numbers that can be strings, and strings that can be numbers
    flip_candidates = []
    for k, v in final_arguments.items():
        if isinstance(v, str) and v.isdigit():
            flip_candidates.append((k, int(v)))
        elif isinstance(v, int):
            flip_candidates.append((k, str(v)))
            
    # Attempt logical permutations
    for r in range(1, len(flip_candidates) + 1):
        for subset in itertools.combinations(flip_candidates, r):
            test_arguments = dict(final_arguments)
            for k, flipped_val in subset:
                test_arguments[k] = flipped_val
                
            retry_res = send_to_one_api_gateway(method="tools/call", params={"name": tool_name, "arguments": test_arguments})
            if "500" not in str(retry_res):
                raw_res = retry_res # Auto-heal successful!
                break


📊 Inspecting Logs with Postico 2

Now that Postgres is fully configured, any user prompt that executes a backend tool will insert a raw log row into Postgres.

Open Postico 2 and connect to your database.

Select the response_logs table from the sidebar.

Click the Content tab at the bottom of the table viewer.

Hit Cmd + R (Refresh) to watch operational raw JSON payloads log live. Double-clicking any cell under the payload column will open a clean, structured JSON inspector right in Postico!
