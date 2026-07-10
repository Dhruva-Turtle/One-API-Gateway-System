import os
import json
import psycopg2
import requests
import itertools
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://gateway_user:mysecretpassword@localhost:5432/oneapi_db")

app = FastAPI(title="OneAPI Gateway - PostgreSQL JSONB Parameter Harvester")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("📚 Loading local documentation vector store...")
local_embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
db = Chroma(persist_directory="./chroma_local_db", embedding_function=local_embeddings)

def init_db():
    """Initializes the PostgreSQL database with an immune JSONB payload column."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS response_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                payload JSONB
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("💾 [DATABASE LOG] PostgreSQL schemaless JSONB table verified successfully.")
    except Exception as e:
        print(f"❌ [DATABASE LOG] Error initializing PostgreSQL database: {e}")

init_db()

def log_raw_response(res_json: dict):
    """Saves any successful backend response into Postgres as an unstructured data dictionary."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO response_logs (payload) VALUES (%s)", (json.dumps(res_json),))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ [DATABASE LOG] Failed to preserve database packet in PostgreSQL: {e}")

def get_all_raw_logs() -> list:
    """Retrieves long-term chronological log history out of the PostgreSQL cluster."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT payload FROM response_logs ORDER BY id DESC")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        parsed_logs = []
        for row in rows:
            val = row[0]
            if isinstance(val, str):
                parsed_logs.append(json.loads(val))
            else:
                parsed_logs.append(val)
        return parsed_logs
    except Exception as e:
        print(f"⚠️ [DATABASE LOG] Failed to read PostgreSQL payload rows: {e}")
        return []

conversation_memory = []

def send_to_one_api_gateway(method: str, params: dict) -> str:
    """Establishes an active channel strictly with the 'one-api' namespace on port 7002."""
    hosts = ["127.0.0.1", "localhost"]
    last_error = "No host attempted"
    
    for host in hosts:
        base_url = f"http://{host}:7002/mcp/one-api"
        post_url = None
        
        try:
            response = requests.get(base_url, stream=True, headers={"Accept": "text/event-stream"}, timeout=4)
            for line in response.iter_lines():
                if line:
                    decoded = line.decode('utf-8')
                    if "data:" in decoded:
                        path = decoded.split("data:")[-1].strip()
                        post_url = f"http://{host}:7002{path}" if path.startswith("/") else path
                        break
            
            if not post_url:
                direct_res = requests.get(base_url, timeout=3)
                if direct_res.status_code == 200 and direct_res.text.strip().startswith("{"):
                    json_data = direct_res.json()
                    path = json_data.get("url") or json_data.get("path") or json_data.get("endpoint")
                    if path:
                        post_url = f"http://{host}:7002{path}" if path.startswith("/") else path
            
            if not post_url:
                post_url = base_url

            init_payload = {
                "jsonrpc": "2.0", "method": "initialize",
                "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "one-api-client", "version": "1.0"}},
                "id": 1
            }
            requests.post(post_url, json=init_payload, timeout=4)
            
            payload = {
                "jsonrpc": "2.0", "method": method, "params": params, "id": 2
            }
            
            final_response = requests.post(post_url, json=payload, timeout=15)
            return final_response.text
            
        except Exception as e:
            last_error = str(e)
            continue
            
    return f"Failed to communicate with local OneAPI gateway on port 7002: {last_error}"


def get_expected_parameters_with_types(tool_name: str) -> dict:
    """Fetches the official MCP catalog schemas to determine tool expected keys and types."""
    try:
        raw_tools_data = send_to_one_api_gateway(method="tools/list", params={})
        tools_json = json.loads(raw_tools_data)
        tools_list = tools_json.get("result", {}).get("tools", [])
        for t in tools_list:
            if t.get("name") == tool_name:
                properties = t.get("inputSchema", {}).get("properties", {})
                return {k: v.get("type", "string") for k, v in properties.items()}
    except Exception as e:
        print(f"⚠️ [WIDGET LOG] Schema blueprint lookup failed: {e}")
    return {}

def harvest_from_postgres_logs(expected_keys: list) -> dict:
    """Scans backward through PostgreSQL data blocks to retrieve cached values seamlessly."""
    harvested = {}
    logs = get_all_raw_logs()
    
    for log in logs:
        if len(harvested) == len(expected_keys):
            break
            
        def search_dict(d):
            if not isinstance(d, dict):
                return
            for k, v in d.items():
                if k in expected_keys and k not in harvested:
                    if v is not None and not isinstance(v, (dict, list)):
                        if isinstance(v, str) and v.strip().startswith("{") and v.strip().endswith("}"):
                            try:
                                search_dict(json.loads(v))
                            except:
                                harvested[k] = str(v)
                        else:
                            harvested[k] = str(v)
                
                if isinstance(v, dict):
                    search_dict(v)
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            search_dict(item)
                        elif isinstance(item, str) and item.strip().startswith("{"):
                            try:
                                search_dict(json.loads(item))
                            except:
                                pass
                                
        search_dict(log)
    return harvested

@tool
def inspect_one_api_catalog() -> str:
    """
    Use this tool to view the live catalog of available operations, functions, schemas,
    and configurations currently loaded into your active One API routing core context.
    """
    print(f"⚙️ [WIDGET LOG] Tool Invoked -> Fetching One API Catalog Manifest...")
    return send_to_one_api_gateway(method="tools/list", params={})

@tool
def execute_one_api_operation(tool_name: str, tool_arguments: dict) -> str:
    """
    Use this tool to run live operational commands or API features inside the One API platform.
    Pass only the arguments explicitly mentioned or provided by the user in this turn.
    """
    expected_params_with_types = get_expected_parameters_with_types(tool_name)
    expected_keys = list(expected_params_with_types.keys())
    
    harvested_args = harvest_from_postgres_logs(expected_keys)
    merged_arguments = {**harvested_args, **tool_arguments}
    
    print(f"⚙️ [WIDGET LOG] Explicit user inputs for this turn: {merged_arguments}")
    print(f"🔍 [WIDGET LOG] Blueprint Lookup: Tool '{tool_name}' expects fields: {expected_keys}")
    
    final_arguments = {}
    for k, v in merged_arguments.items():
        if k in expected_keys:
            if v is None:
                final_arguments[k] = None
                continue
            
            expected_type = expected_params_with_types.get(k, "string")
            if expected_type == "integer":
                try: final_arguments[k] = int(v)
                except: final_arguments[k] = v
            elif expected_type == "number":
                try: final_arguments[k] = float(v)
                except: final_arguments[k] = v
            elif expected_type == "boolean":
                if isinstance(v, str): final_arguments[k] = v.lower() in ("true", "1", "yes")
                else: final_arguments[k] = bool(v)
            else:
                final_arguments[k] = str(v)
                
    print(f"🚀 [WIDGET LOG] Dispatching strict filtered payload to Port 7002: {final_arguments}")
    
    params = {"name": tool_name, "arguments": final_arguments}
    raw_res = send_to_one_api_gateway(method="tools/call", params=params)
    
    if "500" in str(raw_res) or "API call failed" in str(raw_res):
        print("⚠️ [WIDGET LOG] Detected potential 500 error from upstream staging. Evaluating parameter combinations...")
        flip_candidates = []
        for k, v in final_arguments.items():
            if isinstance(v, str) and v.isdigit():
                flip_candidates.append((k, int(v)))
            elif isinstance(v, int):
                flip_candidates.append((k, str(v)))
                
        if flip_candidates:
            success = False
            for r in range(1, len(flip_candidates) + 1):
                if success:
                    break
                for subset in itertools.combinations(flip_candidates, r):
                    test_arguments = dict(final_arguments)
                    for k, flipped_val in subset:
                        test_arguments[k] = flipped_val
                        
                    print(f"🔄 [WIDGET LOG] Auto-Retry testing combination: {test_arguments}")
                    retry_params = {"name": tool_name, "arguments": test_arguments}
                    retry_res = send_to_one_api_gateway(method="tools/call", params=retry_params)
                    
                    if "500" not in str(retry_res) and "API call failed" not in str(retry_res):
                        print(f"✅ [WIDGET LOG] Self-Healing Auto-Retry Succeeded! Clean Bypass achieved.")
                        raw_res = retry_res
                        success = True
                        break
            if not success:
                print("❌ [WIDGET LOG] All combinatorial auto-retries failed with 500. Keeping original response.")
                
    try:
        res_json = json.loads(raw_res)
        log_raw_response(res_json)
    except:
        pass
        
    return raw_res


llm = ChatOpenAI(
    model="gpt-4o-mini",
    api_key=GITHUB_TOKEN,
    base_url="https://models.inference.ai.azure.com",
    temperature=0.1
)
llm_with_tools = llm.bind_tools([inspect_one_api_catalog, execute_one_api_operation])

def ensure_string(content) -> str:
    """Safely flattens structured dictionary blocks into clean text formatting."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts = []
        for part in content:
            if isinstance(part, str):
                text_parts.append(part)
            elif isinstance(part, dict) and "text" in part:
                text_parts.append(part["text"])
        return "".join(text_parts)
    return str(content)


class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat_endpoint(request: ChatRequest):
    global conversation_memory
    user_query = request.message
    print(f"📩 [WIDGET LOG] User input: {user_query}")
    
    try:
        system_context = (
            "You are the dedicated OneAPI System Assistant connected exclusively to your platform gateway.\n\n"
            "STATE SHIFT WARNING:\n"
            "A programmatic runtime proxy automatically handles parameter inheritance behind the scenes.\n"
            "Therefore, look only at what fields the user explicitly provides in this turn. "
            "Do not worry about missing common fields; the backend will harvest and merge them.\n\n"
            "OPERATIONAL PROTOCOL:\n"
            "1. If a user asks what actions, tasks, or capabilities you can handle, invoke 'inspect_one_api_catalog'.\n"
            "2. If a user wants to perform an action, invoke 'execute_one_api_operation' with the current inputs.\n"
            "3. If their request is purely informational, fall back to checking your local documentation files."
        )
        
        messages = [("system", system_context)] + conversation_memory + [("user", user_query)]
        ai_msg = llm_with_tools.invoke(messages)
        
        if ai_msg.tool_calls:
            for tool_call in ai_msg.tool_calls:
                name = tool_call["name"]
                args = tool_call["args"]
                print(f"🚀 [WIDGET LOG] GitHub Brain triggered live backend tool: {name}")
                
                if name == "inspect_one_api_catalog":
                    raw_result = inspect_one_api_catalog.invoke(args)
                elif name == "execute_one_api_operation":
                    raw_result = execute_one_api_operation.invoke(args)
                else:
                    continue
                
                synthesis = llm.invoke(
                    f"The user query was: '{user_query}'.\n"
                    f"The OneAPI gateway platform executed a tool and returned this raw data:\n{raw_result}\n\n"
                    "CRITICAL ASSISTANT INSTRUCTION:\n"
                    "1. Analyze the raw data and dynamically organize the key findings into clean, scannable Markdown (using bold headers and bullet points).\n"
                    "2. Adapt the summary fields to match the specific tool executed (e.g., show authentication fields for login, product details for catalogs, or coverage fields for policies).\n"
                    "3. Keep it detailed enough to be useful, but aggressively prune out system noise: NEVER display security hashes, trace IDs, or microsecond timestamps.\n"
                    "4. STRICT EXCLUSION RULE - NO EMPTY FIELDS: Completely drop any line or parameter if its value is missing, empty, null, false, undefined, or a placeholder string like 'NA'. Only show a parameter if it contains a concrete, meaningful, valid value.\n"
                    "5. Deliver the response cleanly and professionally without conversational filler."
                )
                final_reply = ensure_string(synthesis.content)
                
                conversation_memory.append(("user", user_query))
                conversation_memory.append(("assistant", final_reply))
                conversation_memory = conversation_memory[-14:]
                
                return {"response": final_reply}
        
        print("🔍 [WIDGET LOG] Fallback to local doc store database...")
        docs = db.similarity_search(user_query, k=3)
        context = "\n".join([d.page_content for d in docs])
        response = llm.invoke(f"Context:\n{context}\n\nQuestion: {user_query}\nAnswer:")
        final_reply = ensure_string(response.content)
        
        conversation_memory.append(("user", user_query))
        conversation_memory.append(("assistant", final_reply))
        conversation_memory = conversation_memory[-14:]
        
        return {"response": final_reply}
        
    except Exception as e:
        print(f"❌ [WIDGET LOG] Terminal Exception: {str(e)}")
        return {"response": f"System Error processing query: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8005)