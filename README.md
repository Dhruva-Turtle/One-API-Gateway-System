OneAPI Gateway System Documentation

At its core, this project is an intelligent developer chatbot widget that connects directly to a local Model Context Protocol (MCP) gateway to simplify API testing. It allows developers to inspect tool catalogs, trigger backend workflows, and debug staging environments using natural language instead of writing manual scripts. By acting as a conversational interface, it translates simple chat commands into precise API calls while automatically managing session state and parameter validation in the background.

It is designed to cleanly handle API parameter matching, structural type coercion, and persistent transaction logging without experiencing data-type crashes.


Architectural Overview

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



TechStack:
1.HTML5
2.JavaScript
3.Tailwind CSS
4.FastAPI
5.Uvicorn
6.Model Context Protocol (MCP)
7.LangChain
8.PostgreSQL
9.Chroma DB


OneAPI Gateway: Core Features

1.Conversational Chat Widget UI: Allows developers to test and trigger backend operations using natural language rather than manual code or scripts.

2.Dynamic Parameter Harvesting: Chronologically scans past database logs to automatically retrieve and populate missing session parameters behind the scenes.

3.On-the-Fly Type Coercion: Validates and converts harvested parameters in RAM to match strict upstream API schemas before execution.
4.Self-Healing Auto-Retry Loop: Intercepts 500 errors and automatically retries requests with alternative data type combinations (e.g., swapping strings to integers).

5.Zero-Constraint JSONB Logging: Preserves full transaction histories in PostgreSQL using native binary JSON columns to prevent typing mismatch crashes.

6.Adaptive Response Synthesizer: Excludes sensitive credentials, system noise, and empty fields to return clean, highly readable Markdown summaries.

7.Doc Fallback RAG Engine: Searches a local vector database to provide accurate conversational answers to pure documentation and architectural questions.


