# ⚽ FPL Predictive Model (Graph-RAG + LLM Assistant)

**Team Project — ACL Course / MS3**

This repository implements a **Fantasy Premier League (FPL) Knowledge-Graph-based Retrieval Augmented Generation (RAG) system** enhanced with large language models (LLMs) such as **Gemini, Mistral, and Cohere**.  
It allows users to query historical player performance and receive natural language answers powered by Neo4j knowledge graph + generative AI reasoning.

---

## 🎯 Project Overview

FPL Predictive Model uses a **hybrid AI architecture** that combines:

- 🧠 **Neo4j Knowledge Graph** of FPL historical stats  
- 🔍 **Semantic retrieval using embeddings**  
- 🤝 **RAG pipeline for context-aware responses**  
- 💬 **Large Language Models for answering user queries**
- 📊 **Qualitative & Quantitative evaluation of model responses**

Users can ask natural language questions such as:

> "Provide me with the history of stats for Phil Foden"

The system retrieves structured FPL data → builds a contextual prompt → feeds it to an LLM → returns a reliable, context-based response.

---

## 🚀 Features

✔ Retrieve historical FPL player stats  
✔ Semantic search using vector embeddings + cosine similarity  
✔ Supports **Gemini, Mistral, and Cohere LLMs**  
✔ Streamlit-based chat interface  
✔ Knowledge Graph transparency view  
✔ Response evaluation (tokens, latency, quality dimensions)

---


## 🧠 Technologies Used

| Purpose | Technology |
|--------|------------|
| Backend | Python |
| Knowledge Graph | Neo4j |
| Embeddings | sentence-transformers |
| LLM APIs | Gemini • Mistral • Cohere |
| Frontend | Streamlit |
| Deployment | Streamlit Cloud / GitHub |

