# Enterprise RAG Pipeline with Medallion Architecture

This project demonstrates a production-ready, end-to-end Data Engineering and AI pipeline. It extracts messy, unstructured compliance data, enforces strict data quality contracts, processes it using PySpark, and embeds it into a Vector Database for a Retrieval-Augmented Generation (RAG) system.

## 🏗 Architecture & Tech Stack

This pipeline strictly adheres to the **Medallion Architecture** (Bronze ➔ Silver ➔ Gold) to ensure data lineage, quality, and idempotency.

* **Data Processing:** PySpark, Pandas
* **Data Quality Gates:** Great Expectations (Schema enforcement, null checks, regex validation)
* **AI & Orchestration:** LangChain, Sentence-Transformers (all-MiniLM-L6-v2)
* **Vector Database:** Qdrant (Local disk-based)
* **Environment:** Python 3.11+, Java 17

## 🚀 Pipeline Execution
1. **The Bronze Layer (Raw Data Gate)**
Ingests consumer complaints via the CFPB API, normalizes the schema, and strictly enforces a Data Contract using Great Expectations to prevent schema drift and column shifting.

Bash
python src/pipelines/ingest_bronze.py

2. **The Silver Layer (Feature Engineering)**
Cleans the validated data, drops nulls, and constructs a highly structured rag_text feature tailored for optimal vector embedding.

Bash
python src/pipelines/process_silver.py

3. **The Gold Layer (Vector Database)**
Converts the structured PySpark DataFrame into LangChain Documents, embeds the text using HuggingFace models, and indexes the vectors into Qdrant.

Bash
python src/pipelines/build_gold.py


### 1. Environment Setup
```bash
python -m venv .venv
source venv/bin/activate
pip install -r requirements.txt


**###  Utility**
 Data Inspection Utility
 A built-in CLI tool is provided to inspect the schema and data at any layer of the Medallion architecture:

Bash
python src/utils/inspect_data.py --layer silver --columns complaint_id rag_text --limit 3


** Next Steps: AI Evaluation**
The final phase of this project will implement an automated LLM Evaluation Gate using Ragas to score the RAG system on Faithfulness, Answer Relevance, and Context Recall.