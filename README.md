# Real-Time Customer Support Intelligence Platform

## Overview

A Google Colab academic prototype for customer-support data processing,
quality monitoring, hybrid retrieval-augmented generation, lineage,
and workflow orchestration.

## Pipeline

```text
Raw JSONL Data
    -> JSON Schema Validation
    -> Bronze Layer / Quarantine
    -> Silver Cleaning, PII Masking and Quality Scoring
    -> Gold Analytics and Knowledge Corpus
    -> Great Expectations Quality Gate
    -> Qdrant Dense Search + BM25
    -> Reciprocal Rank Fusion
    -> Cross-Encoder Reranking
    -> Grounded Answer with Citations
```

## Implemented Features

- JSON Schema validation
- Invalid-record quarantine
- Bronze, Silver and Gold logical layers
- Text normalization and PII masking
- Deduplication
- Agent-response quality scoring
- SLA and KPI calculation
- Context-aware contradiction detection
- Sentence Transformer embeddings
- Qdrant local vector index
- BM25 lexical search
- Reciprocal Rank Fusion
- Cross-encoder reranking
- Grounded answers with citations
- Great Expectations validation suites
- OpenLineage START and COMPLETE events
- RAG query audit logs
- Airflow TaskFlow DAG

## Demonstration Results

- Valid Bronze records: 70
- Quarantined records: 3
- High-quality responses: 27
- Low-quality responses: 3
- RAG corpus documents: 37
- Indexed chunks: 37
- Great Expectations: PASSED
- OpenLineage events: 10
- Airflow DAG tasks: 7

## Example Query

**Question:**

My order tracking has not updated for four days. What should I do?

**Primary source:** `Knowledge Base Article KB-0003`

## Running the Project

1. Open `Customer_Support_Capstone.ipynb` in Google Colab.
2. Select a hosted Python runtime.
3. Run all cells from top to bottom.
4. Wait for the embedding and reranker models to download.
5. Review the generated files under `/content/customer_support_capstone`.

## Important Files

- `Customer_Support_Capstone.ipynb`
- `dags/customer_support_pipeline_dag.py`
- `outputs/validation_report.json`
- `outputs/silver_quality_report.json`
- `outputs/gold_summary_report.json`
- `outputs/rag_demo_result.json`
- `outputs/great_expectations/great_expectations_quality_gate.json`
- `logs/lineage/openlineage_events.jsonl`
- `logs/audit/rag_query_audit.jsonl`

## Prototype Scope

The Colab version uses JSONL ingestion and Qdrant local mode.
The Airflow DAG is generated and syntax-validated but requires an
Airflow environment for scheduler execution.

Kafka, Flink and Delta Lake are production architecture extensions
and are not active services in this Colab prototype.
