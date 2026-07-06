"""
End-to-end orchestration DAG for the
Real-Time Customer Support Intelligence Platform.

Airflow 3 TaskFlow DAG.
"""

from datetime import datetime, timezone
from pathlib import Path
import json
import os

from airflow.sdk import dag, task


PROJECT_ROOT = Path(
    os.environ.get(
        "CUSTOMER_SUPPORT_PROJECT_ROOT",
        "/content/customer_support_capstone"
    )
)

OUTPUT_DIR = PROJECT_ROOT / "outputs"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
AUDIT_DIR = PROJECT_ROOT / "logs" / "audit"
LINEAGE_DIR = PROJECT_ROOT / "logs" / "lineage"


def require_file(path: Path) -> Path:
    """Raise an error when a required pipeline artifact is missing."""

    if not path.exists():
        raise FileNotFoundError(
            f"Required pipeline artifact was not found: {path}"
        )

    if path.stat().st_size == 0:
        raise ValueError(
            f"Required pipeline artifact is empty: {path}"
        )

    return path


def load_json(path: Path) -> dict:
    """Load a required JSON artifact."""

    require_file(path)

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


@dag(
    dag_id="customer_support_intelligence_pipeline",
    description=(
        "End-to-end customer support ingestion, quality, "
        "analytics, RAG, lineage, and audit workflow."
    ),
    schedule=None,
    start_date=datetime(
        2025,
        1,
        1,
        tzinfo=timezone.utc
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "customer-support-team",
        "retries": 1
    },
    tags=[
        "customer-support",
        "quality",
        "rag",
        "qdrant",
        "openlineage"
    ]
)
def customer_support_intelligence_pipeline():

    @task
    def validate_bronze_ingestion() -> dict:
        """
        Confirm schema validation, Bronze ingestion,
        and quarantine routing.
        """

        report = load_json(
            OUTPUT_DIR / "validation_report.json"
        )

        valid_records = report.get(
            "total_valid_records",
            0
        )

        quarantined_records = report.get(
            "total_quarantined_records",
            0
        )

        if valid_records <= 0:
            raise ValueError(
                "Bronze ingestion contains no valid records."
            )

        required_bronze_files = [
            PROJECT_ROOT
            / "data/bronze/support_tickets_bronze.jsonl",

            PROJECT_ROOT
            / "data/bronze/chat_messages_bronze.jsonl",

            PROJECT_ROOT
            / "data/bronze/knowledge_articles_bronze.jsonl"
        ]

        for file_path in required_bronze_files:
            require_file(file_path)

        return {
            "stage": "bronze",
            "status": "passed",
            "valid_records": valid_records,
            "quarantined_records": quarantined_records
        }


    @task
    def validate_silver_transformation(
        bronze_result: dict
    ) -> dict:
        """
        Confirm cleaning, PII masking, deduplication,
        and response quality scoring.
        """

        if bronze_result.get("status") != "passed":
            raise RuntimeError(
                "Bronze stage did not pass."
            )

        report = load_json(
            OUTPUT_DIR / "silver_quality_report.json"
        )

        required_silver_files = [
            PROJECT_ROOT
            / "data/silver/support_tickets_silver.jsonl",

            PROJECT_ROOT
            / "data/silver/chat_messages_silver.jsonl",

            PROJECT_ROOT
            / "data/silver/knowledge_articles_silver.jsonl",

            OUTPUT_DIR / "low_quality_responses.jsonl"
        ]

        for file_path in required_silver_files:
            require_file(file_path)

        quality_distribution = report.get(
            "response_quality",
            {}
        )

        return {
            "stage": "silver",
            "status": "passed",
            "ticket_records": report.get(
                "ticket_records",
                0
            ),
            "low_quality_responses": (
                quality_distribution.get("low", 0)
            ),
            "pii_masking_applied": report.get(
                "pii_masking_applied",
                False
            )
        }


    @task
    def validate_gold_layer(
        silver_result: dict
    ) -> dict:
        """
        Confirm Gold analytics, KPI generation,
        contradiction checks, and RAG corpus creation.
        """

        if silver_result.get("status") != "passed":
            raise RuntimeError(
                "Silver stage did not pass."
            )

        summary = load_json(
            OUTPUT_DIR / "gold_summary_report.json"
        )

        kpis = load_json(
            GOLD_DIR / "support_kpis_gold.json"
        )

        required_gold_files = [
            GOLD_DIR / "support_tickets_gold.jsonl",
            GOLD_DIR / "knowledge_corpus_gold.jsonl",
            GOLD_DIR / "contradiction_alerts.jsonl",
            GOLD_DIR / "support_kpis_gold.json"
        ]

        for file_path in required_gold_files:
            require_file(file_path)

        corpus_documents = summary.get(
            "rag_corpus_documents",
            0
        )

        if corpus_documents <= 0:
            raise ValueError(
                "The Gold RAG corpus contains no documents."
            )

        return {
            "stage": "gold",
            "status": "passed",
            "gold_ticket_records": summary.get(
                "gold_ticket_records",
                0
            ),
            "rag_corpus_documents": corpus_documents,
            "contradiction_alerts": summary.get(
                "contradiction_alerts",
                0
            ),
            "sla_compliance_rate": kpis.get(
                "sla_compliance_rate"
            )
        }


    @task
    def enforce_quality_gate(
        gold_result: dict
    ) -> dict:
        """
        Stop the pipeline when Great Expectations fails.
        """

        if gold_result.get("status") != "passed":
            raise RuntimeError(
                "Gold stage did not pass."
            )

        quality_report = load_json(
            OUTPUT_DIR
            / "great_expectations"
            / "great_expectations_quality_gate.json"
        )

        quality_gate_passed = quality_report.get(
            "quality_gate_passed",
            False
        )

        if not quality_gate_passed:
            raise RuntimeError(
                "Great Expectations quality gate failed. "
                "RAG indexing has been blocked."
            )

        suites = quality_report.get(
            "suites",
            {}
        )

        failed_suites = [
            suite_name
            for suite_name, suite_result in suites.items()
            if not suite_result.get("success", False)
        ]

        if failed_suites:
            raise RuntimeError(
                "Failed expectation suites: "
                + ", ".join(failed_suites)
            )

        return {
            "stage": "quality_gate",
            "status": "passed",
            "great_expectations_version": (
                quality_report.get(
                    "great_expectations_version"
                )
            ),
            "validated_suites": len(suites)
        }


    @task
    def validate_rag_index(
        quality_result: dict
    ) -> dict:
        """
        Confirm Qdrant indexing, hybrid retrieval,
        reranking, and citation output.
        """

        if quality_result.get("status") != "passed":
            raise RuntimeError(
                "Quality gate did not pass."
            )

        manifest = load_json(
            OUTPUT_DIR / "rag_index_manifest.json"
        )

        demonstration = load_json(
            OUTPUT_DIR / "rag_demo_result.json"
        )

        indexed_chunks = manifest.get(
            "indexed_chunks",
            0
        )

        if indexed_chunks <= 0:
            raise ValueError(
                "The RAG index contains no chunks."
            )

        citations = demonstration.get(
            "citations",
            []
        )

        if not citations:
            raise ValueError(
                "The RAG demonstration contains no citations."
            )

        primary_source = demonstration.get(
            "primary_source"
        )

        if not primary_source:
            raise ValueError(
                "The RAG response has no primary source."
            )

        return {
            "stage": "rag",
            "status": "passed",
            "collection_name": manifest.get(
                "collection_name"
            ),
            "indexed_chunks": indexed_chunks,
            "embedding_model": manifest.get(
                "embedding_model"
            ),
            "reranker_model": manifest.get(
                "reranker_model"
            ),
            "primary_source": primary_source,
            "citation_count": len(citations)
        }


    @task
    def validate_lineage_and_audit(
        rag_result: dict
    ) -> dict:
        """
        Confirm OpenLineage START/COMPLETE events
        and query audit records.
        """

        if rag_result.get("status") != "passed":
            raise RuntimeError(
                "RAG stage did not pass."
            )

        lineage_summary = load_json(
            OUTPUT_DIR / "lineage_audit_summary.json"
        )

        lineage_file = require_file(
            LINEAGE_DIR / "openlineage_events.jsonl"
        )

        audit_file = require_file(
            AUDIT_DIR / "rag_query_audit.jsonl"
        )

        start_events = lineage_summary.get(
            "start_events",
            0
        )

        complete_events = lineage_summary.get(
            "complete_events",
            0
        )

        if start_events == 0:
            raise ValueError(
                "No OpenLineage START events were recorded."
            )

        if complete_events == 0:
            raise ValueError(
                "No OpenLineage COMPLETE events were recorded."
            )

        if start_events != complete_events:
            raise ValueError(
                "OpenLineage START and COMPLETE event "
                "counts do not match."
            )

        return {
            "stage": "lineage_and_audit",
            "status": "passed",
            "jobs_recorded": lineage_summary.get(
                "jobs_recorded",
                0
            ),
            "start_events": start_events,
            "complete_events": complete_events,
            "lineage_file": str(lineage_file),
            "audit_file": str(audit_file)
        }


    @task
    def publish_pipeline_summary(
        bronze_result: dict,
        silver_result: dict,
        gold_result: dict,
        quality_result: dict,
        rag_result: dict,
        lineage_result: dict
    ) -> dict:
        """
        Publish the final orchestration result.
        """

        stage_results = [
            bronze_result,
            silver_result,
            gold_result,
            quality_result,
            rag_result,
            lineage_result
        ]

        pipeline_passed = all(
            result.get("status") == "passed"
            for result in stage_results
        )

        final_summary = {
            "dag_id": (
                "customer_support_intelligence_pipeline"
            ),
            "run_completed_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "pipeline_status": (
                "passed"
                if pipeline_passed
                else "failed"
            ),
            "stages": stage_results
        }

        summary_path = (
            OUTPUT_DIR / "airflow_run_summary.json"
        )

        summary_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with summary_path.open(
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(
                final_summary,
                file,
                ensure_ascii=False,
                indent=2
            )

        if not pipeline_passed:
            raise RuntimeError(
                "One or more pipeline stages failed."
            )

        return final_summary


    bronze_result = validate_bronze_ingestion()

    silver_result = validate_silver_transformation(
        bronze_result
    )

    gold_result = validate_gold_layer(
        silver_result
    )

    quality_result = enforce_quality_gate(
        gold_result
    )

    rag_result = validate_rag_index(
        quality_result
    )

    lineage_result = validate_lineage_and_audit(
        rag_result
    )

    publish_pipeline_summary(
        bronze_result=bronze_result,
        silver_result=silver_result,
        gold_result=gold_result,
        quality_result=quality_result,
        rag_result=rag_result,
        lineage_result=lineage_result
    )


customer_support_intelligence_pipeline()
