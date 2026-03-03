# Data Lineage Utility (DLU)

## Overview

The **Data Lineage Utility (DLU)** is a **layered, extensible framework** for discovering, analyzing, and visualizing **end‑to‑end data lineage** across modern data platforms.

It is intentionally designed to support:

- **Creating Data Lineage** (static code analysis)
- **Business‑context enrichment** using LLMs
- **Deterministic validation** before automation

The architecture prioritizes:

> Correctness first → Automation second → Intelligence last

---

## Why This Exists

Modern data ecosystems distribute logic across:

- SQL (Snowflake, dbt, views, stored procedures)
- Python (ETL scripts, notebooks)
- YAML & configuration files (Airflow, dbt, orchestration)
- Excel (manually curated lineage, governance inputs)

Traditional lineage tools fail to answer questions such as:

- *What breaks if I change this column?*
- *Which downstream datasets depend on this business rule?*
- *How does data flow through non‑SQL transformations?*
- *What lineage exists before deployment?*

DLU closes these gaps by:

- Separating **lineage ingestion** from **lineage interpretation**
- Supporting **multiple lineage sources**
- Enabling **LLM‑assisted reasoning** without sacrificing determinism

---

## High‑Level Architecture

```
Source (Excel | Snowflake | Git Repository)
        ↓
Lineage Loaders (Source‑specific)
        ↓
Normalized Lineage Model (Nodes & Edges)
        ↓
Graph Builder (NetworkX DAG)
        ↓
Analysis & Visualization (Streamlit / D3 / Neo4j)
```

---

## Core Design Principles

### 1. Excel‑First Validation
- Deterministic and reviewable inputs
- Zero credentials or runtime dependencies
- Stabilizes graph logic before automation
- Mirrors real‑world governance workflows

### 2. Loader Isolation
- Each source ingests lineage independently
- All outputs conform to a **common schema**
- New sources plug in without refactoring graph or UI layers

### 3. Stable Graph Layer
- Graph logic never changes based on lineage source
- Enables upstream, downstream, and impact analysis
- Designed for column‑level and temporal lineage extensions

---

# Lineage Extraction Approaches

DLU follows a **hybrid strategy** composed of two complementary layers.

---

## Approach 1: LLM Route (Intelligence Layer)

This layer focuses on **understanding intent and business logic**, especially where deterministic parsers fall short.

### What It Solves
- Non‑SQL transformations (Python, Shell, config logic)
- Embedded business logic (e.g., `CASE WHEN`, flags, rules)
- Cross‑file or commented dependencies
- High‑level explanations and documentation

### How It Works

#### Step 1: Code Ingestion
- Load repositories using tools such as **LangChain**
- Break files into semantic chunks suitable for LLM processing

#### Step 2: Prompt‑Driven Interpretation

> "Scan these SQL files and list all source tables and final destinations in JSON format. Identify hard‑coded business rules such as CASE WHEN logic."

#### Step 3: Structured Output
- Enforce **strict JSON schemas**
- Validate outputs before downstream merge
- Automatically retry on off‑schema responses

---

## Approach 2: SQLLineage Route (Deterministic Layer) -> Future Scope

This layer provides **technical ground truth** for SQL‑based lineage.

### How It Works

```bash
pip install sqllineage
sqllineage -f your_script.sql -g --level column
```

---

## Summary

DLU is **not a single‑purpose script**. It is a **foundational lineage framework** built for correctness, extensibility, and future intelligence—without compromising determinism or trust.

# How to run file:
- Create Data Lineage: `python llm\process_llm_output.py`
- Run Data Lineage Utility: `streamlit run web_app\data_lineage_app.py`

