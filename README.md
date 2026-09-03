# Airflow Recovery Framework

A configuration-driven Human-in-the-Loop (HITL) recovery framework built with Apache Airflow 3.1.

Teams onboard by providing a YAML configuration. The framework automatically generates team-specific recovery workflows without requiring custom DAG development.

## Category

**Keep a Human in the Loop**

## What It Does

The framework executes a pipeline, detects failed outcomes, searches a team-specific Known Error Database (KEDB), and pauses for human decision-making before proceeding.

Available recovery actions:

- Apply Fix and Rerun
- Create Incident
- Manual Investigation

Manual Investigation uses Airflow's HITLEntryOperator to capture investigation findings from engineers.

## How It Works

1. A team provides a YAML configuration.
2. The framework dynamically generates an Airflow DAG.
3. The pipeline executes.
4. If the pipeline fails, the team KEDB is searched.
5. A human chooses how recovery should proceed.
6. Airflow executes the selected recovery path.

## Architecture

```text
Team Configuration
        ↓
Dynamic DAG Generation
        ↓
Execute Pipeline
        ↓
Success / Failure
        ↓
KEDB Lookup
        ↓
Human Decision
    ┌────┼────┐
    ↓    ↓    ↓
 Fix  Incident Investigation
