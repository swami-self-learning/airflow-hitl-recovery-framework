# Airflow Recovery Framework

A configuration-driven Human-in-the-Loop (HITL) recovery framework built with Apache Airflow 3.1.

Teams onboard by providing a YAML configuration. The framework automatically generates team-specific recovery workflows without requiring custom DAG development.

Built for the **Beyond the DAG Hackathon 2026** under the **Keep a Human in the Loop** category.

---

## Category

**Keep a Human in the Loop**

---

## What It Does

The framework executes a pipeline, detects failed outcomes, searches a team-specific Known Error Database (KEDB), and pauses for human decision-making before proceeding.

Available recovery actions:

- Apply Fix and Rerun
- Create Incident
- Manual Investigation

The Manual Investigation path uses Airflow's `HITLEntryOperator` to capture structured investigation findings from engineers.

---

## How It Works

1. A team provides a YAML configuration.
2. The framework dynamically generates an Airflow DAG.
3. The pipeline executes.
4. If the pipeline fails, the team-specific KEDB is searched.
5. A human chooses how recovery should proceed.
6. Airflow executes the selected recovery path.

---

## Architecture

```text
Team Configuration
        ↓
Dynamic DAG Generation
        ↓
Execute Pipeline
        ↓
Success / Failure
      ↙         ↘
     ↓           ↓
 Complete     KEDB Lookup
                   ↓
             Human Decision
          ┌────┼──────┐
          ↓    ↓      ↓
     Apply Fix Incident Investigation
          ↓
    Recovery Complete

## Key Features

- Configuration-driven onboarding
- Dynamic DAG generation
- Team-specific KEDB lookup
- HITLBranchOperator
- HITLEntryOperator
- Simulated remediation workflow
- Mock incident creation
- Multi-team support

---

## Why It Matters

Data teams often build and maintain separate recovery workflows.

This framework provides:

- Standardized recovery processes
- Human governance of remediation actions
- Faster onboarding of new teams
- Reusable recovery patterns
- Reduced DAG development effort
- Consistent incident handling

---

## Airflow Features Used

- Apache Airflow 3.1.1
- TaskFlow API
- Dynamic DAG Generation
- Branching
- HITLBranchOperator
- HITLEntryOperator
- XCom
- Params
- LocalExecutor

---

## How to Run Locally

Clone the repository:

```bash
git clone https://github.com/swami-self-learning/airflow-hitl-recovery-framework.git
cd airflow-hitl-recovery-framework
```

Build and start Airflow:

```bash
docker compose build
docker compose up -d
```

Open Airflow:

```text
http://localhost:8080
```

Retrieve the generated password:

```bash
docker compose exec airflow \
cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Login:

```text
Username: admin
Password: <generated-password>
```

---

## What Was Challenging

The biggest challenge was building a reusable framework instead of a single workflow.

The solution dynamically generates team-specific recovery workflows while keeping recovery actions controlled, configurable, and standardized.

The framework was designed so that teams can onboard through configuration rather than writing custom DAG code.

---

## AI Assistance Disclosure

Microsoft Copilot was used for design discussions, troubleshooting, documentation support, and code suggestions.

All implementation decisions, testing, validation, and final review were performed by the author.

---

## License

This project is licensed under the Apache License 2.0.

See the LICENSE file for details.