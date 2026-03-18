# 🔎 Data Quality AI Monitor

> AI-powered **data quality monitoring** platform for BigQuery pipelines that uses Gemini LLM to detect anomalies, schema drift, null explosions, and generate natural-language remediation recommendations.

![Python](https://img.shields.io/badge/Python-3.11-blue) ![BigQuery](https://img.shields.io/badge/BigQuery-Data%20Quality-green) ![Gemini](https://img.shields.io/badge/Gemini-LLM-orange) ![Airflow](https://img.shields.io/badge/Airflow-2.8-red)

## 🎯 Problem Statement

Data pipelines silently produce bad data — null explosions, schema changes, statistical distribution shifts — that corrupt downstream dashboards and ML models. Traditional rule-based DQ tools require manual threshold tuning. This platform uses **Gemini to intelligently interpret anomalies** and prescribe fixes in plain English.

## 🏗️ System Architecture

```
BigQuery Tables
    │
    ▼
[DQ Scanner] ──► Profile: nulls, cardinality, distributions, schema
    │
    ▼
[Anomaly Detector] ──► Z-score + IQR + rule-based checks
    │
    ▼
[Gemini Analyzer] ──► LLM interprets anomalies + root cause analysis
    │
    ▼
[Alert Router] ──► Slack / PagerDuty / BigQuery audit table
```

## ✨ Key Features

- **Schema drift detection** — alerts on column additions, type changes, renames
- **Statistical profiling** — tracks nulls, cardinality, min/max, percentiles per column
- **Anomaly scoring** — Z-score + IQR outlier detection with configurable sensitivity
- **Gemini-powered root cause analysis** — LLM explains why data looks anomalous
- **Remediation recommendations** — actionable SQL fixes suggested automatically
- **Airflow integration** — DQ checks as DAG sensors before downstream tasks
- **Slack/PagerDuty alerts** — rich notifications with anomaly context
- **BigQuery audit trail** — full history of DQ scores per table per run

## 📁 Repository Structure

```
src/
├── scanner/
│   ├── bq_profiler.py          # BigQuery table statistical profiling
│   ├── schema_drift.py         # Schema change detection
│   └── null_monitor.py        # Null rate tracking and alerting
├── detection/
│   ├── anomaly_scorer.py       # Statistical anomaly detection
│   ├── threshold_manager.py    # Dynamic threshold learning
│   └── rule_engine.py         # Custom DQ rule evaluation
├── ai_analysis/
│   ├── gemini_analyzer.py      # LLM root cause analysis
│   └── remediation_engine.py  # SQL fix recommendation
├── alerting/
│   ├── slack_notifier.py       # Slack rich message alerts
│   └── pagerduty_client.py    # PagerDuty incident creation
└── airflow/
    └── dq_sensor.py           # Airflow sensor for DQ gates
```

## 🚀 Quick Start

```bash
pip install -r requirements.txt

# Configure monitored tables
cat > config/tables.yaml << EOF
tables:
  - dataset: prod_analytics
    table: daily_orders
    checks: [nulls, schema, distribution]
    sensitivity: high
  - dataset: prod_analytics
    table: user_events
    checks: [nulls, cardinality]
    sensitivity: medium
EOF

# Run DQ scan
python src/scanner/bq_profiler.py \
  --project your-project \
  --config config/tables.yaml

# Get AI analysis of anomalies
python src/ai_analysis/gemini_analyzer.py \
  --scan_results output/scan_results.json
```

## 📊 DQ Check Types

| Check | Description | Alert Trigger |
|-------|-------------|---------------|
| Null Rate | % null values per column | > threshold or +20% change |
| Row Count | Table row count | < 80% or > 120% of baseline |
| Schema Drift | Column additions/removals/type changes | Any structural change |
| Distribution | Value distribution shift | KS-test p-value < 0.05 |
| Freshness | Time since last update | > configured SLA |
| Cardinality | Distinct value count change | > 50% change |

## 📄 License

MIT License
