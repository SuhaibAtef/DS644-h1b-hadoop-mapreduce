# H‑1B 2024 Hadoop MapReduce Analysis

This project builds and runs a Hadoop MapReduce job on an AWS EC2 Hadoop cluster to analyze the **2024 H‑1B Labor Condition Application (LCA)** disclosure data. The job is designed to answer four recruiting‑focused questions from a single pass over the data.

## Research Questions

The MapReduce job produces aggregates that allow us to answer:

1. How do wages differ between **tech** vs **non‑tech** occupations?
2. For which **occupations** do H‑1B workers earn the most in 2024?
3. Which **states** rely most heavily on H‑1B workers?
4. How often is the **worksite state different from the employer state** (cross‑state / remote placements)?

## Repository Layout

- `src/` – Java source code for the MapReduce job (`H1BMultiQuestionAnalysis.java`).
- `data/` – Local sample CSV and documentation about the 2024 H‑1B input data.
- `hadoop-config/` – Example Hadoop configuration files (`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`, `workers`).
- `jar/` – Placeholder for the compiled job JAR to copy to your cluster.
- `results/` – Sample MapReduce output, including `h1b2024_multi/part-r-00000`.
- `notebooks/` – Jupyter notebook for post‑processing the MapReduce output (`H1B_2024_Analysis.ipynb`).
- `docs/` – Milestone write‑ups and screenshots for environment setup and final report.

## MapReduce Job Design

The main driver class is `H1BMultiQuestionAnalysis` (in `src/`). For each certified, full-time `H-1B` 2024 LCA record, the mapper:

- Parses the DOL CSV layout (96 columns).
- Computes an **annual wage** per worker from `WAGE_RATE_OF_PAY_*` and `WAGE_UNIT_OF_PAY`.
- Uses `TOTAL_WORKER_POSITIONS` as the worker count.

It emits four kinds of keys:

- `CAT|TECH` / `CAT|NON_TECH` – tech vs non‑tech wages.
- `SOC|<SOC_CODE>|<SOC_TITLE>` – occupation‑level aggregates.
- `STATE|<WORKSITE_STATE>` – state‑level reliance on H‑1B workers.
- `FLOW|<EMPLOYER_STATE>-><WORKSITE_STATE>` – cross‑state employer→worksite flows.

The reducer aggregates per key:

- `TOTAL_WORKERS = sum(workerCount)`
- `AVG_ANNUAL_WAGE = sum(annualWage * workerCount) / TOTAL_WORKERS`

Each output line has the format:

```text
KEY \t TOTAL_WORKERS \t AVG_ANNUAL_WAGE
```

## Hadoop Cluster Setup (Summary)

The project assumes a **4‑node Hadoop 3.x cluster** on AWS EC2:

- 1 master node (`master`) – NameNode + ResourceManager.
- 3 worker nodes (`worker1`, `worker2`, `worker3`) – DataNodes + NodeManagers.

All nodes run **Ubuntu 22.04 LTS** and **OpenJDK 11**. Example Hadoop configuration files for HDFS and YARN are provided in `hadoop-config/`.

For detailed setup steps (Java, Hadoop install, HDFS/YARN config, and screenshots), see:

- `docs/milestone1-setup.md`

## Building the Job JAR

On a machine with Java and Hadoop installed:

```bash
cd /home/alphani/projects/h1b-hadoop-mapreduce

# Compile
javac -classpath "$(hadoop classpath)" -d target src/H1BMultiQuestionAnalysis.java

# Create JAR
mkdir -p jar
jar -cvf jar/h1b-multi-analysis-2024.jar -C target .
```

Copy `jar/h1b-multi-analysis-2024.jar` to your Hadoop master node.

## Running the MapReduce Job

Assuming the 2024 CSV is stored in HDFS at `/data/h1b/h1b_lca_2024.csv` and you want output under `/results/h1b2024_multi`:

```bash
hadoop jar h1b-multi-analysis-2024.jar \
  H1BMultiQuestionAnalysis \
  /data/h1b/h1b_lca_2024.csv \
  /results/h1b2024_multi

hdfs dfs -ls /results/h1b2024_multi
```

Then copy the output back to your local machine for analysis:

```bash
hdfs dfs -get /results/h1b2024_multi ./results/
```

## Analyzing Results in the Notebook

The notebook `notebooks/H1B_2024_Analysis.ipynb` loads `results/h1b2024_multi/part-r-00000` and:

- Splits keys into categories (`CAT`, `SOC`, `STATE`, `FLOW`).
- Produces tables and plots for all four research questions.

To run the notebook locally (Python 3, `pip`):

```bash
cd notebooks
pip install pandas matplotlib
jupyter notebook H1B_2024_Analysis.ipynb
```

## Data & Licensing

The repository includes only a small sample CSV for illustration. You must download the full 2024 H‑1B LCA disclosure data yourself from the U.S. Department of Labor (OFLC) or an authorized mirror (e.g., Kaggle) and comply with their terms of use.
# H1B Hadoop MapReduce Project

This project analyzes H-1B Labor Condition Application data using a multi-node Hadoop cluster on AWS EC2 and a custom MapReduce job written in Java.

## Components

- **AWS EC2 cluster**: 1 master + 3 workers running Hadoop (HDFS + YARN).
- **MapReduce job**: Java Maven project in `h1b-analysis-mapreduce/` that computes statistics by state and occupation.
- **Dataset**: Public H-1B disclosure data (subset) downloaded from Kaggle / US DoL.

## How to use

- See `docs/milestone1-setup.md` for Hadoop/AWS setup.
- See `docs/milestone2-report-outline.md` for analysis and report structure.
