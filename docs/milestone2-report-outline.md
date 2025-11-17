# Milestone 2 – Final Report Outline

This section defines the structure of the **final project report**.
Each heading corresponds to a section you will fill with text, figures, and tables.

---

## 1. Introduction & Motivation (Recruiting + H-1B Context)

* Briefly explain what **H-1B visas** and **Labor Condition Applications (LCAs)** are.
* Describe why H-1B workers matter in the context of **recruiting and labor markets**:

  * Skill shortages, especially in tech.
  * Global competition for talent.
* State the **high-level goal** of the project:

  * Use Hadoop MapReduce on AWS to analyze large-scale 2024 H-1B LCA data.
* Introduce the **key questions** you answer:

  1. How do wages differ between **tech** vs **non-tech** occupations?
  2. For which **occupations** do H-1B workers earn the most in 2024?
  3. Which **states** rely most heavily on H-1B workers?
  4. How often is the **worksite state different from the employer state**?

---

## 2. Dataset Description

* Name and source of the dataset:

  * H1B LCA Disclosure Data 2020–2024 (subset to 2024).
* Original data provider (U.S. Department of Labor OFLC) and Kaggle link (no raw URL in print if not allowed).
* Describe:

  * Number of records (2024 subset).
  * File format (CSV).
  * Key columns used:

    * `CASE_STATUS`, `FULL_TIME_POSITION`, `TOTAL_WORKER_POSITIONS`
    * `SOC_CODE`, `SOC_TITLE`
    * `EMPLOYER_STATE`, `WORKSITE_STATE`
    * `WAGE_RATE_OF_PAY_FROM`, `WAGE_RATE_OF_PAY_TO`, `WAGE_UNIT_OF_PAY`
* Explain any **filters**:

  * Only 2024 cases.
  * Only `CERTIFIED` / `CERTIFIED-WITHDRAWN`.
  * Only `FULL_TIME_POSITION = 'Y'`.
* Explain the **annual wage** computation from wage fields and unit.

---

## 3. Hadoop Cluster Architecture

* Describe the **logical architecture**:

  * Master node: NameNode + ResourceManager.
  * Worker nodes: DataNodes + NodeManagers.
* Show a **simple diagram** of master–worker architecture and data flow.
* Include:

  * Number of nodes, instance types, OS.
  * Network & security group design:

    * SSH from My IP.
    * All TCP within cluster.
* Describe the **software stack**:

  * Java version, Hadoop version.
  * HDFS, YARN, MapReduce components.
* Summarize HDFS layout:

  * `fs.defaultFS = hdfs://master:9000`
  * Replication factor and data directories.
* Mention how the dataset is stored in HDFS:

  * e.g., `/data/h1b/h1b_lca_2024.csv`.

---

## 4. MapReduce Job Design

* Present the **conceptual design** of the main MapReduce job:

  * What each **record** represents (one LCA).
  * How you **filter** rows (status, full-time).
  * How you compute the **annual wage** per worker.
* Explain the **key design** that supports multiple questions:

  * `CAT|TECH` vs `CAT|NON_TECH` → tech vs non-tech wage comparison.
  * `SOC|<SOC_CODE>|<SOC_TITLE>` → occupation-level stats.
  * `STATE|<WORKSITE_STATE>` → state-level reliance on H-1B workers.
  * `FLOW|<EMPLOYER_STATE>-><WORKSITE_STATE>` → remote / cross-state flows.
* Describe the **Mapper**:

  * Input: a line of CSV.
  * Output: `(Text key, AggregateWritable value)` where `AggregateWritable` contains:

    * `wageTimesWorkers = annualWage * workers`
    * `workerCount = number of workers`
* Describe the **Reducer**:

  * Aggregates `sum(wageTimesWorkers)` and `sum(workerCount)`.
  * Computes `avgAnnualWage = sum(wageTimesWorkers) / sum(workerCount)`.
  * Outputs: `KEY` and `TOTAL_WORKERS` and `AVG_ANNUAL_WAGE`.
* Optionally mention why you chose **one job** instead of multiple separate jobs.

---

## 5. Implementation Details

* Programming language: **Java** (Hadoop MapReduce API).
* Package structure / class names:

  * `H1BMultiQuestionAnalysis` (driver).
  * `H1BMapper`, `H1BReducer`, `AggregateWritable`.
* CSV parsing details:

  * Handling quotes and commas inside fields (regex used).
  * Skipping header rows.
* Error handling:

  * Skipping malformed wage or worker fields.
  * Handling missing or unknown wage units.
* Build & deployment:

  * Compilation command using `hadoop classpath`.
  * Jar creation (`h1b-multi-analysis-2024.jar`).
  * Job submission command:

    ```bash
    hadoop jar h1b-multi-analysis-2024.jar \
      H1BMultiQuestionAnalysis \
      /data/h1b/h1b_lca_2024.csv \
      /output/h1b2024_multi
    ```
* Mention any **optimizations**:

  * Custom Writable to keep network traffic small.
  * Filtering in the mapper to reduce reducer load.

---

## 6. Experimental Results

* Briefly describe the **experimental setup**:

  * Number of nodes, instance types.
  * Size of input data (number of records / file size).
* Runtime metrics (if measured):

  * Total job runtime, number of mappers/reducers.
  * HDFS bytes read/written.
* Present **results** for each question using tables and/or plots:

  1. **Tech vs Non-Tech**

     * Table: category, total workers, avg annual wage.
     * Figure: bar chart comparing wages.
  2. **Highest-Paid Occupations**

     * Table: top N SOC titles by average wage (with worker threshold).
     * Figure: horizontal bar chart of top occupations.
  3. **States Relying on H-1B Workers**

     * Table: top N states by total workers (and their avg wages).
     * Figure: bar chart of total workers per state.
  4. **Employer vs Worksite Flows**

     * Table: total same-state vs cross-state workers, percentages.
     * Table or chart: top state→state flows by worker count.
* Point to the **notebook** (`notebooks/H1B_2024_Analysis.ipynb`) as the source of plots and detailed analysis.

---

## 7. Discussion & Recruiting Insights

Interpret the results **in words**, focusing on **recruiting/HR implications**:

* Tech vs Non-Tech:

  * Are tech occupations significantly better paid?
  * What does this say about demand for tech skills?
* Highest-Paid Occupations:

  * Which roles stand out (e.g., certain software, data/AI roles)?
  * How might this guide candidates or recruiters?
* State Reliance:

  * Which states are most reliant on H-1B workers?
  * Are these states known tech hubs or emerging markets?
* Cross-State Flows:

  * Is there a high proportion of cross-state placements?
  * What patterns do you see (e.g., NJ→NY, CA→WA)?
  * How might this relate to remote work or consulting models?
* Reflect on how **big data tools (Hadoop)** enabled this analysis:

  * Scale, parallelism, and ability to process all 2024 cases.

---

## 8. Limitations & Future Work

* **Data limitations:**

  * LCAs are offers, not guaranteed hires.
  * Wage fields may not reflect bonuses or actual pay.
  * Missing or noisy fields (e.g., inconsistent SOC titles).
* **Method limitations:**

  * Simple tech vs non-tech classification (SOC starting with `15-`).
  * Focus on averages; no distributional analysis (e.g., variance, percentiles).
  * One year only (2024), no time trends.
* **Infrastructure limitations:**

  * Small cluster size limits experimentation with bigger jobs.
  * No fault-tolerance experiments or tuning of YARN/HDFS.
* **Future work:**

  * Multi-year trend analysis (2020–2024) of wages and demand.
  * More detailed skill analysis (e.g., using job titles or SOC descriptions).
  * Combine with external data (cost of living by state).
  * Re-implement analysis using **Spark** and compare performance with MapReduce.

