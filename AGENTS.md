# Repository Guidelines

## Project Structure & Module Organization

This repository implements one Hadoop MapReduce analysis job for 2024 H-1B LCA data. The Java driver, mapper, reducer, and custom writable are all in `src/H1BMultiQuestionAnalysis.java`. Cluster examples live in `hadoop-config/`; keep environment-specific hostnames or credentials out of these shared templates. `data/` is for local input files, including the small sample CSV, while full datasets must remain uncommitted. Generated or reference artifacts live in `jar/` and `results/`. Post-processing is in `notebooks/H1B_2024_Analysis.ipynb`, and report material and screenshots are under `docs/`.

## Build, Run, and Development Commands

The project uses Hadoop's installed libraries rather than Maven or Gradle.

```bash
mkdir -p target jar
javac -classpath "$(hadoop classpath)" -d target src/H1BMultiQuestionAnalysis.java
jar -cvf jar/h1b-multi-analysis-2024.jar -C target .
```

Compile the job and package its classes into a deployable JAR. To run it on HDFS data:

```bash
hadoop jar jar/h1b-multi-analysis-2024.jar H1BMultiQuestionAnalysis \
  /data/h1b/h1b_lca_2024.csv /results/h1b2024_multi
hdfs dfs -cat /results/h1b2024_multi/part-r-00000
```

Use `jupyter notebook notebooks/H1B_2024_Analysis.ipynb` to inspect and visualize copied reducer output.

## Coding Style & Naming Conventions

Use Java 11-compatible code and four-space indentation. Follow existing conventions: `UpperCamelCase` classes, `lowerCamelCase` methods and variables, and `UPPER_SNAKE_CASE` constants such as `WORKSITE_STATE_INDEX`. Keep Hadoop key prefixes stable (`CAT|`, `SOC|`, `STATE|`, `FLOW|`) because the notebook consumes them. Prefer small validation helpers and early returns for malformed records, matching the mapper implementation.

## Testing Guidelines

There is currently no automated test framework or coverage gate. For Java changes, compile the JAR and run it against a small DOL-layout CSV before using the full dataset. Confirm output lines remain tab-delimited as `KEY<TAB>TOTAL_WORKERS<TAB>AVG_ANNUAL_WAGE`, and check representative category, state, occupation, and flow records in `part-r-00000`. Document manual checks in the pull request.

## Commit & Pull Request Guidelines

Git history currently contains only `first commit`, so no established commit convention exists. Use short imperative subjects, for example `Handle missing wage units in mapper`. Pull requests should describe the analytical behavior changed, commands used to validate it, input/schema assumptions, and any updated notebook results or documentation. Do not commit full source datasets, Hadoop logs, compiled classes, or local IDE settings.
