# Stack Overflow Trends Agent (soq.py)

## Overview

This agent analyzes Stack Overflow Developer Survey datasets (2020–2025) stored locally in yearly folders (`./2020 ... ./2025`).  
It extracts and compares the popularity of programming languages, frameworks, and other technologies over time using DuckDB.

The main goal is to compute **percentage adoption trends per technology per year**.

---

## Input Data

### Dataset location
Each year is stored as:


./data/2020/survey\_results\_public.csv
./data/2021/survey\_results\_public.csv
...
./data/2025/survey\_results\_public.csv


### Data characteristics
- One row per respondent
- Multi-select columns use `;` separated strings
- Missing values may appear as:
  - `NA`
  - empty strings
  - NULLs
- Schema changes across years (column names differ)

---

## Supported Technology Categories

The agent is designed to analyze:

### Programming languages
- Python
- JavaScript
- TypeScript
- Go
- Rust

### Frameworks & libraries
- React / React.js
- Angular / AngularJS
- Vue / Vue.js
- Django
- Flask
- FastAPI
- Ruby on Rails

---

## Column Detection Strategy

Because Stack Overflow schema changes yearly, the agent:

### 1. Detects relevant columns dynamically
It searches for columns containing:

- `webframe` + `worked`
- `misctech` + `worked`

Examples:
- `WebFrameWorkedWith`
- `WebframeHaveWorkedWith`
- `MiscTechHaveWorkedWith`

### 2. Uses ALL matching columns per year
Instead of relying on a single column, it concatenates all relevant columns.

---

## Data Processing Pipeline

### Step 1: Load CSV (DuckDB)
- Uses `read_csv_auto`
- Forces `ALL_VARCHAR=TRUE` to avoid type inference issues
- Treats `"NA"` as NULL

### Step 2: Normalize schema differences
- Automatically detects framework-related columns
- Combines multiple columns into one unified text stream

### Step 3: Explode multi-select values
- Splits values on `;`
- Trims whitespace
- Filters empty values

### Step 4: Normalize technology names
Examples:
- React → React.js
- AngularJS → Angular
- Vue → Vue.js
- Rails → Ruby on Rails

---

## Metrics Computed

For each year and technology:


popularity % = (number of respondents mentioning tech) / (total respondents)


Output schema:

| year | tech | pct |
|------|------|-----|
| 2020 | React.js | 32.1 |
| 2020 | Django | 18.4 |

---

## Output Format

Final result is pivoted into a time series table:


tech 2020 2021 2022 2023 2024 2025
React.js ... ... ... ... ... ...
Angular ... ... ... ... ... ...
FastAPI ... ... ... ... ... ...


---

## Assumptions

- Each respondent counts equally (no weighting)
- Multi-selection implies independent usage
- Missing frameworks in early years mean 0% adoption
- Schema differences are handled via column discovery

---

## Known Limitations

### 1. Incomplete historical coverage
Some technologies (e.g. FastAPI) may not appear in early surveys.

### 2. Schema drift
Column naming is inconsistent across years and may change unexpectedly.

### 3. Survey bias
Stack Overflow respondents are not representative of the global developer population.

---

## Error Handling Strategy

### CSV parsing issues
- Always uses `ALL_VARCHAR=TRUE`
- Avoids type inference failures (e.g. BOOLEAN vs "NA")

### Missing columns
- Years without framework columns are skipped safely

### Empty datasets
- Safely continues processing without crashing pipeline

---

## Design Philosophy

- Prefer **robustness over strict typing**
- Accept schema inconsistency as normal
- Treat survey data as semi-structured text, not relational data
- Optimize for comparability across time, not exact precision

---

## Possible Extensions

### 1. Category grouping
- Frontend: React, Angular, Vue
- Backend: Django, Flask, FastAPI, Rails

### 2. Trend smoothing
- Rolling averages across years

### 3. Visualization layer
- Line charts per technology
- Heatmaps of adoption

### 4. SQL-only version
- Fully move pipeline into DuckDB views

---

## Execution Environment

- Python 3.10+
- DuckDB
- Pandas
- Local filesystem dataset (no remote API dependency)

---

## Entry Point

Main script:


soq.py


Expected behavior:
- Loads all yearly datasets
- Extracts framework + language usage
- Computes popularity trends
- Outputs pivot table

---
