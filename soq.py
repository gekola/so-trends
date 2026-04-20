import io
import os
import re
import duckdb
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

YEARS = list(range(2011, 2026))
BASE_PATH = Path("./data")
CACHE_PATH = BASE_PATH / "cache.parquet"

TECHS = [
    "React.js",
    "Angular",
    "Vue.js",
]

NORMALIZATION = {
    "AngularJS": "Angular",
    "Angular.js": "Angular",
    "React": "React.js",
    "ReactJS": "React.js",
    "Vue": "Vue.js",
    "Rails": "Ruby on Rails",
}

# (keyword1, keyword2) — both must appear in column name (case-insensitive)
COLUMN_PATTERNS = [
    ("webframe", "worked"),
    ("misctech", "worked"),
    ("framework", "worked"),
]
# exact column names to also include
COLUMN_EXACT = {"tech_do"}

# 2011-2014: long-form question text that starts a checkbox group
CHECKBOX_QUESTION_PATTERNS = [
    "proficient in",
    "used significantly",
    "languages or tech",
]
# 2015: per-tech columns with explicit names ("Current Lang & Tech: AngularJS", …)
COLUMN_PREFIXES = ["current lang & tech:"]


def find_columns(con):
    cols = [c[0] for c in con.execute("DESCRIBE survey").fetchall()]

    # Structured columns (2016+): short coded names
    structured = [
        c for c in cols
        if c in COLUMN_EXACT
        or any(p1 in c.lower() and p2 in c.lower() for p1, p2 in COLUMN_PATTERNS)
    ]
    if structured:
        return structured

    # 2015: explicit per-tech column names
    prefixed = [c for c in cols if any(c.lower().startswith(p) for p in COLUMN_PREFIXES)]
    if prefixed:
        return prefixed

    # Checkbox-per-column format (2011-2014): named question + auto-named followers
    # DuckDB names unnamed CSV columns as "columnN" (N = 0-based index)
    result = []
    for i, col in enumerate(cols):
        if any(p in col.lower() for p in CHECKBOX_QUESTION_PATTERNS):
            result.append(col)
            j = i + 1
            while j < len(cols) and re.fullmatch(r"column\d+", cols[j]):
                result.append(cols[j])
                j += 1
    return result


def _load_raw_year(file: Path, year: int) -> "pd.DataFrame | None":
    """Return (year, raw_tech, cnt, total) aggregated from one CSV, or None."""
    def _try_load(con, skip=0):
        opts = "AUTO_DETECT=TRUE, ALL_VARCHAR=TRUE" + (f", skip={skip}" if skip else "")
        try:
            con.execute(f"CREATE TABLE survey AS SELECT * FROM read_csv_auto('{file}', {opts})")
        except duckdb.InvalidInputException:
            con.execute(f"CREATE TABLE survey AS SELECT * FROM read_csv_auto('{file}', {opts}, encoding='CP1252')")

    con = duckdb.connect()
    _try_load(con)
    matched_cols = find_columns(con)
    if not matched_cols:
        con = duckdb.connect()
        _try_load(con, skip=1)
        matched_cols = find_columns(con)
    if not matched_cols:
        return None

    col_expr = " || ';' || ".join(f'COALESCE("{c}", \'\')' for c in matched_cols)
    return con.execute(f"""
        SELECT
            {year} AS year,
            TRIM(value) AS raw_tech,
            COUNT(*) AS cnt,
            (SELECT COUNT(*) FROM survey) AS total
        FROM survey,
        UNNEST(STRING_SPLIT(REPLACE({col_expr}, ',', ';'), ';')) AS t(value)
        WHERE TRIM(value) != ''
        GROUP BY raw_tech
    """).df()


def build_cache(years=YEARS, base_path=BASE_PATH, cache_path=CACHE_PATH):
    frames = []
    for year in years:
        file = base_path / str(year) / "survey_results_public.csv"
        if not file.exists():
            continue
        df = _load_raw_year(file, year)
        if df is not None:
            frames.append(df)
            print(f"  {year}: {len(df)} raw tech entries")
    if not frames:
        raise RuntimeError("No data found — nothing to cache")
    combined = pd.concat(frames, ignore_index=True)
    con = duckdb.connect()
    con.register("combined", combined)
    con.execute(f"COPY combined TO '{cache_path}' (FORMAT PARQUET)")
    print(f"Cache written → {cache_path.resolve()}  ({len(combined)} rows)")


def _apply_normalization(df: "pd.DataFrame", techs: list, normalization: dict) -> "pd.DataFrame":
    ci_norm = {k.lower(): v for k, v in normalization.items()}
    ci_norm.update({t.lower(): t for t in techs if t.lower() not in ci_norm})
    df = df.copy()
    df["tech"] = df["raw_tech"].str.lower().map(ci_norm)
    df = df.dropna(subset=["tech"])
    df = df.groupby(["year", "tech"], as_index=False).agg({"cnt": "sum", "total": "first"})
    df["pct"] = df["cnt"] / df["total"] * 100
    return df[["year", "tech", "pct"]]


def load_trends(techs, normalization, years=YEARS, base_path=BASE_PATH, cache_path=CACHE_PATH):
    if cache_path.exists():
        con = duckdb.connect()
        y1, y2 = min(years), max(years)
        df = con.execute(
            f"SELECT * FROM read_parquet('{cache_path}') WHERE year BETWEEN {y1} AND {y2}"
        ).df()
        df = _apply_normalization(df, techs, normalization)
    elif os.environ.get("PRODUCTION"):
        raise RuntimeError(f"Cache not found at {cache_path} and PRODUCTION=1. Run build_cache.py first.")
    else:
        frames = []
        for year in years:
            file = base_path / str(year) / "survey_results_public.csv"
            if not file.exists():
                continue
            raw = _load_raw_year(file, year)
            if raw is None:
                continue
            frames.append(raw)
        if not frames:
            return pd.DataFrame()
        df = _apply_normalization(pd.concat(frames, ignore_index=True), techs, normalization)

    if df.empty:
        return pd.DataFrame()
    pivot = df.pivot(index="year", columns="tech", values="pct").sort_index()
    pivot.index = pivot.index.astype(int)
    return pivot


def plot_trends(pivot, title="Adoption", output=None, colors=None) -> bytes | None:
    cmap = plt.get_cmap("tab10")
    auto = {tech: cmap(i) for i, tech in enumerate(pivot.columns)}
    resolved = {**auto, **(colors or {})}

    years = pivot.index
    year_range = f"{years.min()}–{years.max()}" if len(years) > 1 else str(years[0])

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#161b22")

    for tech in pivot.columns:
        series = pivot[tech].dropna()
        color = resolved[tech]
        ax.plot(series.index, series.values, marker="o", linewidth=2.2,
                markersize=5, label=tech, color=color)
        last_x, last_y = series.index[-1], series.values[-1]
        ax.annotate(tech, xy=(last_x, last_y),
                    xytext=(4, 0), textcoords="offset points",
                    va="center", fontsize=7.5, color=color)

    # Compute right padding dynamically from the longest label
    longest = max((len(t) for t in pivot.columns), default=0)
    right_pad = 0.12 * longest
    ax.set_xlim(pivot.index.min() - 0.2, pivot.index.max() + right_pad)
    ax.set_xticks(pivot.index)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    ax.tick_params(colors="#8b949e")
    ax.spines[:].set_color("#30363d")
    ax.grid(axis="y", color="#21262d", linewidth=0.8)
    ax.set_title(f"Stack Overflow Survey — {title} {year_range}",
                 color="#e6edf3", fontsize=13, pad=14)
    ax.set_xlabel("Year", color="#8b949e", fontsize=10)
    ax.set_ylabel("% of Respondents", color="#8b949e", fontsize=10)
    ax.legend(loc="upper left", framealpha=0.15, labelcolor="#e6edf3",
              fontsize=8, edgecolor="#30363d")

    plt.tight_layout()
    if output is None:
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=220, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    plt.savefig(output, dpi=220, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


if __name__ == "__main__":
    pivot = load_trends(TECHS, NORMALIZATION)
    print(pivot)
    out = Path("trend.png")
    plot_trends(pivot, title="Framework Adoption", output=out)
    print(f"Saved → {out.resolve()}")
