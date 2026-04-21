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

ENTRY_COLUMN_PATTERNS = [("have", "entry")]

# All keywords in a tuple must appear in the column name (case-insensitive)
COLUMN_PATTERNS = [
    ("webframe", "worked"),
    ("misctech", "worked"),
    ("framework", "worked"),
    ("database", "worked"),
    ("language", "worked"),
    ("platform", "worked"),
    ("toolstech", "worked"),
    ("collabtools", "worked"),
    ("embedded", "worked"),
    ("officestack", "worked"),
    ("ai", "worked"),
    ("opsy",),
    ("versioncontrol",),
]
# exact column names to also include
COLUMN_EXACT = {"tech_do", "IDE", "OpSys"}

# 2011-2014: long-form question text that starts a checkbox group
CHECKBOX_QUESTION_PATTERNS = [
    "proficient in",
    "used significantly",
    "languages or tech",
]
# 2015: per-tech columns with explicit names ("Current Lang & Tech: AngularJS", …)
COLUMN_PREFIXES = ["current lang & tech:"]


def find_entry_columns(con):
    cols = [c[0] for c in con.execute("DESCRIBE survey").fetchall()]
    return [c for c in cols if any(all(kw in c.lower() for kw in pat) for pat in ENTRY_COLUMN_PATTERNS)]


def find_columns(con):
    cols = [c[0] for c in con.execute("DESCRIBE survey").fetchall()]

    # Structured columns (2016+): short coded names
    structured = [
        c for c in cols
        if c in COLUMN_EXACT
        or any(all(kw in c.lower() for kw in pat) for pat in COLUMN_PATTERNS)
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


def _pairs_sql(col_list: list[str], year: int) -> str:
    """SQL fragment: DISTINCT (year, rowid, raw_tech, total) pairs from a column list."""
    col_expr = " || ';' || ".join(f'COALESCE("{c}", \'\')' for c in col_list)
    return f"""
        SELECT DISTINCT
            {year} AS year,
            rowid,
            TRIM(value) AS raw_tech,
            (SELECT COUNT(*) FROM survey) AS total
        FROM survey,
        UNNEST(STRING_SPLIT(REPLACE({col_expr}, ',', ';'), ';')) AS t(value)
        WHERE TRIM(value) != ''
    """


def _load_year(file: Path, year: int) -> "tuple[pd.DataFrame | None, pd.DataFrame | None]":
    """Return (structured_df, entry_df) as unaggregated (year, rowid, raw_tech, total) pairs.

    entry_df contains only pairs from entry columns that are NOT already in structured_df
    (EXCEPT logic), so the two DataFrames never overlap on (rowid, raw_tech).
    """
    def _try_load(con, skip=0):
        opts = "AUTO_DETECT=TRUE, ALL_VARCHAR=TRUE" + (f", skip={skip}" if skip else "")
        try:
            con.execute(f"CREATE TABLE survey AS SELECT * FROM read_csv_auto('{file}', {opts})")
        except duckdb.InvalidInputException:
            con.execute(f"CREATE TABLE survey AS SELECT * FROM read_csv_auto('{file}', {opts}, encoding='CP1252')")

    con = duckdb.connect()
    _try_load(con)
    struct_cols = find_columns(con)
    entry_cols = find_entry_columns(con)
    if not struct_cols and not entry_cols:
        con = duckdb.connect()
        _try_load(con, skip=1)
        struct_cols = find_columns(con)
        entry_cols = find_entry_columns(con)

    struct_df = entry_df = None

    if struct_cols:
        struct_df = con.execute(_pairs_sql(struct_cols, year)).df()

    if entry_cols:
        entry_sql = _pairs_sql(entry_cols, year)
        if struct_cols:
            struct_sql = _pairs_sql(struct_cols, year)
            # keep only pairs not already captured by structured columns
            entry_df = con.execute(f"""
                SELECT year, rowid, raw_tech, total FROM ({entry_sql})
                EXCEPT
                SELECT year, rowid, raw_tech, total FROM ({struct_sql})
            """).df()
        else:
            entry_df = con.execute(entry_sql).df()

    return struct_df, entry_df


def _nest_pairs(df: "pd.DataFrame") -> "pd.DataFrame":
    """Aggregate unaggregated (year, rowid, raw_tech, total, is_entry) into nested format.

    Returns (year, raw_tech, rowids: list[int], total, is_entry) — one row per
    (year, raw_tech, is_entry) with respondent IDs collected into a sorted list.
    Storing nested arrays cuts the cache size ~3× vs flat rows.
    """
    return (df.sort_values("rowid")
              .groupby(["year", "raw_tech", "is_entry"], as_index=False)
              .agg(rowids=("rowid", list), total=("total", "first")))


def build_cache(years=YEARS, base_path=BASE_PATH, cache_path=CACHE_PATH):
    frames = []
    for year in years:
        file = base_path / str(year) / "survey_results_public.csv"
        if not file.exists():
            continue
        sdf, edf = _load_year(file, year)
        if sdf is not None:
            sdf["is_entry"] = False
            frames.append(sdf)
            print(f"  {year}: {len(sdf)} structured pairs")
        if edf is not None:
            edf["is_entry"] = True
            frames.append(edf)
            print(f"  {year}: {len(edf)} write-in pairs")

    if not frames:
        raise RuntimeError("No data found — nothing to cache")

    nested = _nest_pairs(pd.concat(frames, ignore_index=True))
    con = duckdb.connect()
    con.register("nested", nested)
    con.execute(f"COPY nested TO '{cache_path}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22)")
    print(f"  → {cache_path.resolve()}  ({len(nested)} rows)")


def _apply_normalization(df: "pd.DataFrame", techs: list, normalization: dict) -> "pd.DataFrame":
    """Map raw_tech → canonical tech, then count distinct respondents per (year, tech).

    df must have columns: year, raw_tech, rowids (list[int]), total.
    Double-counting is prevented by exploding rowids then nunique — a respondent who
    matched multiple raw aliases for the same tech is counted only once.
    """
    ci_norm = {k.lower(): v for k, v in normalization.items()}
    ci_norm.update({t.lower(): t for t in techs if t.lower() not in ci_norm})
    # Sort longest alias first so more specific patterns win
    patterns = [
        (re.compile(r'\b' + re.escape(k) + r'\b', re.IGNORECASE), v)
        for k, v in sorted(ci_norm.items(), key=lambda x: -len(x[0]))
    ]

    def _match(raw):
        for pat, canonical in patterns:
            if pat.search(raw):
                return canonical
        return None

    df = df.copy()
    df["tech"] = df["raw_tech"].apply(_match)
    df = df.dropna(subset=["tech"])
    df = df.explode("rowids")
    df = df.groupby(["year", "tech"], as_index=False).agg(
        cnt=("rowids", "nunique"), total=("total", "first")
    )
    df["pct"] = df["cnt"] / df["total"] * 100
    return df[["year", "tech", "pct"]]


def load_trends(techs, normalization, years=YEARS, base_path=BASE_PATH, cache_path=CACHE_PATH,
                include_entries=False):
    y1, y2 = min(years), max(years)

    if cache_path.exists():
        con = duckdb.connect()
        entry_filter = "" if include_entries else "AND NOT is_entry"
        df = con.execute(
            f"SELECT * FROM read_parquet('{cache_path}') "
            f"WHERE year BETWEEN {y1} AND {y2} {entry_filter}"
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
            sdf, edf = _load_year(file, year)
            if sdf is not None:
                sdf["is_entry"] = False
                frames.append(sdf)
            if include_entries and edf is not None:
                edf["is_entry"] = True
                frames.append(edf)
        if not frames:
            return pd.DataFrame()
        df = _apply_normalization(_nest_pairs(pd.concat(frames, ignore_index=True)), techs, normalization)

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

    fig, ax = plt.subplots(figsize=(12, 6))
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
        plt.savefig(buf, format="png", dpi=192, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    plt.savefig(output, dpi=220, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


if __name__ == "__main__":
    techs = ["React.js", "Angular", "Vue.js"]
    norm = {"AngularJS": "Angular", "React": "React.js", "Vue": "Vue.js"}
    pivot = load_trends(techs, norm)
    print(pivot)
    out = Path("trend.png")
    plot_trends(pivot, title="Framework Adoption", output=out)
    print(f"Saved → {out.resolve()}")
