from collections import defaultdict
import matplotlib
import matplotlib.pyplot as plt
from flask import Flask, make_response, render_template, request

from soq import YEARS, TECHS, NORMALIZATION, load_trends, plot_trends

app = Flask(__name__)

# Build default per-tech config: colours from tab10, aliases from NORMALIZATION
def _default_tech_config():
    cmap = plt.get_cmap("tab10")
    aliases_for = defaultdict(list)
    for alias, canonical in NORMALIZATION.items():
        aliases_for[canonical].append(alias)
    sorted_techs = sorted(TECHS)  # matches pivot column order (alphabetical)
    return [
        {
            "name": tech,
            "aliases": aliases_for.get(tech, []),
            "color": matplotlib.colors.to_hex(cmap(i)),
            "enabled": True,
        }
        for i, tech in enumerate(sorted_techs)
    ]


DEFAULT_TECH_CONFIG = _default_tech_config()


def _parse_body(body):
    """Extract (techs, normalization, years, colors) from a POST JSON body."""
    entries = [t for t in body.get("techs", []) if t.get("enabled")]
    techs = [t["name"] for t in entries]
    norm = {
        alias.strip(): t["name"]
        for t in entries
        for alias in t.get("aliases", [])
        if alias.strip()
    }
    colors = {t["name"]: t["color"] for t in entries if t.get("color")}
    year_from = int(body.get("year_from", min(YEARS)))
    year_to = int(body.get("year_to", max(YEARS)))
    years = list(range(year_from, year_to + 1))
    return techs, norm, years, colors


@app.get("/")
def index():
    return render_template(
        "index.html",
        years=list(range(min(YEARS), max(YEARS) + 1)),
        default_config=DEFAULT_TECH_CONFIG,
        default_year_from=2016,
        default_year_to=max(YEARS),
    )


@app.post("/chart")
def chart():
    body = request.get_json()
    techs, norm, years, colors = _parse_body(body)
    pivot = load_trends(techs, norm, years=years)
    if pivot.empty:
        return ("No data for the selected parameters.", 422)
    png = plot_trends(pivot, title=body.get("title", "Framework Adoption"), colors=colors)
    resp = make_response(png)
    resp.headers["Content-Type"] = "image/png"
    return resp


@app.post("/table")
def table():
    body = request.get_json()
    techs, norm, years, _ = _parse_body(body)
    pivot = load_trends(techs, norm, years=years)
    if pivot.empty:
        return ("<p class='empty'>No data for the selected parameters.</p>", 200)
    html = (
        pivot.rename_axis(None)
             .rename_axis(None, axis=1)
             .round(1)
             .to_html(
                 classes="data-table",
                 float_format=lambda x: f"{x:.1f}%",
                 border=0,
                 na_rep="—",
             )
    )
    return html
