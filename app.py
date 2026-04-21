from flask import Flask, make_response, render_template, request

from soq import YEARS, load_trends, plot_trends

app = Flask(__name__)

# Matches PALETTE in index.html — used to assign default tech colors server-side.
PALETTE = [
    "#58a6ff",  # blue
    "#f78166",  # coral
    "#3fb950",  # green
    "#d2a8ff",  # lavender
    "#ffa657",  # orange
    "#79c0ff",  # sky
    "#ff7b72",  # salmon
    "#56d364",  # lime
    "#e3b341",  # gold
    "#bc8cff",  # violet
]

_DEFAULT_TECHS = {
    "React.js": ["React", "ReactJS"],
    "Angular":  ["AngularJS", "Angular.js"],
    "Vue.js":   ["Vue"],
}

DEFAULT_TECH_CONFIG = [
    {
        "name": tech,
        "aliases": aliases,
        "color": PALETTE[i % len(PALETTE)],
        "enabled": True,
    }
    for i, (tech, aliases) in enumerate(sorted(_DEFAULT_TECHS.items()))
]


def _parse_body(body):
    """Extract (techs, normalization, years, colors, include_entries) from a POST JSON body."""
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
    include_entries = bool(body.get("include_entries", False))
    return techs, norm, years, colors, include_entries


@app.get("/")
def index():
    return render_template(
        "index.html",
        years=list(range(min(YEARS), max(YEARS) + 1)),
        default_config=DEFAULT_TECH_CONFIG,
        palette=PALETTE,
        default_year_from=2016,
        default_year_to=max(YEARS),
    )


@app.post("/chart")
def chart():
    body = request.get_json()
    techs, norm, years, colors, include_entries = _parse_body(body)
    pivot = load_trends(techs, norm, years=years, include_entries=include_entries)
    if pivot.empty:
        return ("No data for the selected parameters.", 422)
    png = plot_trends(pivot, title=body.get("title", "Framework Adoption"), colors=colors)
    resp = make_response(png)
    resp.headers["Content-Type"] = "image/png"
    return resp


@app.post("/table")
def table():
    body = request.get_json()
    techs, norm, years, _, include_entries = _parse_body(body)
    pivot = load_trends(techs, norm, years=years, include_entries=include_entries)
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
