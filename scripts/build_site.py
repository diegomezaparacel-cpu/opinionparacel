from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl
import yaml

from paracel_monitor.pipeline import summarize, to_dashboard_json


HTML_TEMPLATE = r'''<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Paracel · Opinion Monitor</title>

  <link href="https://unpkg.com/tabulator-tables@6.2.1/dist/css/tabulator.min.css" rel="stylesheet">
  <script src="https://cdn.plot.ly/plotly-2.30.0.min.js"></script>
  <script src="https://unpkg.com/tabulator-tables@6.2.1/dist/js/tabulator.min.js"></script>

  <style>
    :root { --bg:#0b1220; --card:#0f1b33; --text:#e6eefc; --muted:#a7b3c7; --accent:#22c55e; --border:#1e2b49;}
    body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background:var(--bg); color:var(--text); }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 18px; }
    h1 { font-size: 20px; margin: 0 0 10px 0; }
    .sub { color: var(--muted); margin: 0 0 18px 0; font-size: 13px; }
    .grid { display: grid; grid-template-columns: 1fr; gap: 14px; }
    @media(min-width: 980px){ .grid { grid-template-columns: 1fr 1fr; } }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 14px; box-shadow: 0 8px 24px rgba(0,0,0,.25); }
    .controls { display:grid; grid-template-columns: 1fr; gap: 10px; }
    @media(min-width: 980px){ .controls { grid-template-columns: 1fr 1fr 1fr 1fr; } }
    label { font-size: 12px; color: var(--muted); display:block; margin-bottom: 6px; }
    select, input { width:100%; padding:10px 10px; border-radius: 10px; border:1px solid var(--border); background:#0a1630; color: var(--text); }
    .kpis { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    @media(min-width: 980px){ .kpis { grid-template-columns: 1fr 1fr 1fr 1fr; } }
    .kpi { background:#0a1630; border:1px solid var(--border); border-radius: 12px; padding: 12px; }
    .kpi .v { font-size: 20px; font-weight: 650; }
    .kpi .t { font-size: 12px; color: var(--muted); }
    a { color: var(--accent); text-decoration: none; }
    .footer { margin-top: 16px; color: var(--muted); font-size: 12px; }
    .tabulator { border-radius: 12px; border: 1px solid var(--border); background: #0a1630; }
    .tabulator .tabulator-header { background: #0b1a34; border-bottom: 1px solid var(--border); color: var(--text); }
    .tabulator .tabulator-row { background: #0a1630; color: var(--text); border-bottom: 1px solid rgba(30,43,73,.35); }
    .tabulator .tabulator-row:hover { background: #0c1e3b; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Paracel · Opinion Monitor</h1>
    <p class="sub">Tablero estático (GitHub Pages) con filtros client-side. Dataset actualizado automáticamente por GitHub Actions.</p>

    <div class="card">
      <div class="kpis">
        <div class="kpi"><div class="v" id="kpi_total">–</div><div class="t">Menciones (en tablero)</div></div>
        <div class="kpi"><div class="v" id="kpi_sources">–</div><div class="t">Fuentes</div></div>
        <div class="kpi"><div class="v" id="kpi_domains">–</div><div class="t">Dominios</div></div>
        <div class="kpi"><div class="v" id="kpi_range">–</div><div class="t">Rango de fechas</div></div>
      </div>
    </div>

    <div class="card">
      <div class="controls">
        <div>
          <label for="f_source">Fuente</label>
          <select id="f_source"><option value="">Todas</option></select>
        </div>
        <div>
          <label for="f_sent">Sentimiento</label>
          <select id="f_sent"><option value="">Todos</option></select>
        </div>
        <div>
          <label for="f_topic">Tópico</label>
          <select id="f_topic"><option value="">Todos</option></select>
        </div>
        <div>
          <label for="f_text">Búsqueda (título/snippet)</label>
          <input id="f_text" type="text" placeholder="palabra clave..."/>
        </div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div id="chart_time" style="height:320px;"></div>
      </div>
      <div class="card">
        <div id="chart_sent" style="height:320px;"></div>
      </div>
    </div>

    <div class="card">
      <div id="table"></div>
      <div class="footer">
        Consejo: use filtros y luego exporte CSV desde el menú del navegador si lo requiere. Las URLs se abren en una nueva pestaña.
      </div>
    </div>

    <div class="footer">
      Última actualización: <span id="last_update">–</span>
    </div>
  </div>

<script>
async function loadJSON(path){
  const r = await fetch(path, {cache: "no-store"});
  if(!r.ok) throw new Error("No se pudo cargar " + path);
  return await r.json();
}

function uniq(arr){ return Array.from(new Set(arr)); }

function fmtDateISO(s){
  if(!s) return null;
  const d = new Date(s);
  if(Number.isNaN(d.getTime())) return null;
  return d;
}

function dayKey(d){
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth()+1).padStart(2,'0');
  const dd = String(d.getUTCDate()).padStart(2,'0');
  return `${yyyy}-${mm}-${dd}`;
}

function buildTimeSeries(rows){
  const m = new Map();
  rows.forEach(r=>{
    const d = fmtDateISO(r.published_at);
    if(!d) return;
    const k = dayKey(d);
    m.set(k, (m.get(k)||0)+1);
  });
  const keys = Array.from(m.keys()).sort();
  const y = keys.map(k=>m.get(k));
  return {x: keys, y: y};
}

function buildCounts(rows, field){
  const m = new Map();
  rows.forEach(r=>{
    const v = r[field];
    if(!v) return;
    m.set(v, (m.get(v)||0)+1);
  });
  const keys = Array.from(m.keys()).sort((a,b)=> (m.get(b)-m.get(a)));
  return {labels: keys, values: keys.map(k=>m.get(k))};
}

function getTopicsFlatten(rows){
  const out = [];
  rows.forEach(r=>{
    (r.topics||[]).forEach(t=> out.push(t));
  });
  return out;
}

function updateSelect(sel, values){
  const cur = sel.value;
  sel.innerHTML = '<option value="">Todos</option>';
  values.forEach(v=>{
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    sel.appendChild(opt);
  });
  sel.value = cur;
}

function applyFilters(allRows){
  const src = document.getElementById("f_source").value;
  const sent = document.getElementById("f_sent").value;
  const topic = document.getElementById("f_topic").value;
  const text = document.getElementById("f_text").value.trim().toLowerCase();

  return allRows.filter(r=>{
    if(src && r.source !== src) return false;
    if(sent && r.sentiment_label !== sent) return false;
    if(topic){
      const ts = r.topics || [];
      if(!ts.includes(topic)) return false;
    }
    if(text){
      const hay = ((r.best_title||"") + " " + (r.snippet||"")).toLowerCase();
      if(!hay.includes(text)) return false;
    }
    return true;
  });
}

function updateKPIs(rows){
  document.getElementById("kpi_total").textContent = rows.length.toLocaleString();

  const sources = uniq(rows.map(r=>r.source).filter(Boolean));
  const domains = uniq(rows.map(r=>r.domain).filter(Boolean));
  document.getElementById("kpi_sources").textContent = sources.length.toLocaleString();
  document.getElementById("kpi_domains").textContent = domains.length.toLocaleString();

  const dates = rows.map(r=>fmtDateISO(r.published_at)).filter(Boolean).sort((a,b)=>a-b);
  if(dates.length){
    const a = dates[0].toISOString().slice(0,10);
    const b = dates[dates.length-1].toISOString().slice(0,10);
    document.getElementById("kpi_range").textContent = `${a} a ${b}`;
  } else {
    document.getElementById("kpi_range").textContent = "–";
  }
}

function drawCharts(rows){
  const ts = buildTimeSeries(rows);
  Plotly.newPlot("chart_time", [{
    x: ts.x, y: ts.y, type:"scatter", mode:"lines+markers", name:"Menciones"
  }], {
    margin:{l:40,r:10,t:20,b:40},
    paper_bgcolor:"rgba(0,0,0,0)",
    plot_bgcolor:"rgba(0,0,0,0)",
    font:{color:"#e6eefc"},
    xaxis:{title:"Día", gridcolor:"rgba(30,43,73,.35)"},
    yaxis:{title:"n", gridcolor:"rgba(30,43,73,.35)"}
  }, {displayModeBar:false});

  const sc = buildCounts(rows, "sentiment_label");
  Plotly.newPlot("chart_sent", [{
    x: sc.labels, y: sc.values, type:"bar", name:"Sentimiento"
  }], {
    margin:{l:40,r:10,t:20,b:40},
    paper_bgcolor:"rgba(0,0,0,0)",
    plot_bgcolor:"rgba(0,0,0,0)",
    font:{color:"#e6eefc"},
    xaxis:{title:"Etiqueta", gridcolor:"rgba(30,43,73,.35)"},
    yaxis:{title:"n", gridcolor:"rgba(30,43,73,.35)"}
  }, {displayModeBar:false});
}

function buildTable(rows){
  const table = new Tabulator("#table", {
    data: rows,
    layout:"fitColumns",
    pagination:"local",
    paginationSize: 20,
    movableColumns:true,
    columns:[
      {title:"Fecha", field:"published_at", width:120, formatter:(c)=> (c.getValue()||"").slice(0,10)},
      {title:"Fuente", field:"source", width:140},
      {title:"Dominio", field:"domain", width:200},
      {title:"Sent", field:"sentiment_label", width:110},
      {title:"Score", field:"sentiment_score", width:100, formatter:(c)=> {
        const v = c.getValue();
        if(v===null || v===undefined) return "";
        return Number(v).toFixed(3);
      }},
      {title:"Tópicos", field:"topics", width:240, formatter:(c)=> (c.getValue()||[]).join(", ")},
      {title:"Título", field:"best_title", minWidth:320, formatter:(c)=> {
        const r = c.getRow().getData();
        const t = c.getValue() || "";
        const u = r.url || "#";
        const a = document.createElement("a");
        a.href = u;
        a.target = "_blank";
        a.rel = "noopener noreferrer";
        a.textContent = t;
        return a;
      }},
      {title:"Snippet", field:"snippet", minWidth:380},
      {title:"Query", field:"query", minWidth:260},
    ],
  });
  return table;
}

async function main(){
  const meta = await loadJSON("data/meta.json");
  document.getElementById("last_update").textContent = meta.generated_at || "–";

  const allRows = await loadJSON("data/latest.json");

  const sources = uniq(allRows.map(r=>r.source).filter(Boolean)).sort();
  const sents = uniq(allRows.map(r=>r.sentiment_label).filter(Boolean)).sort();
  const topics = uniq(getTopicsFlatten(allRows).filter(Boolean)).sort();

  updateSelect(document.getElementById("f_source"), sources);
  updateSelect(document.getElementById("f_sent"), sents);
  updateSelect(document.getElementById("f_topic"), topics);

  let filtered = applyFilters(allRows);
  updateKPIs(filtered);
  drawCharts(filtered);
  let table = buildTable(filtered);

  const rerender = ()=>{
    filtered = applyFilters(allRows);
    updateKPIs(filtered);
    drawCharts(filtered);
    table.replaceData(filtered);
  };

  document.getElementById("f_source").addEventListener("change", rerender);
  document.getElementById("f_sent").addEventListener("change", rerender);
  document.getElementById("f_topic").addEventListener("change", rerender);
  document.getElementById("f_text").addEventListener("input", ()=> {
    window.clearTimeout(window.__t);
    window.__t = window.setTimeout(rerender, 220);
  });
}

main().catch(err=>{
  document.body.innerHTML = "<pre style='color:#fff;padding:16px;'>" + String(err) + "</pre>";
});
</script>
</body>
</html>'''


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/config.yml")
    ap.add_argument("--in-parquet", default="data/paracel_mentions.parquet")
    ap.add_argument("--docs-dir", default="docs")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    max_items = int(cfg["project"].get("max_items_dashboard", 800))

    df = pl.read_parquet(args.in_parquet)

    docs = Path(args.docs_dir)
    (docs / "data").mkdir(parents=True, exist_ok=True)

    payload = to_dashboard_json(df, max_items=max_items)
    (docs / "data" / "latest.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    sums = summarize(df)
    for k, sdf in sums.items():
        (docs / "data" / f"summary__{k}.json").write_text(json.dumps(sdf.to_dicts(), ensure_ascii=False), encoding="utf-8")

    meta = {
        "generated_at": pl.datetime.now(time_zone="UTC").cast(pl.Utf8).to_list()[0],
        "n_total": int(df.height),
        "n_dashboard": int(min(df.height, max_items)),
    }
    (docs / "data" / "meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")

    (docs / "index.html").write_text(HTML_TEMPLATE, encoding="utf-8")


if __name__ == "__main__":
    main()
