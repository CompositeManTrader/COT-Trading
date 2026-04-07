"""
COT Dashboard — Commitment of Traders Visualizer  v2
Fuente de datos: CFTC (Commodity Futures Trading Commission)

Fixes v2:
- Prefijos URL correctos: fut_xls (Legacy) / fut_disagg_txt / fut_fin_txt
- Esquemas de columnas distintos por reporte (Legacy / Disaggregated / Financial TFF)
- Búsqueda de mercados por tokens (tolerante a variaciones de nombre)
- Diagnóstico interactivo: muestra mercados disponibles si no hay match
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests, zipfile, io
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────────
st.set_page_config(page_title="COT Dashboard", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")

# ─── CUSTOM CSS ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background: #0d1117; color: #e6edf3; }
[data-testid="stSidebar"] { background: #161b22 !important; border-right: 1px solid #21262d; }
[data-testid="stSidebar"] label {
    color: #8b949e !important; font-family: 'IBM Plex Mono', monospace;
    font-size: 0.75rem; letter-spacing: 0.08em; text-transform: uppercase; }
.metric-card {
    background: #161b22; border: 1px solid #21262d; border-radius: 6px;
    padding: 16px 20px; text-align: center; font-family: 'IBM Plex Mono', monospace; }
.metric-label { font-size:.68rem; color:#8b949e; text-transform:uppercase; letter-spacing:.1em; margin-bottom:6px; }
.metric-value { font-size:1.35rem; font-weight:600; line-height:1; }
.metric-delta { font-size:.73rem; margin-top:4px; }
.positive { color:#3fb950; } .negative { color:#f85149; } .neutral { color:#58a6ff; }
.main-title {
    font-family:'IBM Plex Mono',monospace; font-size:1.55rem; font-weight:600;
    color:#e6edf3; letter-spacing:-.02em;
    border-bottom:1px solid #21262d; padding-bottom:12px; margin-bottom:4px; }
.subtitle { font-size:.78rem; color:#8b949e; font-family:'IBM Plex Mono',monospace; margin-bottom:20px; }
.section-header {
    font-family:'IBM Plex Mono',monospace; font-size:.68rem; text-transform:uppercase;
    letter-spacing:.15em; color:#58a6ff; border-left:3px solid #58a6ff;
    padding-left:10px; margin:22px 0 12px 0; }
.info-box {
    background:#161b22; border:1px solid #21262d; border-left:3px solid #58a6ff;
    border-radius:4px; padding:12px 16px; font-size:.83rem; color:#8b949e; line-height:1.6; }
.info-box strong { color:#e6edf3; }
.warn-box {
    background:#2d1f00; border:1px solid #bb8009; border-left:3px solid #d29922;
    border-radius:4px; padding:12px 16px; font-size:.83rem; color:#d29922; line-height:1.6; }
.signal-bull { background:#0d4429; color:#3fb950; border:1px solid #3fb950; border-radius:20px; padding:4px 14px; font-family:'IBM Plex Mono',monospace; font-size:.8rem; font-weight:600; display:inline-block; }
.signal-bear { background:#3d1a1a; color:#f85149; border:1px solid #f85149; border-radius:20px; padding:4px 14px; font-family:'IBM Plex Mono',monospace; font-size:.8rem; font-weight:600; display:inline-block; }
.signal-neutral { background:#1c2a3a; color:#58a6ff; border:1px solid #58a6ff; border-radius:20px; padding:4px 14px; font-family:'IBM Plex Mono',monospace; font-size:.8rem; font-weight:600; display:inline-block; }
</style>
""", unsafe_allow_html=True)

# ─── REPORT CONFIG ───────────────────────────────────────────────────────────────
# Cada reporte tiene prefijo de URL distinto + esquema de columnas distinto
REPORTS = {
    "Legacy — Futures Only": {
        "prefix":    "fut_xls",          # <-- URL correcta para Legacy
        "long_nc":   "Noncommercial_Positions_Long_All",
        "short_nc":  "Noncommercial_Positions_Short_All",
        "long_cm":   "Commercial_Positions_Long_All",
        "short_cm":  "Commercial_Positions_Short_All",
        "long_nr":   "Nonrept_Positions_Long_All",
        "short_nr":  "Nonrept_Positions_Short_All",
        "nc_label":  "Non-Commercial (Specs)",
        "cm_label":  "Commercial (Hedgers)",
        "description": "Reporte clásico. Cubre <strong>Forex, Commodities y Bonos</strong>.",
    },
    "Legacy — Combined (Futures+Options)": {
        "prefix":    "com_xls",
        "long_nc":   "Noncommercial_Positions_Long_All",
        "short_nc":  "Noncommercial_Positions_Short_All",
        "long_cm":   "Commercial_Positions_Long_All",
        "short_cm":  "Commercial_Positions_Short_All",
        "long_nr":   "Nonrept_Positions_Long_All",
        "short_nr":  "Nonrept_Positions_Short_All",
        "nc_label":  "Non-Commercial (Specs)",
        "cm_label":  "Commercial (Hedgers)",
        "description": "Legacy combinado (futuros + opciones). Mismo universo que Legacy.",
    },
    "Disaggregated — Futures Only": {
        "prefix":    "fut_disagg_txt",
        "long_nc":   "M_Money_Positions_Long_All",
        "short_nc":  "M_Money_Positions_Short_All",
        "long_cm":   "Prod_Merc_Positions_Long_All",
        "short_cm":  "Prod_Merc_Positions_Short_All",
        "long_nr":   "NonRept_Positions_Long_All",
        "short_nr":  "NonRept_Positions_Short_All",
        "nc_label":  "Managed Money (Specs)",
        "cm_label":  "Prod/Merchants (Hedgers)",
        "description": "Desglosa más categorías. Ideal para <strong>energía, metales y granos</strong>.",
    },
    "Financial TFF — Futures Only": {
        "prefix":    "fut_fin_txt",
        "long_nc":   "Lev_Money_Positions_Long_All",
        "short_nc":  "Lev_Money_Positions_Short_All",
        "long_cm":   "Asset_Mgr_Positions_Long_All",
        "short_cm":  "Asset_Mgr_Positions_Short_All",
        "long_nr":   "Other_Rept_Positions_Long_All",
        "short_nr":  "Other_Rept_Positions_Short_All",
        "nc_label":  "Leveraged Money (Specs)",
        "cm_label":  "Asset Managers",
        "description": "Específico para <strong>índices bursátiles y tasas de interés</strong>.",
    },
}

MARKETS = {
    "— FOREX —": None,
    "EUR/USD  ·  Euro FX":          "EURO FX",
    "GBP/USD  ·  British Pound":    "BRITISH POUND",
    "JPY/USD  ·  Japanese Yen":     "JAPANESE YEN",
    "CHF/USD  ·  Swiss Franc":      "SWISS FRANC",
    "AUD/USD  ·  Australian $":     "AUSTRALIAN DOLLAR",
    "CAD/USD  ·  Canadian $":       "CANADIAN DOLLAR",
    "NZD/USD  ·  New Zealand $":    "NEW ZEALAND DOLLAR",
    "MXN/USD  ·  Peso Mexicano":    "MEXICAN PESO",
    "— ÍNDICES BURSÁTILES —": None,
    "S&P 500":                      "S&P 500 STOCK INDEX",
    "Nasdaq-100":                   "NASDAQ-100 STOCK INDEX",
    "Dow Jones Industrial":         "DOW JONES INDUSTRIAL AVG",
    "Russell 2000":                 "RUSSELL 2000",
    "— COMMODITIES ENERGÍA —": None,
    "Crude Oil (WTI)":              "CRUDE OIL, LIGHT SWEET",
    "Natural Gas":                  "NATURAL GAS",
    "— METALES —": None,
    "Gold":                         "GOLD",
    "Silver":                       "SILVER",
    "Copper":                       "COPPER",
    "— GRANOS —": None,
    "Corn":                         "CORN",
    "Wheat (CBOT)":                 "WHEAT",
    "Soybeans":                     "SOYBEANS",
    "— TASAS DE INTERÉS —": None,
    "10Y T-Note":                   "10-YEAR U.S. TREASURY NOTES",
    "2Y T-Note":                    "2-YEAR U.S. TREASURY NOTES",
    "30Y T-Bond":                   "U.S. TREASURY BONDS",
}

CFTC_BASE = "https://www.cftc.gov/files/dea/history"

# ─── DATA LAYER ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_year(year: int, prefix: str) -> pd.DataFrame:
    url = f"{CFTC_BASE}/{prefix}_{year}.zip"
    try:
        r = requests.get(url, timeout=40)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            fname = sorted(z.namelist())[-1]
            with z.open(fname) as f:
                return pd.read_csv(f, low_memory=False)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_cot(years: list, prefix: str) -> pd.DataFrame:
    frames = [_fetch_year(y, prefix) for y in years]
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _name_col(df):
    for c in ["Market_and_Exchange_Names", "Contract_Market_Name", "Market and Exchange Names"]:
        if c in df.columns:
            return c
    return None


def _date_col(df):
    for c in ["Report_Date_as_MM_DD_YYYY", "Report_Date_as_YYYY-MM-DD",
              "As_of_Date_In_Form_YYMMDD", "Report Date"]:
        if c in df.columns:
            return c
    return None


def available_markets(df):
    nc = _name_col(df)
    return sorted(df[nc].dropna().unique().tolist()) if nc and not df.empty else []


def parse_cot(df: pd.DataFrame, market_search: str, rcfg: dict) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    nc, dc = _name_col(df), _date_col(df)
    if not nc or not dc:
        return pd.DataFrame()

    # Búsqueda por tokens independientes (tolerante a "EURO FX - CHICAGO MERCANTILE EXCHANGE")
    tokens = [t for t in market_search.upper().split() if len(t) > 1]
    mask = df[nc].str.upper().apply(
        lambda x: all(t in str(x) for t in tokens) if pd.notna(x) else False)
    df_m = df[mask].copy()
    if df_m.empty:
        return pd.DataFrame()

    df_m["Date"] = pd.to_datetime(df_m[dc], errors="coerce")
    df_m = df_m.dropna(subset=["Date"]).sort_values("Date").drop_duplicates("Date")

    def _n(col):
        return pd.to_numeric(df_m.get(col, pd.Series(np.nan, index=df_m.index)), errors="coerce")

    df_m["OI"]       = _n("Open_Interest_All")
    df_m["NC_Long"]  = _n(rcfg["long_nc"])
    df_m["NC_Short"] = _n(rcfg["short_nc"])
    df_m["CM_Long"]  = _n(rcfg["long_cm"])
    df_m["CM_Short"] = _n(rcfg["short_cm"])
    df_m["NR_Long"]  = _n(rcfg["long_nr"])
    df_m["NR_Short"] = _n(rcfg["short_nr"])
    df_m["Net_NC"]   = df_m["NC_Long"]  - df_m["NC_Short"]
    df_m["Net_CM"]   = df_m["CM_Long"]  - df_m["CM_Short"]
    df_m["Net_NR"]   = df_m["NR_Long"]  - df_m["NR_Short"]

    keep = ["Date","OI","NC_Long","NC_Short","Net_NC",
            "CM_Long","CM_Short","Net_CM","NR_Long","NR_Short","Net_NR"]
    return df_m[keep].reset_index(drop=True)


def cot_index(series: pd.Series, window: int = 52) -> pd.Series:
    mn  = series.rolling(window, min_periods=5).min()
    mx  = series.rolling(window, min_periods=5).max()
    rng = mx - mn
    idx = np.where(rng != 0, (series - mn) / rng * 100, 50.0)
    return pd.Series(idx, index=series.index)

# ─── CHARTS ──────────────────────────────────────────────────────────────────────
_T = dict(
    paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
    font=dict(family="IBM Plex Mono", color="#8b949e", size=11),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#21262d", borderwidth=1,
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=10, r=10, t=36, b=10), hovermode="x unified",
)
_AX = dict(gridcolor="#21262d", zeroline=False, showline=True, linecolor="#21262d")


def _apply(fig, **kw):
    fig.update_layout(**_T, **kw)
    fig.update_xaxes(**_AX)
    fig.update_yaxes(**_AX)
    return fig


def chart_net_cot(df, window, nc_label, cm_label):
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.04,
        row_heights=[0.38, 0.33, 0.29],
        subplot_titles=[f"Posición Neta — {nc_label}",
                        f"Posición Neta — {cm_label}",
                        "COT Index (especuladores)"])
    nc = df["Net_NC"]
    fig.add_trace(go.Bar(x=df["Date"], y=nc, name=nc_label, opacity=0.8,
        marker_color=["#3fb950" if v >= 0 else "#f85149" for v in nc]), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=nc.rolling(4).mean(),
        name="MA 4s", line=dict(color="#f0e68c", width=1.8)), row=1, col=1)

    cm = df["Net_CM"]
    fig.add_trace(go.Bar(x=df["Date"], y=cm, name=cm_label, opacity=0.8,
        marker_color=["#79c0ff" if v >= 0 else "#ffa657" for v in cm]), row=2, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=cm.rolling(4).mean(),
        name="MA 4s (H)", line=dict(color="#d2a8ff", width=1.8)), row=2, col=1)

    ci = cot_index(nc, window=window)
    fig.add_hrect(y0=75, y1=100, fillcolor="#3fb950", opacity=0.07,
                  layer="below", line_width=0, row=3, col=1)
    fig.add_hrect(y0=0, y1=25,   fillcolor="#f85149", opacity=0.07,
                  layer="below", line_width=0, row=3, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=ci, name=f"COT Idx ({window}w)",
        line=dict(color="#58a6ff", width=2),
        fill="tozeroy", fillcolor="rgba(88,166,255,0.07)"), row=3, col=1)
    for lvl, color in [(75,"#3fb950"),(50,"#8b949e"),(25,"#f85149")]:
        fig.add_hline(y=lvl, line=dict(color=color, dash="dash", width=1), row=3, col=1)

    _apply(fig, height=620)
    fig.update_yaxes(title_text="Contratos", row=1, col=1, **_AX)
    fig.update_yaxes(title_text="Contratos", row=2, col=1, **_AX)
    fig.update_yaxes(title_text="0 – 100",   row=3, col=1, range=[0,100], **_AX)
    for ann in fig.layout.annotations:
        ann.font.update(family="IBM Plex Mono", size=11, color="#8b949e")
    return fig


def chart_gross(df, nc_label, cm_label):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["NC_Long"],  name=f"{nc_label} Long",  line=dict(color="#3fb950", width=1.5)))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["NC_Short"], name=f"{nc_label} Short", line=dict(color="#f85149", width=1.5)))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["CM_Long"],  name=f"{cm_label} Long",  line=dict(color="#79c0ff", width=1.5, dash="dot")))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["CM_Short"], name=f"{cm_label} Short", line=dict(color="#ffa657", width=1.5, dash="dot")))
    return _apply(fig, yaxis_title="Contratos")


def chart_oi(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["OI"], name="Open Interest",
        fill="tozeroy", line=dict(color="#58a6ff", width=2),
        fillcolor="rgba(88,166,255,0.09)"))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["OI"].rolling(13).mean(),
        name="MA 13w", line=dict(color="#ffa657", width=1.5, dash="dash")))
    return _apply(fig, yaxis_title="Contratos")


def chart_donut(df):
    lat = df.iloc[-1]
    fig = go.Figure(go.Pie(
        labels=["NC Long","NC Short","CM Long","CM Short"],
        values=[lat["NC_Long"], lat["NC_Short"], lat["CM_Long"], lat["CM_Short"]],
        hole=0.55,
        marker=dict(colors=["#3fb950","#f85149","#79c0ff","#ffa657"],
                    line=dict(color="#0d1117", width=2)),
        textfont=dict(family="IBM Plex Mono", size=11, color="#e6edf3"),
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} contratos<br>%{percent}<extra></extra>",
    ))
    return _apply(fig, height=320, showlegend=True)

# ─── SIGNALS ─────────────────────────────────────────────────────────────────────
def signals(df, window):
    if len(df) < 6:
        return {}
    lat, prv = df.iloc[-1], df.iloc[-6]
    ci = cot_index(df["Net_NC"], window).iloc[-1]
    oi_chg = (lat["OI"] - prv["OI"]) / prv["OI"] * 100 if prv["OI"] else 0
    sig, cls = (("ALCISTA","signal-bull") if ci >= 75
                else ("BAJISTA","signal-bear") if ci <= 25
                else ("NEUTRAL","signal-neutral"))
    return dict(
        signal=sig, signal_cls=cls,
        nc_net=lat["Net_NC"], cm_net=lat["Net_CM"],
        nc_trend="↑ Subiendo" if lat["Net_NC"] > prv["Net_NC"] else "↓ Bajando",
        cm_trend="↑ Subiendo" if lat["Net_CM"] > prv["Net_CM"] else "↓ Bajando",
        cot_index=ci, oi=lat["OI"], oi_change=oi_chg,
        last_date=lat["Date"].strftime("%d %b %Y"),
    )

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("<div style='font-family:IBM Plex Mono;font-size:1.05rem;font-weight:600;color:#58a6ff;padding:8px 0 16px'>⬡ COT Dashboard</div>", unsafe_allow_html=True)
    market_key = st.selectbox("MERCADO", list(MARKETS.keys()),
                              index=list(MARKETS.keys()).index("EUR/USD  ·  Euro FX"))
    report_key = st.selectbox("TIPO DE REPORTE", list(REPORTS.keys()),
                              help="Legacy → Forex/Commodities | Financial TFF → Índices/Tasas")
    years_back = st.slider("AÑOS DE HISTORIA", 1, 10, 3)
    cot_win    = st.slider("VENTANA COT INDEX (semanas)", 13, 156, 52, step=13)
    st.markdown("---")
    rcfg = REPORTS[report_key]
    st.markdown(f"<div class='info-box'>{rcfg['description']}<br><br>"
                f"<strong>Speculators:</strong> {rcfg['nc_label']}<br>"
                f"<strong>Hedgers:</strong> {rcfg['cm_label']}</div>", unsafe_allow_html=True)
    st.markdown("<div style='font-family:IBM Plex Mono;font-size:.65rem;color:#8b949e;margin-top:12px'>Datos: CFTC · Actualización viernes</div>", unsafe_allow_html=True)

# ─── MAIN ────────────────────────────────────────────────────────────────────────
market_search = MARKETS.get(market_key)
if market_search is None:
    st.info("Selecciona un mercado del sidebar para comenzar.")
    st.stop()

rcfg = REPORTS[report_key]
current_year = datetime.now().year
years = list(range(current_year - years_back + 1, current_year + 1))

st.markdown(f"<div class='main-title'>📊 Commitment of Traders</div>", unsafe_allow_html=True)
st.markdown(f"<div class='subtitle'>CFTC · {market_key.strip()} · {report_key} · COT window {cot_win}w</div>",
            unsafe_allow_html=True)

with st.spinner("⟳  Descargando datos del CFTC..."):
    raw = load_cot(years, rcfg["prefix"])
    df  = parse_cot(raw, market_search, rcfg)

# ── Error con diagnóstico ──────────────────────────────────────────────────────
if df.empty:
    st.markdown(f"""<div class='warn-box'>
    ⚠️ <strong>Sin datos</strong> para <em>{market_key}</em> con <em>{report_key}</em>.<br><br>
    <strong>Guía de reporte por activo:</strong><br>
    • <strong>Forex, Gold, Silver, Oil, Bonds</strong> → Legacy — Futures Only<br>
    • <strong>S&P 500, Nasdaq, Dow, Russell</strong> → Financial TFF — Futures Only<br>
    • <strong>Corn, Wheat, Soybeans, Copper</strong> → Disaggregated — Futures Only
    </div>""", unsafe_allow_html=True)

    if not raw.empty:
        avail = available_markets(raw)
        if avail:
            with st.expander(f"🔍  Explorar los {len(avail)} mercados disponibles en este reporte", expanded=True):
                q = st.text_input("Buscar:", placeholder="e.g. EURO, GOLD, S&P...")
                hits = [m for m in avail if q.upper() in m.upper()] if q else avail
                for m in hits[:120]:
                    st.text(m)
    st.stop()

# ── KPIs ──────────────────────────────────────────────────────────────────────
s = signals(df, cot_win)
st.markdown("<div class='section-header'>Resumen de Posicionamiento</div>", unsafe_allow_html=True)
c1,c2,c3,c4,c5 = st.columns(5)

with c1:
    st.markdown(f"""<div class='metric-card'>
        <div class='metric-label'>Señal COT</div>
        <div style='padding:4px 0'><span class='{s["signal_cls"]}'>{s["signal"]}</span></div>
        <div class='metric-delta neutral'>{s["last_date"]}</div></div>""", unsafe_allow_html=True)
with c2:
    v=s["nc_net"]; cls="positive" if v>0 else "negative"
    st.markdown(f"""<div class='metric-card'>
        <div class='metric-label'>Spec Net</div>
        <div class='metric-value {cls}'>{v:+,.0f}</div>
        <div class='metric-delta neutral'>{s["nc_trend"]}</div></div>""", unsafe_allow_html=True)
with c3:
    v=s["cm_net"]; cls="positive" if v>0 else "negative"
    st.markdown(f"""<div class='metric-card'>
        <div class='metric-label'>Hedger Net</div>
        <div class='metric-value {cls}'>{v:+,.0f}</div>
        <div class='metric-delta neutral'>{s["cm_trend"]}</div></div>""", unsafe_allow_html=True)
with c4:
    idx=s["cot_index"]; cls="positive" if idx>66 else ("negative" if idx<33 else "neutral")
    st.markdown(f"""<div class='metric-card'>
        <div class='metric-label'>COT Index</div>
        <div class='metric-value {cls}'>{idx:.1f}</div>
        <div class='metric-delta neutral'>Ventana {cot_win}w</div></div>""", unsafe_allow_html=True)
with c5:
    oi=s["oi"]; oic=s["oi_change"]; cls="positive" if oic>0 else "negative"
    st.markdown(f"""<div class='metric-card'>
        <div class='metric-label'>Open Interest</div>
        <div class='metric-value neutral'>{oi:,.0f}</div>
        <div class='metric-delta {cls}'>{oic:+.1f}% (5s)</div></div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Charts ────────────────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>Posiciones Netas & COT Index</div>", unsafe_allow_html=True)
st.plotly_chart(chart_net_cot(df, cot_win, rcfg["nc_label"], rcfg["cm_label"]),
                use_container_width=True, config={"displayModeBar": False})

col_l, col_r = st.columns([2, 1])
with col_l:
    st.markdown("<div class='section-header'>Posiciones Brutas</div>", unsafe_allow_html=True)
    st.plotly_chart(chart_gross(df, rcfg["nc_label"], rcfg["cm_label"]),
                    use_container_width=True, config={"displayModeBar": False})
with col_r:
    st.markdown("<div class='section-header'>Distribución Actual</div>", unsafe_allow_html=True)
    st.plotly_chart(chart_donut(df), use_container_width=True, config={"displayModeBar": False})

st.markdown("<div class='section-header'>Open Interest</div>", unsafe_allow_html=True)
st.plotly_chart(chart_oi(df), use_container_width=True, config={"displayModeBar": False})

with st.expander("📋  Datos crudos"):
    num_cols = df.select_dtypes("number").columns.tolist()
    st.dataframe(df.sort_values("Date", ascending=False)
                   .style.format({c:"{:,.0f}" for c in num_cols}),
                 use_container_width=True)

st.markdown("---")
st.markdown("""<div style='text-align:center;font-family:IBM Plex Mono;font-size:.68rem;color:#8b949e'>
Datos públicos CFTC · Actualización semanal (viernes) · Solo informativo, no es asesoramiento financiero
</div>""", unsafe_allow_html=True)
