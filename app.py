"""
COT Dashboard — Commitment of Traders Visualizer  v4
Fuente: CFTC (Commodity Futures Trading Commission)

Fixes v4 (definitivos):
- URL base correcta: https://cftc.gov/ (sin www)
- Prefijos correctos verificados via cot-reports package:
    Legacy fut     → deacot{year}.zip
    Legacy futopt  → deahistfo{year}.zip
    Disaggregated  → fut_disagg_txt_{year}.zip
    Financial TFF  → fut_fin_txt_{year}.zip
- Columnas correctas por tipo de reporte
- Mínimo 2 años garantizado
- Log de descarga siempre visible
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

st.set_page_config(page_title="COT Dashboard", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
.stApp{background:#0d1117;color:#e6edf3;}
[data-testid="stSidebar"]{background:#161b22!important;border-right:1px solid #21262d;}
[data-testid="stSidebar"] label{color:#8b949e!important;font-family:'IBM Plex Mono',monospace;font-size:.75rem;letter-spacing:.08em;text-transform:uppercase;}
.metric-card{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:16px 20px;text-align:center;font-family:'IBM Plex Mono',monospace;}
.metric-label{font-size:.68rem;color:#8b949e;text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px;}
.metric-value{font-size:1.35rem;font-weight:600;line-height:1;}
.metric-delta{font-size:.73rem;margin-top:4px;}
.positive{color:#3fb950;}.negative{color:#f85149;}.neutral{color:#58a6ff;}
.main-title{font-family:'IBM Plex Mono',monospace;font-size:1.55rem;font-weight:600;color:#e6edf3;letter-spacing:-.02em;border-bottom:1px solid #21262d;padding-bottom:12px;margin-bottom:4px;}
.subtitle{font-size:.78rem;color:#8b949e;font-family:'IBM Plex Mono',monospace;margin-bottom:20px;}
.section-header{font-family:'IBM Plex Mono',monospace;font-size:.68rem;text-transform:uppercase;letter-spacing:.15em;color:#58a6ff;border-left:3px solid #58a6ff;padding-left:10px;margin:22px 0 12px 0;}
.info-box{background:#161b22;border:1px solid #21262d;border-left:3px solid #58a6ff;border-radius:4px;padding:12px 16px;font-size:.83rem;color:#8b949e;line-height:1.6;}
.info-box strong{color:#e6edf3;}
.signal-bull{background:#0d4429;color:#3fb950;border:1px solid #3fb950;border-radius:20px;padding:4px 14px;font-family:'IBM Plex Mono',monospace;font-size:.8rem;font-weight:600;display:inline-block;}
.signal-bear{background:#3d1a1a;color:#f85149;border:1px solid #f85149;border-radius:20px;padding:4px 14px;font-family:'IBM Plex Mono',monospace;font-size:.8rem;font-weight:600;display:inline-block;}
.signal-neutral{background:#1c2a3a;color:#58a6ff;border:1px solid #58a6ff;border-radius:20px;padding:4px 14px;font-family:'IBM Plex Mono',monospace;font-size:.8rem;font-weight:600;display:inline-block;}
</style>
""", unsafe_allow_html=True)

# ─── CONFIG ───────────────────────────────────────────────────────────────────────
# URL base SIN www — verificado via cot-reports package source
CFTC_BASE = "https://cftc.gov/files/dea/history"

REPORTS = {
    "Legacy — Futures Only": {
        # URL real: https://cftc.gov/files/dea/history/deacot{year}.zip
        "prefix":   "deacot",
        "long_nc":  "Noncommercial_Positions_Long_All",
        "short_nc": "Noncommercial_Positions_Short_All",
        "long_cm":  "Commercial_Positions_Long_All",
        "short_cm": "Commercial_Positions_Short_All",
        "long_nr":  "Nonrept_Positions_Long_All",
        "short_nr": "Nonrept_Positions_Short_All",
        "nc_label": "Non-Commercial (Specs)",
        "cm_label": "Commercial (Hedgers)",
        "description": "Reporte clásico. Cubre <strong>Forex, Commodities y Bonos</strong>.",
    },
    "Legacy — Combined (Futures+Options)": {
        # URL real: https://cftc.gov/files/dea/history/deahistfo{year}.zip
        "prefix":   "deahistfo",
        "long_nc":  "Noncommercial_Positions_Long_All",
        "short_nc": "Noncommercial_Positions_Short_All",
        "long_cm":  "Commercial_Positions_Long_All",
        "short_cm": "Commercial_Positions_Short_All",
        "long_nr":  "Nonrept_Positions_Long_All",
        "short_nr": "Nonrept_Positions_Short_All",
        "nc_label": "Non-Commercial (Specs)",
        "cm_label": "Commercial (Hedgers)",
        "description": "Legacy combinado futuros + opciones.",
    },
    "Disaggregated — Futures Only": {
        # URL real: https://cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip
        "prefix":   "fut_disagg_txt_",
        "long_nc":  "M_Money_Positions_Long_All",
        "short_nc": "M_Money_Positions_Short_All",
        "long_cm":  "Prod_Merc_Positions_Long_All",
        "short_cm": "Prod_Merc_Positions_Short_All",
        "long_nr":  "NonRept_Positions_Long_All",
        "short_nr": "NonRept_Positions_Short_All",
        "nc_label": "Managed Money (Specs)",
        "cm_label": "Prod/Merchants (Hedgers)",
        "description": "Desglosa más categorías. Ideal para <strong>energía, metales y granos</strong>.",
    },
    "Financial TFF — Futures Only": {
        # URL real: https://cftc.gov/files/dea/history/fut_fin_txt_{year}.zip
        "prefix":   "fut_fin_txt_",
        "long_nc":  "Lev_Money_Positions_Long_All",
        "short_nc": "Lev_Money_Positions_Short_All",
        "long_cm":  "Asset_Mgr_Positions_Long_All",
        "short_cm": "Asset_Mgr_Positions_Short_All",
        "long_nr":  "Other_Rept_Positions_Long_All",
        "short_nr": "Other_Rept_Positions_Short_All",
        "nc_label": "Leveraged Money (Specs)",
        "cm_label": "Asset Managers",
        "description": "Específico para <strong>índices bursátiles y tasas de interés</strong>.",
    },
}

# Nombres EXACTOS verificados contra datos reales del CFTC (deacot*.zip)
MARKETS = {
    "— FOREX (Legacy — Futures Only) —": None,
    "EUR/USD  ·  Euro FX":       "EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "GBP/USD  ·  British Pound": "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
    "JPY/USD  ·  Japanese Yen":  "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "CHF/USD  ·  Swiss Franc":   "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
    "AUD/USD  ·  Australian $":  "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "CAD/USD  ·  Canadian $":    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "NZD/USD  ·  New Zealand $": "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "MXN/USD  ·  Peso Mexicano": "MEXICAN PESO - CHICAGO MERCANTILE EXCHANGE",
    "— ÍNDICES (Financial TFF — Futures Only) —": None,
    "S&P 500":                   "S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
    "Nasdaq-100":                "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE",
    "Dow Jones Industrial":      "DOW JONES INDUSTRIAL AVG- x $5 - CHICAGO BOARD OF TRADE",
    "Russell 2000":              "E-MINI RUSSELL 2000 INDEX - CHICAGO MERCANTILE EXCHANGE",
    "— ENERGÍA (Legacy — Futures Only) —": None,
    "Crude Oil (WTI)":           "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
    "Natural Gas":               "NATURAL GAS - NEW YORK MERCANTILE EXCHANGE",
    "— METALES (Legacy — Futures Only) —": None,
    "Gold":                      "GOLD - COMMODITY EXCHANGE INC.",
    "Silver":                    "SILVER - COMMODITY EXCHANGE INC.",
    "Copper":                    "COPPER- #1 - COMMODITY EXCHANGE INC.",
    "— GRANOS (Disaggregated — Futures Only) —": None,
    "Corn":                      "CORN - CHICAGO BOARD OF TRADE",
    "Wheat (SRW)":               "WHEAT-SRW - CHICAGO BOARD OF TRADE",
    "Soybeans":                  "SOYBEANS - CHICAGO BOARD OF TRADE",
    "— TASAS (Legacy — Futures Only) —": None,
    "10Y T-Note":                "10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE",
    "2Y T-Note":                 "2-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE",
    "30Y T-Bond":                "U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE",
}

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/zip,*/*",
}

# ─── DATA ────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_year(year: int, prefix: str) -> tuple:
    url = f"{CFTC_BASE}/{prefix}{year}.zip"
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        if r.status_code == 404:
            return pd.DataFrame(), f"⚠️ {year}: 404 — {url}"
        if r.status_code != 200:
            return pd.DataFrame(), f"❌ {year}: HTTP {r.status_code} — {url}"
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            fname = sorted(z.namelist())[-1]
            with z.open(fname) as f:
                df = pd.read_csv(f, low_memory=False)
        return df, f"✅ {year}: {len(df):,} filas — {url}"
    except requests.exceptions.ConnectionError as e:
        return pd.DataFrame(), f"❌ {year}: Conexión fallida — {str(e)[:100]}"
    except zipfile.BadZipFile:
        return pd.DataFrame(), f"❌ {year}: ZIP inválido (posible HTML de error) — {url}"
    except Exception as e:
        return pd.DataFrame(), f"❌ {year}: {type(e).__name__}: {str(e)[:100]}"


def load_cot(years: list, prefix: str) -> tuple:
    frames, logs = [], []
    for y in years:
        df, msg = fetch_year(y, prefix)
        logs.append(msg)
        if not df.empty:
            frames.append(df)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return combined, logs


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


def available_markets(df) -> list:
    nc = _name_col(df)
    return sorted(df[nc].dropna().unique().tolist()) if nc and not df.empty else []


def parse_cot(df: pd.DataFrame, market_search: str, rcfg: dict) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    nc, dc = _name_col(df), _date_col(df)
    if not nc or not dc:
        return pd.DataFrame()
    # Exact match (case-insensitive) — names verified against real CFTC data
    mask = df[nc].str.upper() == market_search.upper()
    # Fallback to token match if exact match returns nothing
    if mask.sum() == 0:
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

    df_m["OI"]      = _n("Open_Interest_All")
    df_m["NC_Long"] = _n(rcfg["long_nc"]);  df_m["NC_Short"] = _n(rcfg["short_nc"])
    df_m["CM_Long"] = _n(rcfg["long_cm"]);  df_m["CM_Short"] = _n(rcfg["short_cm"])
    df_m["NR_Long"] = _n(rcfg["long_nr"]);  df_m["NR_Short"] = _n(rcfg["short_nr"])
    df_m["Net_NC"]  = df_m["NC_Long"] - df_m["NC_Short"]
    df_m["Net_CM"]  = df_m["CM_Long"] - df_m["CM_Short"]
    df_m["Net_NR"]  = df_m["NR_Long"] - df_m["NR_Short"]
    return df_m[["Date","OI","NC_Long","NC_Short","Net_NC",
                 "CM_Long","CM_Short","Net_CM","NR_Long","NR_Short","Net_NR"]].reset_index(drop=True)


def cot_index(series: pd.Series, window: int = 52) -> pd.Series:
    mn = series.rolling(window, min_periods=5).min()
    mx = series.rolling(window, min_periods=5).max()
    rng = mx - mn
    return pd.Series(np.where(rng != 0, (series - mn) / rng * 100, 50.0), index=series.index)

# ─── CHARTS ──────────────────────────────────────────────────────────────────────
_T = dict(paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
          font=dict(family="IBM Plex Mono", color="#8b949e", size=11),
          legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#21262d", borderwidth=1,
                      orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
          margin=dict(l=10, r=10, t=36, b=10), hovermode="x unified")
_AX = dict(gridcolor="#21262d", zeroline=False, showline=True, linecolor="#21262d")

def _theme(fig, **kw):
    fig.update_layout(**_T, **kw); fig.update_xaxes(**_AX); fig.update_yaxes(**_AX)
    return fig

def chart_net_cot(df, window, nc_label, cm_label):
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.04,
        row_heights=[0.38, 0.33, 0.29],
        subplot_titles=[f"Posición Neta — {nc_label}", f"Posición Neta — {cm_label}", "COT Index"])
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
    fig.add_hrect(y0=75, y1=100, fillcolor="#3fb950", opacity=0.07, layer="below", line_width=0, row=3, col=1)
    fig.add_hrect(y0=0,  y1=25,  fillcolor="#f85149", opacity=0.07, layer="below", line_width=0, row=3, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=ci, name=f"COT Idx ({window}w)",
        line=dict(color="#58a6ff", width=2), fill="tozeroy", fillcolor="rgba(88,166,255,0.07)"), row=3, col=1)
    for lvl, color in [(75,"#3fb950"),(50,"#8b949e"),(25,"#f85149")]:
        fig.add_hline(y=lvl, line=dict(color=color, dash="dash", width=1), row=3, col=1)
    _theme(fig, height=620)
    fig.update_yaxes(title_text="Contratos", row=1, col=1, **_AX)
    fig.update_yaxes(title_text="Contratos", row=2, col=1, **_AX)
    fig.update_yaxes(title_text="0–100", row=3, col=1, range=[0,100], **_AX)
    for ann in fig.layout.annotations:
        ann.font.update(family="IBM Plex Mono", size=11, color="#8b949e")
    return fig

def chart_gross(df, nc_label, cm_label):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["NC_Long"],  name=f"{nc_label} Long",  line=dict(color="#3fb950", width=1.5)))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["NC_Short"], name=f"{nc_label} Short", line=dict(color="#f85149", width=1.5)))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["CM_Long"],  name=f"{cm_label} Long",  line=dict(color="#79c0ff", width=1.5, dash="dot")))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["CM_Short"], name=f"{cm_label} Short", line=dict(color="#ffa657", width=1.5, dash="dot")))
    return _theme(fig, yaxis_title="Contratos")

def chart_oi(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["OI"], name="Open Interest",
        fill="tozeroy", line=dict(color="#58a6ff", width=2), fillcolor="rgba(88,166,255,0.09)"))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["OI"].rolling(13).mean(),
        name="MA 13w", line=dict(color="#ffa657", width=1.5, dash="dash")))
    return _theme(fig, yaxis_title="Contratos")

def chart_donut(df):
    lat = df.iloc[-1]
    fig = go.Figure(go.Pie(
        labels=["NC Long","NC Short","CM Long","CM Short"],
        values=[lat["NC_Long"], lat["NC_Short"], lat["CM_Long"], lat["CM_Short"]],
        hole=0.55,
        marker=dict(colors=["#3fb950","#f85149","#79c0ff","#ffa657"],
                    line=dict(color="#0d1117", width=2)),
        textfont=dict(family="IBM Plex Mono", size=11, color="#e6edf3"),
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} contratos<br>%{percent}<extra></extra>"))
    return _theme(fig, height=320, showlegend=True)

def signals(df, window):
    if len(df) < 6:
        return {}
    lat, prv = df.iloc[-1], df.iloc[-6]
    ci = cot_index(df["Net_NC"], window).iloc[-1]
    oi_chg = (lat["OI"] - prv["OI"]) / prv["OI"] * 100 if prv["OI"] else 0
    sig, cls = (("ALCISTA","signal-bull") if ci >= 75 else
                ("BAJISTA","signal-bear") if ci <= 25 else ("NEUTRAL","signal-neutral"))
    return dict(signal=sig, signal_cls=cls,
                nc_net=lat["Net_NC"], cm_net=lat["Net_CM"],
                nc_trend="↑ Subiendo" if lat["Net_NC"] > prv["Net_NC"] else "↓ Bajando",
                cm_trend="↑ Subiendo" if lat["Net_CM"] > prv["Net_CM"] else "↓ Bajando",
                cot_index=ci, oi=lat["OI"], oi_change=oi_chg,
                last_date=lat["Date"].strftime("%d %b %Y"))

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
    st.info("Selecciona un mercado del sidebar.")
    st.stop()

rcfg = REPORTS[report_key]
cy = datetime.now().year
min_years = max(years_back, 2)
years = list(range(cy - min_years + 1, cy + 1))

st.markdown("<div class='main-title'>📊 Commitment of Traders</div>", unsafe_allow_html=True)
st.markdown(f"<div class='subtitle'>CFTC · {market_key.strip()} · {report_key} · COT window {cot_win}w</div>",
            unsafe_allow_html=True)

with st.spinner(f"⟳  Descargando datos CFTC ({years[0]}–{years[-1]})..."):
    raw, download_logs = load_cot(years, rcfg["prefix"])
    df = parse_cot(raw, market_search, rcfg)

has_data = not df.empty
with st.expander("📡  Log de descarga CFTC", expanded=not has_data):
    for log in download_logs:
        st.markdown(f"`{log}`")
    if not raw.empty:
        st.markdown(f"`Total filas: {len(raw):,} · Mercados disponibles: {raw[_name_col(raw)].nunique() if _name_col(raw) else '?'}`")

if df.empty:
    st.error(f"**Sin datos** para `{market_search}` en `{report_key}`.")
    if not raw.empty:
        avail = available_markets(raw)
        tokens = [t for t in market_search.upper().split() if len(t) > 1]
        auto_hits = [m for m in avail
                     if m.upper() == market_search.upper()
                     or any(t in m.upper() for t in tokens)]
        if auto_hits:
            st.markdown(f"#### 🔍 Posibles coincidencias para `{market_search}`")
            st.dataframe(pd.DataFrame({"Nombre exacto en CFTC": auto_hits}), width='stretch')
        st.markdown(f"#### 📋 Todos los mercados ({len(avail)}) — descarga el CSV")
        csv_all = pd.DataFrame({"market_name": avail}).to_csv(index=False).encode()
        st.download_button(
            label=f"⬇️  Descargar lista completa ({len(avail)} mercados)",
            data=csv_all,
            file_name="cftc_markets.csv",
            mime="text/csv",
        )
        q = st.text_input("🔍 Filtrar en tiempo real:", placeholder="EURO, PESO, GOLD, S&P, CRUDE...")
        hits = [m for m in avail if q.upper() in m.upper()] if q else avail
        st.dataframe(pd.DataFrame({"Nombre en CFTC": hits[:400]}), width='stretch')
    else:
        st.warning("Descarga fallida. Revisa el log ↑ para el error exacto.")
    st.stop()

# ── KPIs ──────────────────────────────────────────────────────────────────────────
s = signals(df, cot_win)
st.markdown("<div class='section-header'>Resumen de Posicionamiento</div>", unsafe_allow_html=True)
c1,c2,c3,c4,c5 = st.columns(5)
with c1:
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>Señal COT</div>
        <div style='padding:4px 0'><span class='{s["signal_cls"]}'>{s["signal"]}</span></div>
        <div class='metric-delta neutral'>{s["last_date"]}</div></div>""", unsafe_allow_html=True)
with c2:
    v=s["nc_net"]; cls="positive" if v>0 else "negative"
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>Spec Net</div>
        <div class='metric-value {cls}'>{v:+,.0f}</div>
        <div class='metric-delta neutral'>{s["nc_trend"]}</div></div>""", unsafe_allow_html=True)
with c3:
    v=s["cm_net"]; cls="positive" if v>0 else "negative"
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>Hedger Net</div>
        <div class='metric-value {cls}'>{v:+,.0f}</div>
        <div class='metric-delta neutral'>{s["cm_trend"]}</div></div>""", unsafe_allow_html=True)
with c4:
    idx=s["cot_index"]; cls="positive" if idx>66 else("negative" if idx<33 else "neutral")
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>COT Index</div>
        <div class='metric-value {cls}'>{idx:.1f}</div>
        <div class='metric-delta neutral'>Ventana {cot_win}w</div></div>""", unsafe_allow_html=True)
with c5:
    oi=s["oi"]; oic=s["oi_change"]; cls="positive" if oic>0 else "negative"
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>Open Interest</div>
        <div class='metric-value neutral'>{oi:,.0f}</div>
        <div class='metric-delta {cls}'>{oic:+.1f}% (5s)</div></div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

st.markdown("<div class='section-header'>Posiciones Netas & COT Index</div>", unsafe_allow_html=True)
st.plotly_chart(chart_net_cot(df, cot_win, rcfg["nc_label"], rcfg["cm_label"]),
                width='stretch', config={"displayModeBar": False})

col_l, col_r = st.columns([2,1])
with col_l:
    st.markdown("<div class='section-header'>Posiciones Brutas</div>", unsafe_allow_html=True)
    st.plotly_chart(chart_gross(df, rcfg["nc_label"], rcfg["cm_label"]),
                    width='stretch', config={"displayModeBar": False})
with col_r:
    st.markdown("<div class='section-header'>Distribución Actual</div>", unsafe_allow_html=True)
    st.plotly_chart(chart_donut(df), width='stretch', config={"displayModeBar": False})

st.markdown("<div class='section-header'>Open Interest</div>", unsafe_allow_html=True)
st.plotly_chart(chart_oi(df), width='stretch', config={"displayModeBar": False})

with st.expander("📋  Datos crudos"):
    num_cols = df.select_dtypes("number").columns.tolist()
    st.dataframe(df.sort_values("Date", ascending=False)
                   .style.format({c:"{:,.0f}" for c in num_cols}),
                 width='stretch')

st.markdown("---")
st.markdown("""<div style='text-align:center;font-family:IBM Plex Mono;font-size:.68rem;color:#8b949e'>
Datos públicos CFTC · Actualización semanal (viernes) · Solo informativo</div>""",
            unsafe_allow_html=True)
