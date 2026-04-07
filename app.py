"""
COT Dashboard — Commitment of Traders Visualizer
Fuente de datos: CFTC (Commodity Futures Trading Commission)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import zipfile
import io
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ─── PAGE CONFIG ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="COT Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CUSTOM CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

.stApp {
    background: #0d1117;
    color: #e6edf3;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #161b22 !important;
    border-right: 1px solid #21262d;
}

[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stRadio label {
    color: #8b949e !important;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.75rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

/* Metric cards */
.metric-card {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 6px;
    padding: 16px 20px;
    text-align: center;
    font-family: 'IBM Plex Mono', monospace;
}
.metric-label {
    font-size: 0.7rem;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 6px;
}
.metric-value {
    font-size: 1.4rem;
    font-weight: 600;
    line-height: 1;
}
.metric-delta {
    font-size: 0.75rem;
    margin-top: 4px;
}
.positive { color: #3fb950; }
.negative { color: #f85149; }
.neutral  { color: #58a6ff; }

/* Title */
.main-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.6rem;
    font-weight: 600;
    color: #e6edf3;
    letter-spacing: -0.02em;
    border-bottom: 1px solid #21262d;
    padding-bottom: 12px;
    margin-bottom: 4px;
}
.subtitle {
    font-size: 0.8rem;
    color: #8b949e;
    font-family: 'IBM Plex Mono', monospace;
    margin-bottom: 24px;
}

/* Section headers */
.section-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    color: #58a6ff;
    border-left: 3px solid #58a6ff;
    padding-left: 10px;
    margin: 24px 0 14px 0;
}

/* Info box */
.info-box {
    background: #161b22;
    border: 1px solid #21262d;
    border-left: 3px solid #58a6ff;
    border-radius: 4px;
    padding: 14px 18px;
    font-size: 0.85rem;
    color: #8b949e;
    line-height: 1.6;
}
.info-box strong { color: #e6edf3; }

/* Signal badge */
.signal-bull {
    background: #0d4429;
    color: #3fb950;
    border: 1px solid #3fb950;
    border-radius: 20px;
    padding: 4px 14px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    font-weight: 600;
    display: inline-block;
}
.signal-bear {
    background: #3d1a1a;
    color: #f85149;
    border: 1px solid #f85149;
    border-radius: 20px;
    padding: 4px 14px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    font-weight: 600;
    display: inline-block;
}
.signal-neutral {
    background: #1c2a3a;
    color: #58a6ff;
    border: 1px solid #58a6ff;
    border-radius: 20px;
    padding: 4px 14px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    font-weight: 600;
    display: inline-block;
}
</style>
""", unsafe_allow_html=True)

# ─── CONSTANTS ───────────────────────────────────────────────────────────────────
CFTC_BASE_URL = "https://www.cftc.gov/files/dea/history"

REPORT_TYPES = {
    "Legacy Futures Only": "fut_disagg_txt",
    "Legacy Combined": "com_disagg_txt",
    "Financial Futures Only": "fut_fin_txt",
    "Financial Combined": "com_fin_txt",
}

# Markets more relevantes para trading
MARKETS_OF_INTEREST = {
    "— Forex —": None,
    "EUR/USD (Euro)": "EURO FX",
    "GBP/USD (British Pound)": "BRITISH POUND STERLING",
    "JPY/USD (Japanese Yen)": "JAPANESE YEN",
    "CHF/USD (Swiss Franc)": "SWISS FRANC",
    "AUD/USD (Aussie)": "AUSTRALIAN DOLLAR",
    "CAD/USD (Loonie)": "CANADIAN DOLLAR",
    "NZD/USD (Kiwi)": "NEW ZEALAND DOLLAR",
    "MXN/USD (Peso Mexicano)": "MEXICAN PESO",
    "— Índices —": None,
    "S&P 500": "S&P 500 STOCK INDEX",
    "Nasdaq 100": "NASDAQ-100 STOCK INDEX",
    "Dow Jones": "DOW JONES INDUSTRIAL AVG",
    "Russell 2000": "RUSSELL 2000 MINI",
    "— Commodities —": None,
    "Crude Oil (WTI)": "CRUDE OIL, LIGHT SWEET",
    "Gold": "GOLD",
    "Silver": "SILVER",
    "Copper": "COPPER",
    "Natural Gas": "NATURAL GAS",
    "Corn": "CORN",
    "Wheat": "WHEAT",
    "Soybeans": "SOYBEANS",
    "— Rates —": None,
    "10Y T-Note": "10-YEAR U.S. TREASURY NOTES",
    "2Y T-Note": "2-YEAR U.S. TREASURY NOTES",
    "30Y T-Bond": "U.S. TREASURY BONDS",
    "Eurodollar": "EURODOLLAR",
}

# ─── DATA FETCHING ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_cot_data(year: int, report_prefix: str) -> pd.DataFrame:
    """Descarga y parsea datos COT del CFTC para un año dado."""
    url = f"{CFTC_BASE_URL}/{report_prefix}_{year}.zip"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            csv_name = [n for n in z.namelist() if n.endswith('.txt') or n.endswith('.csv')][0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f, low_memory=False)
        return df
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_multi_year_cot(years: list, report_prefix: str) -> pd.DataFrame:
    """Carga varios años y los concatena."""
    frames = []
    for y in years:
        df = fetch_cot_data(y, report_prefix)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def parse_cot(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    """Filtra y prepara el DataFrame para un mercado específico."""
    if df.empty:
        return pd.DataFrame()

    # Buscar columna de nombre de mercado
    name_col = None
    for c in ["Market_and_Exchange_Names", "Market and Exchange Names", "Contract_Market_Name"]:
        if c in df.columns:
            name_col = c
            break
    if name_col is None:
        return pd.DataFrame()

    # Filtrar por nombre (case-insensitive, partial match)
    mask = df[name_col].str.upper().str.contains(market_name.upper(), na=False)
    df_m = df[mask].copy()
    if df_m.empty:
        return pd.DataFrame()

    # Columna de fecha
    date_col = None
    for c in ["Report_Date_as_MM_DD_YYYY", "As_of_Date_In_Form_YYMMDD", "Report Date"]:
        if c in df_m.columns:
            date_col = c
            break
    if date_col is None:
        return pd.DataFrame()

    df_m["Date"] = pd.to_datetime(df_m[date_col], infer_datetime_format=True, errors="coerce")
    df_m = df_m.dropna(subset=["Date"]).sort_values("Date")

    # Mapeo de columnas clave (Legacy report)
    col_map = {
        "NonComm_Positions_Long_All": ["Noncommercial_Positions_Long_All", "NonComm Positions-Long (All)"],
        "NonComm_Positions_Short_All": ["Noncommercial_Positions_Short_All", "NonComm Positions-Short (All)"],
        "Comm_Positions_Long_All": ["Commercial_Positions_Long_All", "Comm Positions-Long (All)"],
        "Comm_Positions_Short_All": ["Commercial_Positions_Short_All", "Comm Positions-Short (All)"],
        "Open_Interest_All": ["Open_Interest_All", "Open Interest (All)"],
        "NonRept_Positions_Long_All": ["NonRept_Positions_Long_All", "NonRept Positions-Long (All)"],
        "NonRept_Positions_Short_All": ["NonRept_Positions_Short_All", "NonRept Positions-Short (All)"],
    }

    for target, candidates in col_map.items():
        for c in candidates:
            if c in df_m.columns:
                df_m[target] = pd.to_numeric(df_m[c], errors="coerce")
                break
        if target not in df_m.columns:
            df_m[target] = np.nan

    # Calcular netas
    df_m["Net_NonComm"] = df_m["NonComm_Positions_Long_All"] - df_m["NonComm_Positions_Short_All"]
    df_m["Net_Comm"]    = df_m["Comm_Positions_Long_All"]    - df_m["Comm_Positions_Short_All"]
    df_m["Net_NonRept"] = df_m["NonRept_Positions_Long_All"] - df_m["NonRept_Positions_Short_All"]

    return df_m[["Date", "Open_Interest_All",
                 "NonComm_Positions_Long_All", "NonComm_Positions_Short_All", "Net_NonComm",
                 "Comm_Positions_Long_All",    "Comm_Positions_Short_All",    "Net_Comm",
                 "NonRept_Positions_Long_All", "NonRept_Positions_Short_All", "Net_NonRept"]].reset_index(drop=True)


def compute_cot_index(series: pd.Series, window: int = 52) -> pd.Series:
    """COT Index: posición neta normalizada en ventana rolling (0-100)."""
    roll_min = series.rolling(window, min_periods=5).min()
    roll_max = series.rolling(window, min_periods=5).max()
    rng = roll_max - roll_min
    idx = np.where(rng != 0, (series - roll_min) / rng * 100, 50)
    return pd.Series(idx, index=series.index)


# ─── CHART HELPERS ───────────────────────────────────────────────────────────────
CHART_THEME = dict(
    paper_bgcolor="#0d1117",
    plot_bgcolor="#0d1117",
    font=dict(family="IBM Plex Mono", color="#8b949e", size=11),
    xaxis=dict(gridcolor="#21262d", zeroline=False, showline=True, linecolor="#21262d"),
    yaxis=dict(gridcolor="#21262d", zeroline=False, showline=True, linecolor="#21262d"),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#21262d", borderwidth=1,
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=10, r=10, t=40, b=10),
    hovermode="x unified",
)

def apply_theme(fig, **extra):
    fig.update_layout(**CHART_THEME, **extra)
    return fig


def chart_positions(df: pd.DataFrame, title: str) -> go.Figure:
    """Gráfico de posiciones largas/cortas por categoría."""
    fig = go.Figure()

    # Non-Commercial (speculators)
    fig.add_trace(go.Scatter(x=df["Date"], y=df["NonComm_Positions_Long_All"],
        name="NC Long", line=dict(color="#3fb950", width=1.5), fill=None))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["NonComm_Positions_Short_All"],
        name="NC Short", line=dict(color="#f85149", width=1.5), fill=None))

    # Commercial (hedgers)
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Comm_Positions_Long_All"],
        name="Comm Long", line=dict(color="#79c0ff", width=1.5, dash="dot")))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Comm_Positions_Short_All"],
        name="Comm Short", line=dict(color="#ffa657", width=1.5, dash="dot")))

    apply_theme(fig, title=title, yaxis_title="Contratos")
    return fig


def chart_net_positions(df: pd.DataFrame, cot_window: int) -> go.Figure:
    """Posiciones netas + COT Index."""
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.04,
                        row_heights=[0.4, 0.35, 0.25])

    # ── Net Non-Commercial (speculators)
    nc = df["Net_NonComm"]
    colors_nc = ["#3fb950" if v >= 0 else "#f85149" for v in nc]
    fig.add_trace(go.Bar(x=df["Date"], y=nc, name="Spec Net",
                         marker_color=colors_nc, opacity=0.85), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=nc.rolling(4).mean(),
                             name="Spec MA4", line=dict(color="#f0e68c", width=1.5)), row=1, col=1)

    # ── Net Commercial (hedgers)
    comm = df["Net_Comm"]
    colors_cm = ["#79c0ff" if v >= 0 else "#ffa657" for v in comm]
    fig.add_trace(go.Bar(x=df["Date"], y=comm, name="Hedger Net",
                         marker_color=colors_cm, opacity=0.85), row=2, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=comm.rolling(4).mean(),
                             name="Hedger MA4", line=dict(color="#d2a8ff", width=1.5)), row=2, col=1)

    # ── COT Index (speculadores)
    cot_idx = compute_cot_index(nc, window=cot_window)
    # Zona overbought/oversold
    fig.add_hrect(y0=75, y1=100, fillcolor="#3fb950", opacity=0.07,
                  layer="below", line_width=0, row=3, col=1)
    fig.add_hrect(y0=0, y1=25, fillcolor="#f85149", opacity=0.07,
                  layer="below", line_width=0, row=3, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=cot_idx, name=f"COT Index ({cot_window}w)",
                             line=dict(color="#58a6ff", width=2), fill="tozeroy",
                             fillcolor="rgba(88,166,255,0.08)"), row=3, col=1)
    fig.add_hline(y=75, line=dict(color="#3fb950", dash="dash", width=1), row=3, col=1)
    fig.add_hline(y=25, line=dict(color="#f85149", dash="dash", width=1), row=3, col=1)
    fig.add_hline(y=50, line=dict(color="#8b949e", dash="dot", width=1), row=3, col=1)

    apply_theme(fig, height=600,
                yaxis=dict(title="Contratos", gridcolor="#21262d"),
                yaxis2=dict(title="Contratos", gridcolor="#21262d"),
                yaxis3=dict(title="Índice 0-100", gridcolor="#21262d", range=[0, 100]))

    fig.update_yaxes(gridcolor="#21262d")
    return fig


def chart_open_interest(df: pd.DataFrame) -> go.Figure:
    """Open Interest total."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Open_Interest_All"],
        name="Open Interest", fill="tozeroy",
        line=dict(color="#58a6ff", width=2),
        fillcolor="rgba(88,166,255,0.1)"))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Open_Interest_All"].rolling(13).mean(),
        name="MA 13w", line=dict(color="#ffa657", width=1.5, dash="dash")))
    apply_theme(fig, title="Open Interest Total", yaxis_title="Contratos")
    return fig


def chart_positioning_breakdown(df: pd.DataFrame) -> go.Figure:
    """Desglose porcentual de posiciones."""
    total = df["Open_Interest_All"].replace(0, np.nan)
    nc_long_pct  = df["NonComm_Positions_Long_All"]  / total * 100
    nc_short_pct = df["NonComm_Positions_Short_All"] / total * 100
    cm_long_pct  = df["Comm_Positions_Long_All"]     / total * 100
    cm_short_pct = df["Comm_Positions_Short_All"]    / total * 100

    fig = go.Figure()
    latest = df.iloc[-1]
    labels = ["NC Long", "NC Short", "Comm Long", "Comm Short"]
    values = [
        latest["NonComm_Positions_Long_All"],
        latest["NonComm_Positions_Short_All"],
        latest["Comm_Positions_Long_All"],
        latest["Comm_Positions_Short_All"],
    ]
    colors = ["#3fb950", "#f85149", "#79c0ff", "#ffa657"]

    fig.add_trace(go.Pie(
        labels=labels, values=values, hole=0.55,
        marker=dict(colors=colors, line=dict(color="#0d1117", width=2)),
        textfont=dict(family="IBM Plex Mono", size=11, color="#e6edf3"),
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} contratos<br>%{percent}<extra></extra>",
    ))

    apply_theme(fig, title="Distribución de Posiciones (última semana)",
                height=320, showlegend=True)
    return fig


# ─── SIGNAL ENGINE ───────────────────────────────────────────────────────────────
def compute_signals(df: pd.DataFrame, cot_window: int) -> dict:
    """Genera señales básicas del COT."""
    if len(df) < 10:
        return {}

    latest = df.iloc[-1]
    prev   = df.iloc[-5] if len(df) > 5 else df.iloc[0]

    nc_net  = latest["Net_NonComm"]
    cm_net  = latest["Net_Comm"]
    nc_prev = prev["Net_NonComm"]
    cm_prev = prev["Net_Comm"]

    cot_idx = compute_cot_index(df["Net_NonComm"], window=cot_window)
    current_cot_idx = cot_idx.iloc[-1]

    nc_trend = "↑ Aumentando" if nc_net > nc_prev else "↓ Reduciendo"
    cm_trend = "↑ Aumentando" if cm_net > cm_prev else "↓ Reduciendo"

    if current_cot_idx >= 75:
        signal = "ALCISTA"
        signal_cls = "signal-bull"
    elif current_cot_idx <= 25:
        signal = "BAJISTA"
        signal_cls = "signal-bear"
    else:
        signal = "NEUTRAL"
        signal_cls = "signal-neutral"

    oi_change = (latest["Open_Interest_All"] - prev["Open_Interest_All"]) / prev["Open_Interest_All"] * 100 if prev["Open_Interest_All"] else 0

    return {
        "signal": signal,
        "signal_cls": signal_cls,
        "nc_net": nc_net,
        "cm_net": cm_net,
        "nc_trend": nc_trend,
        "cm_trend": cm_trend,
        "cot_index": current_cot_idx,
        "oi": latest["Open_Interest_All"],
        "oi_change": oi_change,
        "last_date": latest["Date"].strftime("%d %b %Y"),
    }


# ─── SIDEBAR ──────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("<div style='font-family:IBM Plex Mono;font-size:1.1rem;font-weight:600;color:#58a6ff;padding:8px 0 16px'>⬡ COT Dashboard</div>", unsafe_allow_html=True)

    st.markdown("<div style='font-family:IBM Plex Mono;font-size:0.65rem;color:#8b949e;text-transform:uppercase;letter-spacing:0.12em;margin-bottom:6px'>Mercado</div>", unsafe_allow_html=True)
    market_label = st.selectbox("", list(MARKETS_OF_INTEREST.keys()),
                                index=list(MARKETS_OF_INTEREST.keys()).index("EUR/USD (Euro)"),
                                label_visibility="collapsed")

    st.markdown("<div style='font-family:IBM Plex Mono;font-size:0.65rem;color:#8b949e;text-transform:uppercase;letter-spacing:0.12em;margin:12px 0 6px'>Tipo de Reporte</div>", unsafe_allow_html=True)
    report_label = st.radio("", list(REPORT_TYPES.keys()),
                            index=0, label_visibility="collapsed")

    st.markdown("<div style='font-family:IBM Plex Mono;font-size:0.65rem;color:#8b949e;text-transform:uppercase;letter-spacing:0.12em;margin:12px 0 6px'>Años de historia</div>", unsafe_allow_html=True)
    years_back = st.slider("", 1, 10, 3, label_visibility="collapsed")

    st.markdown("<div style='font-family:IBM Plex Mono;font-size:0.65rem;color:#8b949e;text-transform:uppercase;letter-spacing:0.12em;margin:12px 0 6px'>Ventana COT Index (semanas)</div>", unsafe_allow_html=True)
    cot_window = st.slider("", 13, 156, 52, step=13, label_visibility="collapsed")

    st.markdown("---")
    st.markdown("<div class='info-box'>Los datos provienen del <strong>CFTC</strong> (Commodity Futures Trading Commission). Se actualizan cada viernes.</div>", unsafe_allow_html=True)

# ─── MAIN ─────────────────────────────────────────────────────────────────────────
market_search = MARKETS_OF_INTEREST.get(market_label)
report_prefix = REPORT_TYPES[report_label]

if market_search is None:
    st.info("Selecciona un mercado del sidebar para comenzar.")
    st.stop()

# Header
st.markdown(f"<div class='main-title'>📊 Commitment of Traders</div>", unsafe_allow_html=True)
st.markdown(f"<div class='subtitle'>CFTC Weekly Report · {market_label} · {report_label}</div>", unsafe_allow_html=True)

# ── Load data
current_year = datetime.now().year
years = list(range(current_year - years_back + 1, current_year + 1))

with st.spinner("⟳  Descargando datos CFTC..."):
    raw_df = load_multi_year_cot(years, report_prefix)
    df = parse_cot(raw_df, market_search)

if df.empty:
    st.error(f"""
    ⚠️  No se encontraron datos para **{market_label}** con el reporte **{report_label}**.

    **Sugerencias:**
    - Prueba con "Legacy Futures Only" para la mayoría de mercados
    - Usa "Financial Futures Only" para índices bursátiles y tasas
    - Verifica tu conexión a internet
    """)
    st.stop()

# ── Signals
sigs = compute_signals(df, cot_window)

# ── KPI Row
st.markdown("<div class='section-header'>Resumen de Posicionamiento</div>", unsafe_allow_html=True)

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class='metric-card'>
        <div class='metric-label'>Señal COT</div>
        <div><span class='{sigs["signal_cls"]}'>{sigs["signal"]}</span></div>
        <div class='metric-delta neutral'>Última: {sigs["last_date"]}</div>
    </div>""", unsafe_allow_html=True)

with col2:
    v = sigs["nc_net"]
    cls = "positive" if v > 0 else "negative"
    st.markdown(f"""
    <div class='metric-card'>
        <div class='metric-label'>Spec Net Position</div>
        <div class='metric-value {cls}'>{v:+,.0f}</div>
        <div class='metric-delta neutral'>{sigs["nc_trend"]}</div>
    </div>""", unsafe_allow_html=True)

with col3:
    v = sigs["cm_net"]
    cls = "positive" if v > 0 else "negative"
    st.markdown(f"""
    <div class='metric-card'>
        <div class='metric-label'>Hedger Net Position</div>
        <div class='metric-value {cls}'>{v:+,.0f}</div>
        <div class='metric-delta neutral'>{sigs["cm_trend"]}</div>
    </div>""", unsafe_allow_html=True)

with col4:
    idx = sigs["cot_index"]
    cls = "positive" if idx > 66 else ("negative" if idx < 33 else "neutral")
    st.markdown(f"""
    <div class='metric-card'>
        <div class='metric-label'>COT Index</div>
        <div class='metric-value {cls}'>{idx:.1f}</div>
        <div class='metric-delta neutral'>Ventana: {cot_window} semanas</div>
    </div>""", unsafe_allow_html=True)

with col5:
    oi = sigs["oi"]
    oic = sigs["oi_change"]
    cls = "positive" if oic > 0 else "negative"
    st.markdown(f"""
    <div class='metric-card'>
        <div class='metric-label'>Open Interest</div>
        <div class='metric-value neutral'>{oi:,.0f}</div>
        <div class='metric-delta {cls}'>{oic:+.1f}% (4s)</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Charts
st.markdown("<div class='section-header'>Posiciones Netas & COT Index</div>", unsafe_allow_html=True)
st.plotly_chart(chart_net_positions(df, cot_window), use_container_width=True, config={"displayModeBar": False})

col_l, col_r = st.columns([2, 1])

with col_l:
    st.markdown("<div class='section-header'>Posiciones Brutas por Categoría</div>", unsafe_allow_html=True)
    st.plotly_chart(chart_positions(df, "Posiciones Largas vs Cortas"), use_container_width=True, config={"displayModeBar": False})

with col_r:
    st.markdown("<div class='section-header'>Distribución Actual</div>", unsafe_allow_html=True)
    st.plotly_chart(chart_positioning_breakdown(df), use_container_width=True, config={"displayModeBar": False})

st.markdown("<div class='section-header'>Open Interest</div>", unsafe_allow_html=True)
st.plotly_chart(chart_open_interest(df), use_container_width=True, config={"displayModeBar": False})

# ── Raw data toggle
with st.expander("📋  Ver datos crudos"):
    st.dataframe(
        df.sort_values("Date", ascending=False)
          .style.format({c: "{:,.0f}" for c in df.select_dtypes("number").columns}),
        use_container_width=True
    )

# ── Footer
st.markdown("---")
st.markdown("""
<div style='text-align:center;font-family:IBM Plex Mono;font-size:0.7rem;color:#8b949e'>
Datos públicos del CFTC · Actualización semanal (viernes) · Solo informativo, no constituye asesoramiento financiero
</div>
""", unsafe_allow_html=True)
