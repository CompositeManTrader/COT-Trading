"""
COT Dashboard v5 — Commitment of Traders · Señales Quant
Fuente: CFTC (Commodity Futures Trading Commission)

Novedades v5:
- Contratos actualizados (E-mini S&P 500, E-mini Dow, etc.) — fix "termina en 2021"
- Modo Preset + Personalizado (cualquiera de los ~500 mercados del CFTC)
- 5 modelos cuantitativos con reglas de entrada/salida explícitas
- Histórico ampliado a 20 años
- Lookup canónico de columnas (soporta Legacy y Disaggregated/TFF)
"""

import re
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

# ─── CSS ──────────────────────────────────────────────────────────────────────────
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
.quant-card{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:16px 18px;font-family:'IBM Plex Sans',sans-serif;margin-bottom:12px;height:100%;}
.quant-title{font-family:'IBM Plex Mono',monospace;font-size:.72rem;text-transform:uppercase;letter-spacing:.12em;color:#8b949e;margin-bottom:8px;}
.quant-value{font-family:'IBM Plex Mono',monospace;font-size:1.6rem;font-weight:600;color:#e6edf3;line-height:1;margin-bottom:4px;}
.quant-desc{font-size:.78rem;color:#8b949e;line-height:1.5;margin-top:10px;}
.quant-rule{font-size:.73rem;color:#a0a9b4;line-height:1.5;margin-top:6px;font-family:'IBM Plex Mono',monospace;background:#0d1117;border:1px dashed #30363d;border-radius:4px;padding:6px 10px;}
.quant-action{font-size:.78rem;font-weight:600;margin-top:8px;padding:6px 10px;border-radius:4px;font-family:'IBM Plex Mono',monospace;}
.action-buy{background:#0d4429;color:#3fb950;border:1px solid #3fb950;}
.action-sell{background:#3d1a1a;color:#f85149;border:1px solid #f85149;}
.action-hold{background:#1c2a3a;color:#58a6ff;border:1px solid #58a6ff;}
</style>
""", unsafe_allow_html=True)

# ─── CONFIG ──────────────────────────────────────────────────────────────────────
CFTC_BASE = "https://www.cftc.gov/files/dea/history"

REPORTS = {
    "Legacy — Futures Only": {
        "prefix":   "deacot",
        "long_nc":  "Noncommercial_Positions_Long_All",
        "short_nc": "Noncommercial_Positions_Short_All",
        "long_cm":  "Commercial_Positions_Long_All",
        "short_cm": "Commercial_Positions_Short_All",
        "long_nr":  "Nonreportable_Positions_Long_All",
        "short_nr": "Nonreportable_Positions_Short_All",
        "nc_label": "Non-Commercial (Specs)",
        "cm_label": "Commercial (Hedgers)",
        "description": "Reporte clásico. Cubre <strong>Forex, Commodities y Bonos</strong>.",
    },
    "Legacy — Combined (Futures+Options)": {
        "prefix":   "deahistfo",
        "long_nc":  "Noncommercial_Positions_Long_All",
        "short_nc": "Noncommercial_Positions_Short_All",
        "long_cm":  "Commercial_Positions_Long_All",
        "short_cm": "Commercial_Positions_Short_All",
        "long_nr":  "Nonreportable_Positions_Long_All",
        "short_nr": "Nonreportable_Positions_Short_All",
        "nc_label": "Non-Commercial (Specs)",
        "cm_label": "Commercial (Hedgers)",
        "description": "Legacy combinado futuros + opciones.",
    },
    "Disaggregated — Futures Only": {
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
        "prefix":   "fut_fin_txt_",
        "long_nc":  "Lev_Money_Positions_Long_All",
        "short_nc": "Lev_Money_Positions_Short_All",
        "long_cm":  "Asset_Mgr_Positions_Long_All",
        "short_cm": "Asset_Mgr_Positions_Short_All",
        "long_nr":  "Other_Rept_Positions_Long_All",
        "short_nr": "Other_Rept_Positions_Short_All",
        "nc_label": "Leveraged Money (Specs)",
        "cm_label": "Asset Managers",
        "description": "Específico para <strong>índices bursátiles, tasas y cripto</strong>.",
    },
}

# ─── PRESETS: cada entry tiene lista de candidatos + reporte recomendado ──────────
SEP = {"is_sep": True}
MARKETS_PRESET = {
    "— FOREX (Legacy) —": SEP,
    "EUR/USD  ·  Euro FX":        {"names": ["EURO FX - CHICAGO MERCANTILE EXCHANGE"], "report": "Legacy — Futures Only"},
    "GBP/USD  ·  British Pound":  {"names": ["BRITISH POUND - CHICAGO MERCANTILE EXCHANGE"], "report": "Legacy — Futures Only"},
    "JPY/USD  ·  Japanese Yen":   {"names": ["JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE"], "report": "Legacy — Futures Only"},
    "CHF/USD  ·  Swiss Franc":    {"names": ["SWISS FRANC - CHICAGO MERCANTILE EXCHANGE"], "report": "Legacy — Futures Only"},
    "AUD/USD  ·  Australian $":   {"names": ["AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE"], "report": "Legacy — Futures Only"},
    "CAD/USD  ·  Canadian $":     {"names": ["CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE"], "report": "Legacy — Futures Only"},
    "NZD/USD  ·  New Zealand $":  {"names": ["NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE"], "report": "Legacy — Futures Only"},
    "MXN/USD  ·  Peso Mexicano":  {"names": ["MEXICAN PESO - CHICAGO MERCANTILE EXCHANGE"], "report": "Legacy — Futures Only"},
    "USD Index (DXY)":            {"names": ["USD INDEX - ICE FUTURES U.S."], "report": "Legacy — Futures Only"},

    "— ÍNDICES US (Financial TFF) —": SEP,
    "S&P 500 (E-mini)":           {"names": ["E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
                                              "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE"], "report": "Financial TFF — Futures Only"},
    "Nasdaq-100 (E-mini)":        {"names": ["NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE",
                                              "E-MINI NASDAQ-100 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
                                              "Nasdaq-100 Consolidated - CHICAGO MERCANTILE EXCHANGE"], "report": "Financial TFF — Futures Only"},
    "Dow Jones (E-mini)":         {"names": ["E-MINI DOW JONES ($5) - CHICAGO BOARD OF TRADE",
                                              "DOW JONES INDUSTRIAL AVG- x $5 - CHICAGO BOARD OF TRADE",
                                              "DJIA Consolidated - CHICAGO BOARD OF TRADE"], "report": "Financial TFF — Futures Only"},
    "Russell 2000 (E-mini)":      {"names": ["E-MINI RUSSELL 2000 INDEX - CHICAGO MERCANTILE EXCHANGE",
                                              "RUSSELL 2000 MINI INDEX - ICE FUTURES U.S."], "report": "Financial TFF — Futures Only"},
    "VIX (Volatilidad)":          {"names": ["VIX FUTURES - CBOE FUTURES EXCHANGE"], "report": "Financial TFF — Futures Only"},
    "Nikkei 225 (USD)":           {"names": ["NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE"], "report": "Financial TFF — Futures Only"},

    "— ENERGÍA (Disaggregated) —": SEP,
    "Crude Oil WTI":              {"names": ["CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},
    "Brent Crude":                {"names": ["BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE",
                                              "BRENT CRUDE OIL LAST DAY - NEW YORK MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},
    "Natural Gas (Henry Hub)":    {"names": ["NATURAL GAS - NEW YORK MERCANTILE EXCHANGE",
                                              "HENRY HUB - NEW YORK MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},
    "Gasoline RBOB":              {"names": ["GASOLINE BLENDSTOCK (RBOB) - NEW YORK MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},
    "Heating Oil":                {"names": ["#2 HEATING OIL, NEW YORK HARBOR - NEW YORK MERCANTILE EXCHANGE",
                                              "NO. 2 HEATING OIL, NEW YORK HARBOR - NEW YORK MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},

    "— METALES (Disaggregated) —": SEP,
    "Gold":                       {"names": ["GOLD - COMMODITY EXCHANGE INC."], "report": "Disaggregated — Futures Only"},
    "Silver":                     {"names": ["SILVER - COMMODITY EXCHANGE INC."], "report": "Disaggregated — Futures Only"},
    "Copper":                     {"names": ["COPPER- #1 - COMMODITY EXCHANGE INC.",
                                              "COPPER-GRADE #1 - COMMODITY EXCHANGE INC."], "report": "Disaggregated — Futures Only"},
    "Platinum":                   {"names": ["PLATINUM - NEW YORK MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},
    "Palladium":                  {"names": ["PALLADIUM - NEW YORK MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},

    "— GRANOS Y SOFTS (Disaggregated) —": SEP,
    "Corn":                       {"names": ["CORN - CHICAGO BOARD OF TRADE"], "report": "Disaggregated — Futures Only"},
    "Wheat SRW (Chicago)":        {"names": ["WHEAT-SRW - CHICAGO BOARD OF TRADE"], "report": "Disaggregated — Futures Only"},
    "Wheat HRW (Kansas)":         {"names": ["WHEAT-HRW - CHICAGO BOARD OF TRADE"], "report": "Disaggregated — Futures Only"},
    "Soybeans":                   {"names": ["SOYBEANS - CHICAGO BOARD OF TRADE"], "report": "Disaggregated — Futures Only"},
    "Soybean Oil":                {"names": ["SOYBEAN OIL - CHICAGO BOARD OF TRADE"], "report": "Disaggregated — Futures Only"},
    "Soybean Meal":               {"names": ["SOYBEAN MEAL - CHICAGO BOARD OF TRADE"], "report": "Disaggregated — Futures Only"},
    "Sugar #11":                  {"names": ["SUGAR NO. 11 - ICE FUTURES U.S."], "report": "Disaggregated — Futures Only"},
    "Coffee C":                   {"names": ["COFFEE C - ICE FUTURES U.S."], "report": "Disaggregated — Futures Only"},
    "Cocoa":                      {"names": ["COCOA - ICE FUTURES U.S."], "report": "Disaggregated — Futures Only"},
    "Cotton #2":                  {"names": ["COTTON NO. 2 - ICE FUTURES U.S."], "report": "Disaggregated — Futures Only"},

    "— LIVESTOCK (Disaggregated) —": SEP,
    "Live Cattle":                {"names": ["LIVE CATTLE - CHICAGO MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},
    "Lean Hogs":                  {"names": ["LEAN HOGS - CHICAGO MERCANTILE EXCHANGE"], "report": "Disaggregated — Futures Only"},

    "— TASAS (Financial TFF) —": SEP,
    "2Y T-Note":                  {"names": ["2-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE"], "report": "Financial TFF — Futures Only"},
    "5Y T-Note":                  {"names": ["5-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE"], "report": "Financial TFF — Futures Only"},
    "10Y T-Note":                 {"names": ["10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE",
                                              "UST 10Y NOTE - CHICAGO BOARD OF TRADE"], "report": "Financial TFF — Futures Only"},
    "30Y T-Bond":                 {"names": ["U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE",
                                              "UST BOND - CHICAGO BOARD OF TRADE"], "report": "Financial TFF — Futures Only"},
    "Ultra T-Bond":               {"names": ["ULTRA U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE"], "report": "Financial TFF — Futures Only"},
    "SOFR 3M":                    {"names": ["SOFR-3M - CHICAGO MERCANTILE EXCHANGE"], "report": "Financial TFF — Futures Only"},

    "— CRYPTO (Financial TFF) —": SEP,
    "Bitcoin":                    {"names": ["BITCOIN - CHICAGO MERCANTILE EXCHANGE",
                                              "MICRO BITCOIN - CHICAGO MERCANTILE EXCHANGE"], "report": "Financial TFF — Futures Only"},
    "Ether":                      {"names": ["ETHER CASH SETTLED - CHICAGO MERCANTILE EXCHANGE",
                                              "MICRO ETHER - CHICAGO MERCANTILE EXCHANGE"], "report": "Financial TFF — Futures Only"},
}

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/zip,*/*",
}

# ─── DATA HELPERS ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_year(year: int, prefix: str) -> tuple:
    url = f"{CFTC_BASE}/{prefix}{year}.zip"
    try:
        r = requests.get(url, headers=HEADERS, timeout=60, allow_redirects=True)
        if r.status_code == 404:
            return pd.DataFrame(), f"⚠️ {year}: 404 — {url}"
        if r.status_code != 200:
            return pd.DataFrame(), f"❌ {year}: HTTP {r.status_code} — {url}"
        if not r.content.startswith(b"PK"):
            return pd.DataFrame(), f"❌ {year}: respuesta no es ZIP — {url}"
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            txt_files = [n for n in z.namelist()
                         if n.lower().endswith(".txt") and not n.startswith("__MACOSX")]
            if not txt_files:
                return pd.DataFrame(), f"❌ {year}: ZIP sin .txt"
            fname = txt_files[0]
            with z.open(fname) as f:
                df = pd.read_csv(f, low_memory=False)
        return df, f"✅ {year}: {len(df):,} filas — {url}"
    except requests.exceptions.ConnectionError as e:
        return pd.DataFrame(), f"❌ {year}: Conexión fallida — {str(e)[:80]}"
    except zipfile.BadZipFile:
        return pd.DataFrame(), f"❌ {year}: ZIP inválido"
    except Exception as e:
        return pd.DataFrame(), f"❌ {year}: {type(e).__name__}: {str(e)[:80]}"


def load_cot(years: list, prefix: str) -> tuple:
    frames, logs = [], []
    for y in years:
        df, msg = fetch_year(y, prefix)
        logs.append(msg)
        if not df.empty:
            frames.append(df)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return combined, logs


# ─── COLUMN LOOKUP CANÓNICO ──────────────────────────────────────────────────────
def _canon(name) -> str:
    """Forma canónica: solo alfanuméricos en mayúsculas."""
    return re.sub(r"[^A-Z0-9]", "", str(name).upper())

def _col_index(df) -> dict:
    return {_canon(c): c for c in df.columns}

def _find_col(idx: dict, *candidates):
    for cand in candidates:
        actual = idx.get(_canon(cand))
        if actual is not None:
            return actual
    return None

def _name_col(df):
    idx = _col_index(df)
    return _find_col(idx,
        "Market_and_Exchange_Names", "Market and Exchange Names", "Contract_Market_Name")

def _date_col(df):
    idx = _col_index(df)
    return _find_col(idx,
        "Report_Date_as_YYYY-MM-DD", "Report Date as YYYY-MM-DD",
        "Report_Date_as_MM_DD_YYYY", "Report Date as MM_DD_YYYY",
        "As of Date in Form YYYY-MM-DD", "As_of_Date_In_Form_YYYY-MM-DD",
        "As of Date in Form YYMMDD", "As_of_Date_In_Form_YYMMDD")


def available_markets(df) -> list:
    nc = _name_col(df)
    return sorted(df[nc].dropna().unique().tolist()) if nc and not df.empty else []


def _normalize(s) -> str:
    return re.sub(r"\s+", " ", str(s).upper().strip())


def _match_market(series: pd.Series, targets) -> pd.Series:
    """Busca cualquiera de los nombres candidatos en la serie. Cascada:
    (1) exacto normalizado → (2) head antes del ' - ' → (3) todos los tokens."""
    if isinstance(targets, str):
        targets = [targets]
    series_n = series.fillna("").astype(str).map(_normalize)
    mask_total = pd.Series(False, index=series.index)

    for target in targets:
        target_n = _normalize(target)
        m = series_n == target_n
        if m.any():
            mask_total = mask_total | m
            continue
        if " - " in target_n:
            head = target_n.split(" - ")[0].strip()
            m = series_n.str.startswith(head + " -") | series_n.str.startswith(head + "-") | (series_n == head)
            if m.any():
                mask_total = mask_total | m
                continue
        tokens = [t for t in target_n.replace("-", " ").split() if len(t) > 2]
        if tokens:
            m = series_n.apply(lambda x: all(t in x for t in tokens))
            mask_total = mask_total | m

    return mask_total


def parse_cot(df: pd.DataFrame, market_search, rcfg: dict) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    nc, dc = _name_col(df), _date_col(df)
    if not nc or not dc:
        return pd.DataFrame()
    mask = _match_market(df[nc], market_search)
    df_m = df[mask].copy()
    if df_m.empty:
        return pd.DataFrame()
    raw_date = df_m[dc].astype(str).str.strip()
    df_m["Date"] = pd.to_datetime(raw_date, format="%Y-%m-%d", errors="coerce")
    m_nat = df_m["Date"].isna()
    if m_nat.any():
        df_m.loc[m_nat, "Date"] = pd.to_datetime(raw_date[m_nat], format="%m/%d/%Y", errors="coerce")
    m_nat = df_m["Date"].isna()
    if m_nat.any():
        df_m.loc[m_nat, "Date"] = pd.to_datetime(raw_date[m_nat], errors="coerce")
    df_m = df_m.dropna(subset=["Date"]).sort_values("Date").drop_duplicates("Date")

    idx = _col_index(df_m)
    def _n(*candidates):
        col = _find_col(idx, *candidates)
        if col is None:
            return pd.Series(np.nan, index=df_m.index)
        return pd.to_numeric(df_m[col], errors="coerce")

    df_m["OI"]      = _n("Open_Interest_All")
    df_m["NC_Long"] = _n(rcfg["long_nc"]);  df_m["NC_Short"] = _n(rcfg["short_nc"])
    df_m["CM_Long"] = _n(rcfg["long_cm"]);  df_m["CM_Short"] = _n(rcfg["short_cm"])
    df_m["NR_Long"] = _n(rcfg["long_nr"], "Nonreportable_Positions_Long_All", "NonRept_Positions_Long_All")
    df_m["NR_Short"] = _n(rcfg["short_nr"], "Nonreportable_Positions_Short_All", "NonRept_Positions_Short_All")
    df_m["Net_NC"]  = df_m["NC_Long"] - df_m["NC_Short"]
    df_m["Net_CM"]  = df_m["CM_Long"] - df_m["CM_Short"]
    df_m["Net_NR"]  = df_m["NR_Long"] - df_m["NR_Short"]
    return df_m[["Date","OI","NC_Long","NC_Short","Net_NC",
                 "CM_Long","CM_Short","Net_CM","NR_Long","NR_Short","Net_NR"]].reset_index(drop=True)


# ─── INDICADORES CUANTITATIVOS ───────────────────────────────────────────────────
def cot_index(series: pd.Series, window: int = 52) -> pd.Series:
    """Briese COT Index: percentil de la posición neta en ventana rolling."""
    mp = max(13, min(window // 2, 26))
    mn = series.rolling(window, min_periods=mp).min()
    mx = series.rolling(window, min_periods=mp).max()
    rng = mx - mn
    return pd.Series(np.where(rng != 0, (series - mn) / rng * 100, 50.0), index=series.index)


def cot_zscore(series: pd.Series, window: int = 52) -> pd.Series:
    """Z-score rolling: cuántas desviaciones estándar del promedio histórico."""
    mp = max(13, min(window // 2, 26))
    mean = series.rolling(window, min_periods=mp).mean()
    std  = series.rolling(window, min_periods=mp).std()
    return pd.Series(np.where(std != 0, (series - mean) / std, 0.0), index=series.index)


def consecutive_direction(series: pd.Series) -> int:
    """Cuenta semanas consecutivas de misma dirección (signo positivo/negativo)."""
    if len(series) < 2:
        return 0
    diffs = series.diff().dropna()
    if diffs.empty:
        return 0
    last_sign = 1 if diffs.iloc[-1] > 0 else (-1 if diffs.iloc[-1] < 0 else 0)
    if last_sign == 0:
        return 0
    count = 0
    for d in reversed(diffs.values):
        sign = 1 if d > 0 else (-1 if d < 0 else 0)
        if sign == last_sign:
            count += 1
        else:
            break
    return count * last_sign


def quant_signals(df: pd.DataFrame, window: int) -> dict:
    """Calcula las 5 señales quant. Devuelve dict con toda la info para la UI."""
    if len(df) < max(window // 2, 13):
        return {}

    nc = df["Net_NC"]
    cm = df["Net_CM"]

    ci_nc_series = cot_index(nc, window)
    ci_cm_series = cot_index(cm, window)
    z_nc_series  = cot_zscore(nc, window)

    ci_nc = float(ci_nc_series.iloc[-1]) if pd.notna(ci_nc_series.iloc[-1]) else 50.0
    ci_cm = float(ci_cm_series.iloc[-1]) if pd.notna(ci_cm_series.iloc[-1]) else 50.0
    z_nc  = float(z_nc_series.iloc[-1])  if pd.notna(z_nc_series.iloc[-1])  else 0.0

    # Señal 1: Briese COT Index (specs) — contrarian
    if ci_nc >= 80:
        s1 = ("SELL", "action-sell", "Specs en extremo LARGO. Históricamente mal-posicionados en techos. Buscar SHORT en resistencias.")
    elif ci_nc <= 20:
        s1 = ("BUY", "action-buy", "Specs en extremo CORTO. Alta probabilidad de squeeze al alza. Buscar LONG en soportes.")
    else:
        s1 = ("HOLD", "action-hold", "Sin extremo en specs. Esperar lectura >80 o <20 para actuar.")

    # Señal 2: Smart Money (Commercials) — direccional
    if ci_cm >= 80:
        s2 = ("BUY", "action-buy", "Hedgers en extremo LARGO (smart money acumula). Tendencia alcista probable. Alinearse LONG.")
    elif ci_cm <= 20:
        s2 = ("SELL", "action-sell", "Hedgers en extremo CORTO (smart money distribuye). Tendencia bajista probable. Alinearse SHORT.")
    else:
        s2 = ("HOLD", "action-hold", "Hedgers neutrales. No hay conviction institucional clara.")

    # Señal 3: Z-Score
    if z_nc >= 2.0:
        s3 = ("SELL", "action-sell", f"Posición a +{z_nc:.1f}σ del promedio. Estadísticamente extrema. Reversión probable.")
    elif z_nc <= -2.0:
        s3 = ("BUY", "action-buy", f"Posición a {z_nc:.1f}σ del promedio. Estadísticamente extrema. Reversión probable.")
    else:
        s3 = ("HOLD", "action-hold", f"Posición dentro de ±2σ ({z_nc:+.1f}σ). No hay anomalía estadística.")

    # Señal 4: Divergencia Specs vs Hedgers
    div = ci_nc - ci_cm
    if ci_nc >= 75 and ci_cm <= 25:
        s4 = ("SELL", "action-sell", "DIVERGENCIA MÁXIMA: specs muy largos + hedgers muy cortos. Setup clásico de techo.")
    elif ci_nc <= 25 and ci_cm >= 75:
        s4 = ("BUY", "action-buy", "DIVERGENCIA MÁXIMA: specs muy cortos + hedgers muy largos. Setup clásico de suelo.")
    else:
        s4 = ("HOLD", "action-hold", f"Divergencia moderada ({div:+.0f}). Sin setup contrarian claro.")

    # Señal 5: Momentum (streak)
    streak = consecutive_direction(nc.tail(12))
    if streak >= 3:
        s5 = ("BUY", "action-buy", f"Specs incrementando longs {streak} semanas consecutivas. Momentum alcista confirmado. Seguir la tendencia.")
    elif streak <= -3:
        s5 = ("SELL", "action-sell", f"Specs incrementando shorts {-streak} semanas consecutivas. Momentum bajista confirmado. Seguir la tendencia.")
    else:
        s5 = ("HOLD", "action-hold", f"Sin momentum direccional ({streak:+d} semanas). Esperar 3+ consecutivas.")

    # Agregador: voto mayoritario
    votes = [s[0] for s in [s1, s2, s3, s4, s5]]
    buy_votes = votes.count("BUY")
    sell_votes = votes.count("SELL")
    if buy_votes >= 3:
        agg = ("STRONG BUY", "signal-bull", buy_votes, sell_votes)
    elif sell_votes >= 3:
        agg = ("STRONG SELL", "signal-bear", buy_votes, sell_votes)
    elif buy_votes > sell_votes:
        agg = ("LEAN BUY", "signal-bull", buy_votes, sell_votes)
    elif sell_votes > buy_votes:
        agg = ("LEAN SELL", "signal-bear", buy_votes, sell_votes)
    else:
        agg = ("NEUTRAL", "signal-neutral", buy_votes, sell_votes)

    return dict(
        ci_nc=ci_nc, ci_cm=ci_cm, z_nc=z_nc, divergence=div, streak=streak,
        s1=s1, s2=s2, s3=s3, s4=s4, s5=s5, agg=agg,
        ci_nc_series=ci_nc_series, ci_cm_series=ci_cm_series, z_nc_series=z_nc_series,
    )


def kpi_signals(df, window):
    """KPIs simples para las metric-cards superiores."""
    if len(df) < 6:
        return {}
    lat, prv = df.iloc[-1], df.iloc[-6]
    ci = cot_index(df["Net_NC"], window).iloc[-1]
    if pd.isna(ci):
        ci = 50.0
    prv_oi, lat_oi = prv["OI"], lat["OI"]
    if pd.notna(prv_oi) and pd.notna(lat_oi) and prv_oi != 0:
        oi_chg = (lat_oi - prv_oi) / prv_oi * 100
    else:
        oi_chg = 0.0
    sig, cls = (("ALCISTA","signal-bull") if ci >= 75 else
                ("BAJISTA","signal-bear") if ci <= 25 else ("NEUTRAL","signal-neutral"))
    return dict(signal=sig, signal_cls=cls,
                nc_net=lat["Net_NC"], cm_net=lat["Net_CM"],
                nc_trend="↑ Subiendo" if lat["Net_NC"] > prv["Net_NC"] else "↓ Bajando",
                cm_trend="↑ Subiendo" if lat["Net_CM"] > prv["Net_CM"] else "↓ Bajando",
                cot_index=ci, oi=lat_oi if pd.notna(lat_oi) else 0, oi_change=oi_chg,
                last_date=lat["Date"].strftime("%d %b %Y"))


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
        subplot_titles=[f"Posición Neta — {nc_label}", f"Posición Neta — {cm_label}", "COT Index (Specs vs Hedgers)"])
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
    ci_nc = cot_index(nc, window=window)
    ci_cm = cot_index(cm, window=window)
    fig.add_hrect(y0=75, y1=100, fillcolor="#3fb950", opacity=0.07, layer="below", line_width=0, row=3, col=1)
    fig.add_hrect(y0=0,  y1=25,  fillcolor="#f85149", opacity=0.07, layer="below", line_width=0, row=3, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=ci_nc, name="COT Idx Specs",
        line=dict(color="#58a6ff", width=2)), row=3, col=1)
    fig.add_trace(go.Scatter(x=df["Date"], y=ci_cm, name="COT Idx Hedgers",
        line=dict(color="#ffa657", width=1.6, dash="dot")), row=3, col=1)
    for lvl, color in [(80,"#3fb950"),(50,"#8b949e"),(20,"#f85149")]:
        fig.add_hline(y=lvl, line=dict(color=color, dash="dash", width=1), row=3, col=1)
    _theme(fig, height=720)
    fig.update_yaxes(title_text="Contratos", row=1, col=1, **_AX)
    fig.update_yaxes(title_text="Contratos", row=2, col=1, **_AX)
    fig.update_yaxes(title_text="0–100", row=3, col=1, range=[0,100], **_AX)
    for ann in fig.layout.annotations:
        ann.font.update(family="IBM Plex Mono", size=11, color="#8b949e")
    return fig

def chart_zscore(df, window):
    z = cot_zscore(df["Net_NC"], window)
    fig = go.Figure()
    fig.add_hrect(y0=2, y1=4, fillcolor="#f85149", opacity=0.09, layer="below", line_width=0)
    fig.add_hrect(y0=-4, y1=-2, fillcolor="#3fb950", opacity=0.09, layer="below", line_width=0)
    fig.add_trace(go.Scatter(x=df["Date"], y=z, name="Z-Score Specs",
        line=dict(color="#58a6ff", width=2), fill="tozeroy", fillcolor="rgba(88,166,255,0.08)"))
    for lvl, color, dash in [(2,"#f85149","dash"),(0,"#8b949e","dot"),(-2,"#3fb950","dash")]:
        fig.add_hline(y=lvl, line=dict(color=color, dash=dash, width=1))
    fig.update_yaxes(range=[-4,4])
    return _theme(fig, height=300, yaxis_title="σ del promedio")

def chart_divergence(df, window, nc_label, cm_label):
    ci_nc = cot_index(df["Net_NC"], window)
    ci_cm = cot_index(df["Net_CM"], window)
    div = ci_nc - ci_cm
    fig = go.Figure()
    fig.add_hrect(y0=50,  y1=100, fillcolor="#f85149", opacity=0.08, layer="below", line_width=0)
    fig.add_hrect(y0=-100, y1=-50, fillcolor="#3fb950", opacity=0.08, layer="below", line_width=0)
    fig.add_trace(go.Scatter(x=df["Date"], y=div, name=f"{nc_label} − {cm_label}",
        line=dict(color="#d2a8ff", width=2), fill="tozeroy", fillcolor="rgba(210,168,255,0.08)"))
    for lvl, color, dash in [(50,"#f85149","dash"),(0,"#8b949e","dot"),(-50,"#3fb950","dash")]:
        fig.add_hline(y=lvl, line=dict(color=color, dash=dash, width=1))
    fig.update_yaxes(range=[-100,100])
    return _theme(fig, height=300, yaxis_title="Divergencia")

def chart_gross(df, nc_label, cm_label):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["NC_Long"],  name=f"{nc_label} Long",  line=dict(color="#3fb950", width=1.5)))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["NC_Short"], name=f"{nc_label} Short", line=dict(color="#f85149", width=1.5)))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["CM_Long"],  name=f"{cm_label} Long",  line=dict(color="#79c0ff", width=1.5, dash="dot")))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["CM_Short"], name=f"{cm_label} Short", line=dict(color="#ffa657", width=1.5, dash="dot")))
    return _theme(fig, height=340, yaxis_title="Contratos")

def chart_oi(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["OI"], name="Open Interest",
        fill="tozeroy", line=dict(color="#58a6ff", width=2), fillcolor="rgba(88,166,255,0.09)"))
    fig.add_trace(go.Scatter(x=df["Date"], y=df["OI"].rolling(13).mean(),
        name="MA 13w", line=dict(color="#ffa657", width=1.5, dash="dash")))
    return _theme(fig, height=300, yaxis_title="Contratos")

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


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("<div style='font-family:IBM Plex Mono;font-size:1.05rem;font-weight:600;color:#58a6ff;padding:8px 0 16px'>⬡ COT Dashboard</div>", unsafe_allow_html=True)

    mode = st.radio("MODO", ["Preset", "Personalizado"], horizontal=True,
                    help="Preset = instrumentos comunes con config óptima · Personalizado = elige cualquiera de los 500+ mercados del CFTC")

    preset_info = None
    market_key = None
    if mode == "Preset":
        keys = list(MARKETS_PRESET.keys())
        default_idx = keys.index("EUR/USD  ·  Euro FX") if "EUR/USD  ·  Euro FX" in keys else 0
        market_key = st.selectbox("INSTRUMENTO", keys, index=default_idx)
        preset_info = MARKETS_PRESET[market_key]
        if preset_info.get("is_sep"):
            st.info("← Elige un instrumento (no un separador)")
            st.stop()
        default_report = preset_info["report"]
    else:
        default_report = "Legacy — Futures Only"

    report_key = st.selectbox("TIPO DE REPORTE", list(REPORTS.keys()),
                              index=list(REPORTS.keys()).index(default_report),
                              help="Legacy → Forex/Commodities · TFF → Índices/Tasas/Cripto · Disaggregated → Energía/Metales/Granos")

    years_back = st.slider("AÑOS DE HISTORIA", 1, 20, 5,
                           help="Más años = más contexto para señales quant")
    cot_win    = st.slider("VENTANA COT INDEX (semanas)", 13, 260, 52, step=13,
                           help="52w = estándar Briese · 156w = ciclo 3 años · 260w = ciclo 5 años")

    st.markdown("---")
    rcfg = REPORTS[report_key]
    st.markdown(f"<div class='info-box'>{rcfg['description']}<br><br>"
                f"<strong>Speculators:</strong> {rcfg['nc_label']}<br>"
                f"<strong>Hedgers:</strong> {rcfg['cm_label']}</div>", unsafe_allow_html=True)
    if mode == "Preset" and preset_info and preset_info.get("report") != report_key:
        st.warning(f"💡 Para este instrumento se recomienda: **{preset_info['report']}**")
    st.markdown("<div style='font-family:IBM Plex Mono;font-size:.65rem;color:#8b949e;margin-top:12px'>Datos: CFTC · Actualización viernes</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
cy = datetime.now().year
years = list(range(cy - max(years_back, 2) + 1, cy + 1))

st.markdown("<div class='main-title'>📊 Commitment of Traders · Quant Signals</div>", unsafe_allow_html=True)

with st.spinner(f"⟳  Descargando {years_back} años de datos CFTC ({years[0]}–{years[-1]})..."):
    raw, download_logs = load_cot(years, rcfg["prefix"])

# Modo Personalizado: selector de mercado basado en los datos cargados
if mode == "Personalizado":
    if raw.empty:
        st.error("No se pudo descargar datos para este tipo de reporte.")
        with st.expander("📡  Log de descarga", expanded=True):
            for log in download_logs:
                st.markdown(f"`{log}`")
        st.stop()
    avail = available_markets(raw)
    st.markdown("<div class='section-header'>Modo Personalizado — Elige cualquier mercado del CFTC</div>", unsafe_allow_html=True)
    col_a, col_b = st.columns([1, 2])
    with col_a:
        q_text = st.text_input("🔍 Filtrar", placeholder="GOLD, CRUDE, S&P, PESO...")
    with col_b:
        filtered = [m for m in avail if q_text.upper() in m.upper()] if q_text else avail
        if not filtered:
            st.warning(f"Ningún mercado matchea '{q_text}'. Mercados totales: {len(avail)}")
            st.stop()
        manual = st.selectbox(f"Mercado ({len(filtered)} disponibles)", filtered, key="custom_market")
    market_search = [manual]
    market_display = manual
else:
    market_search = preset_info["names"]
    market_display = market_key.strip()

st.markdown(f"<div class='subtitle'>CFTC · {market_display} · {report_key} · {years_back}y · COT window {cot_win}w</div>",
            unsafe_allow_html=True)

df = parse_cot(raw, market_search, rcfg)
has_data = not df.empty

with st.expander("📡  Log de descarga & diagnóstico", expanded=not has_data):
    for log in download_logs:
        st.markdown(f"`{log}`")
    if not raw.empty:
        nc_col = _name_col(raw)
        total_markets = raw[nc_col].nunique() if nc_col else 0
        st.markdown(f"`Total filas: {len(raw):,} · Mercados disponibles: {total_markets}`")
        idx = _col_index(raw)
        resolved = {
            "Name":    _find_col(idx, "Market_and_Exchange_Names", "Market and Exchange Names"),
            "Date":    _find_col(idx, "Report_Date_as_YYYY-MM-DD", "As of Date in Form YYYY-MM-DD",
                                  "Report_Date_as_MM_DD_YYYY"),
            "OI":      _find_col(idx, "Open_Interest_All"),
            "NC_Long": _find_col(idx, rcfg["long_nc"]),
            "CM_Long": _find_col(idx, rcfg["long_cm"]),
            "NR_Long": _find_col(idx, rcfg["long_nr"], "Nonreportable_Positions_Long_All", "NonRept_Positions_Long_All"),
        }
        st.markdown("**Columnas resueltas:**")
        for kk, vv in resolved.items():
            icon = "✅" if vv else "❌"
            st.markdown(f"`{icon} {kk:<8} → {vv or '(no encontrada)'}`")
        if has_data:
            st.markdown(f"**Mercado parseado:** `{len(df)} filas · {df['Date'].min().date()} → {df['Date'].max().date()}`")

if not has_data:
    st.error(f"**Sin datos** para `{market_display}` en `{report_key}`.")
    if not raw.empty:
        avail = available_markets(raw)
        st.markdown("#### 🔍 Busca el nombre exacto en modo Personalizado")
        tokens = [t for t in _normalize(market_display).replace("-", " ").split() if len(t) > 2]
        hits = [m for m in avail if any(t in _normalize(m) for t in tokens)][:40]
        if hits:
            st.dataframe(pd.DataFrame({"Posibles coincidencias": hits}), width='stretch')
        st.info("💡 Cambia MODO a 'Personalizado' en el sidebar para ver los 500+ mercados.")
    st.stop()

# ─── KPIs ────────────────────────────────────────────────────────────────────────
k = kpi_signals(df, cot_win)
st.markdown("<div class='section-header'>Resumen de Posicionamiento</div>", unsafe_allow_html=True)
c1,c2,c3,c4,c5 = st.columns(5)
with c1:
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>Señal COT</div>
        <div style='padding:4px 0'><span class='{k["signal_cls"]}'>{k["signal"]}</span></div>
        <div class='metric-delta neutral'>{k["last_date"]}</div></div>""", unsafe_allow_html=True)
with c2:
    v=k["nc_net"]; cls="positive" if v>0 else "negative"
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>Spec Net</div>
        <div class='metric-value {cls}'>{v:+,.0f}</div>
        <div class='metric-delta neutral'>{k["nc_trend"]}</div></div>""", unsafe_allow_html=True)
with c3:
    v=k["cm_net"]; cls="positive" if v>0 else "negative"
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>Hedger Net</div>
        <div class='metric-value {cls}'>{v:+,.0f}</div>
        <div class='metric-delta neutral'>{k["cm_trend"]}</div></div>""", unsafe_allow_html=True)
with c4:
    idx_v=k["cot_index"]; cls="positive" if idx_v>66 else("negative" if idx_v<33 else "neutral")
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>COT Index</div>
        <div class='metric-value {cls}'>{idx_v:.1f}</div>
        <div class='metric-delta neutral'>Ventana {cot_win}w</div></div>""", unsafe_allow_html=True)
with c5:
    oi=k["oi"]; oic=k["oi_change"]; cls="positive" if oic>0 else "negative"
    st.markdown(f"""<div class='metric-card'><div class='metric-label'>Open Interest</div>
        <div class='metric-value neutral'>{oi:,.0f}</div>
        <div class='metric-delta {cls}'>{oic:+.1f}% (5s)</div></div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─── CHART PRINCIPAL ─────────────────────────────────────────────────────────────
st.markdown("<div class='section-header'>Posiciones Netas & COT Index</div>", unsafe_allow_html=True)
st.plotly_chart(chart_net_cot(df, cot_win, rcfg["nc_label"], rcfg["cm_label"]),
                width='stretch', config={"displayModeBar": False})

# ─── SEÑALES QUANT ───────────────────────────────────────────────────────────────
q = quant_signals(df, cot_win)
if q:
    st.markdown("<div class='section-header'>🎯 Señales Cuantitativas — 5 Modelos</div>", unsafe_allow_html=True)

    agg = q["agg"]
    st.markdown(f"""
    <div style='background:#161b22;border:1px solid #21262d;border-radius:6px;padding:16px 20px;margin-bottom:16px;text-align:center'>
        <div style='font-family:IBM Plex Mono;font-size:.7rem;color:#8b949e;text-transform:uppercase;letter-spacing:.12em;margin-bottom:8px'>Consenso de los 5 modelos</div>
        <span class='{agg[1]}' style='font-size:1rem'>{agg[0]}</span>
        <div style='font-family:IBM Plex Mono;font-size:.75rem;color:#8b949e;margin-top:8px'>
            {agg[2]} votos BUY · {agg[3]} votos SELL · {5-agg[2]-agg[3]} HOLD
        </div>
    </div>
    """, unsafe_allow_html=True)

    def _card(title, value, desc, rule, action, action_cls):
        return f"""<div class='quant-card'>
            <div class='quant-title'>{title}</div>
            <div class='quant-value'>{value}</div>
            <div class='quant-rule'>{rule}</div>
            <div class='quant-desc'>{desc}</div>
            <div class='quant-action {action_cls}'>→ {action}</div>
        </div>"""

    r1c1, r1c2, r1c3 = st.columns(3)
    with r1c1:
        st.markdown(_card("1. Briese COT Index (Specs)",
                          f"{q['ci_nc']:.1f}",
                          q['s1'][2],
                          "≥80 → SELL  ·  ≤20 → BUY",
                          q['s1'][0], q['s1'][1]), unsafe_allow_html=True)
    with r1c2:
        st.markdown(_card("2. Smart Money (Hedgers)",
                          f"{q['ci_cm']:.1f}",
                          q['s2'][2],
                          "≥80 → BUY  ·  ≤20 → SELL",
                          q['s2'][0], q['s2'][1]), unsafe_allow_html=True)
    with r1c3:
        st.markdown(_card("3. Z-Score Specs",
                          f"{q['z_nc']:+.2f}σ",
                          q['s3'][2],
                          "≥+2σ → SELL  ·  ≤−2σ → BUY",
                          q['s3'][0], q['s3'][1]), unsafe_allow_html=True)

    r2c1, r2c2 = st.columns(2)
    with r2c1:
        st.markdown(_card("4. Divergencia Specs vs Hedgers",
                          f"{q['divergence']:+.0f}",
                          q['s4'][2],
                          "Specs≥75 & Hedgers≤25 → SELL  ·  inverso → BUY",
                          q['s4'][0], q['s4'][1]), unsafe_allow_html=True)
    with r2c2:
        streak_txt = f"{q['streak']:+d} semanas"
        st.markdown(_card("5. Momentum (Streak)",
                          streak_txt,
                          q['s5'][2],
                          "≥3 consecutivas ↑ → BUY  ·  ≤−3 ↓ → SELL",
                          q['s5'][0], q['s5'][1]), unsafe_allow_html=True)

    st.markdown("<div class='section-header'>Gráficas Quant — Z-Score & Divergencia</div>", unsafe_allow_html=True)
    qc1, qc2 = st.columns(2)
    with qc1:
        st.plotly_chart(chart_zscore(df, cot_win), width='stretch', config={"displayModeBar": False})
    with qc2:
        st.plotly_chart(chart_divergence(df, cot_win, rcfg["nc_label"], rcfg["cm_label"]),
                        width='stretch', config={"displayModeBar": False})
else:
    st.info(f"Se necesitan al menos {max(cot_win//2, 13)} semanas de datos para calcular señales quant. Sube el slider de AÑOS DE HISTORIA.")

# ─── POSICIONES BRUTAS + DONUT + OI ──────────────────────────────────────────────
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

with st.expander("📚  Guía: Cómo interpretar los 5 modelos quant"):
    st.markdown("""
**1. Briese COT Index (Specs)** — Indicador clásico de Stephen Briese (*The Commitments of Traders Bible*). Normaliza la posición neta de especuladores en un percentil 0–100 sobre una ventana rolling. Los specs son típicamente trend-followers mal posicionados en los extremos, por eso se usa como señal **contrarian**.

**2. Smart Money (Hedgers)** — Mismo cálculo pero sobre los hedgers comerciales. Los productores/usuarios físicos conocen el precio justo del subyacente. Cuando acumulan largos agresivamente, suelen estar anticipando un piso de precios. Se usa como señal **direccional** (seguir a los hedgers).

**3. Z-Score Specs** — Normalización estadística. Mide a cuántas desviaciones estándar está la posición neta actual del promedio de N semanas. Más riguroso que el COT Index porque pondera la dispersión histórica. Lecturas >+2σ o <−2σ son estadísticamente extremas (95% confianza).

**4. Divergencia Specs vs Hedgers** — Calcula `COT_Index_Specs − COT_Index_Hedgers`. El setup más potente ocurre cuando ambos grupos están simultáneamente en extremos opuestos: specs máximamente largos + hedgers máximamente cortos = techo probable (y viceversa). Históricamente da mejores señales que cualquiera de los dos por separado.

**5. Momentum (Streak)** — Cuenta semanas consecutivas de incremento o decremento en la posición neta de specs. 3+ semanas consecutivas en la misma dirección confirman inercia de flujo institucional. Se usa para **alinearse con la tendencia**, no contrariarla.

---
**Lectura conjunta**: las señales 1, 3, 4 son **contrarian**. Las señales 2, 5 son **trend-following**. Cuando 3+ modelos dan la misma señal, la conviction es alta. Cuando hay conflicto, la posición está en transición — conviene **esperar**.

> ⚠️ Solo informativo. No constituye asesoramiento financiero. El COT es lagged 3 días (datos del martes publicados el viernes). Siempre combinar con análisis técnico de precio y gestión de riesgo estricta.
""")

st.markdown("---")
st.markdown("""<div style='text-align:center;font-family:IBM Plex Mono;font-size:.68rem;color:#8b949e'>
Datos públicos CFTC · Actualización semanal (viernes) · Solo informativo</div>""",
            unsafe_allow_html=True)
