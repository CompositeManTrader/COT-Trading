# 📊 COT Dashboard — Commitment of Traders Visualizer

Dashboard interactivo en **Streamlit** para visualizar el reporte semanal **COT (Commitment of Traders)** publicado por el CFTC.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io)

---

## 🚀 Deploy en Streamlit Cloud (gratis)

1. **Fork este repo** en GitHub
2. Ve a [share.streamlit.io](https://share.streamlit.io) → *New app*
3. Conecta tu repo → selecciona `app.py`
4. Deploy ✅

---

## 💻 Correr localmente

```bash
git clone https://github.com/TU_USUARIO/cot-dashboard.git
cd cot-dashboard
pip install -r requirements.txt
streamlit run app.py
```

---

## ¿Qué es el COT?

El **Commitment of Traders** es un reporte semanal publicado cada viernes por la **CFTC** (Commodity Futures Trading Commission, el regulador de futuros en EE.UU.).

Muestra las **posiciones abiertas en futuros** clasificadas en tres grupos:

| Grupo | Quiénes son | Comportamiento |
|---|---|---|
| **Non-Commercial (Specs)** | Hedge funds, CTAs, asset managers | Seguidores de tendencia, mueven el mercado |
| **Commercial (Hedgers)** | Productores, empresas con exposición real | Contrarian natural, conocen el precio justo |
| **Non-Reportable (Small)** | Retail, traders pequeños | Señal contrarian débil |

---

## 📐 Indicadores del Dashboard

### 1. Posición Neta
```
Net Position = Longs - Shorts
```
Positivo → sesgo alcista del grupo | Negativo → sesgo bajista

### 2. COT Index
```
COT Index = (Net_actual - Net_min_N) / (Net_max_N - Net_min_N) × 100
```
Normaliza la posición neta en una ventana rolling de N semanas (defecto: 52).
- **>75** → Especuladores extremadamente largos → señal alcista
- **<25** → Especuladores extremadamente cortos → señal bajista
- **25-75** → Zona neutral

---

## 📈 Estrategias de Trading con COT

### Estrategia 1 — COT Extremes (Contrarian)
Cuando los **especuladores** alcanzan posiciones extremas, el mercado suele revertir:
- COT Index > 80 + precio en resistencia → buscar shorts
- COT Index < 20 + precio en soporte → buscar longs

### Estrategia 2 — Comerciales vs Especuladores (Divergencia)
Cuando **hedgers** y **especuladores** divergen al máximo:
- Specs muy largos + Comerciales muy cortos → techo probable
- Specs muy cortos + Comerciales muy largos → suelo probable

### Estrategia 3 — Momentum de Posición
Seguir el cambio de dirección semanal de specs:
- Specs incrementando longs por 3+ semanas consecutivas → alinearse alcista
- Specs incrementando shorts por 3+ semanas consecutivas → alinearse bajista

### Estrategia 4 — Open Interest + Precio
- Precio sube + OI sube → tendencia saludable, continuar
- Precio sube + OI baja → rally débil, posible reversión

---

## 📁 Estructura del Proyecto

```
cot-dashboard/
├── app.py              # Aplicación principal Streamlit
├── requirements.txt    # Dependencias Python
└── README.md           # Este archivo
```

---

## 🔗 Fuentes de Datos

- [CFTC - Commitment of Traders Reports](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm)
- Datos históricos desde 1986 disponibles gratuitamente

---

> ⚠️ Solo informativo. No constituye asesoramiento financiero.
