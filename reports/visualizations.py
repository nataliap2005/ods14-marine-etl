import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from DB.queries import (
    MICRO_BY_REGION_TOP10,
    DEPTH_BINS_EFFECT_TOP10,
    CRITICAL_ZONES_HIGH,
    REGION_HOTSPOTS,
    METHOD_EFFECTS_TOP10,
    CONC_CLASS_BY_REGION_TOP10,
    PAIRED_OBSERVATIONS,
    YEAR_TREND,
    OCEAN_RANKING_TOTAL,
    ORGANIZATION_ACTIVITY,
    CRITICAL_ZONES_HIGHHIGH,
    CRITICAL_ZONES_LOWBIODIV_HIGHCONT,
    SAMPLES_PER_YEAR,
    METHODS_BY_YEAR_COUNTS,
    METHODS_BY_WATERSAMPLEDEPTH,
    MARINE_SETTING_RANKING,
    MONTHLY_TREND
)

sns.set(style="whitegrid")

# ---------------------------
# Helpers
# ---------------------------
def _ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def _run_df(engine, query, params=None):
    return pd.read_sql(query, con=engine, params=params or {})

def _save_table(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)

# ---------------------------
# Plot functions
# ---------------------------
def plot_region_avgs(df, out_path):
    df = df[df["region"].notna()]
    df.plot(kind="bar", x="region", y="avg_microplastics", figsize=(10,6))
    plt.title("Top 10 Average Microplastics Measurement by Region")
    plt.ylabel("Measurement")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_depth_bands(df, out_path):
    df = df[df["depth_band"] != "Unknown"]
    df.plot(kind="bar", x="depth_band", y="avg_microplastics", figsize=(10,6))
    plt.title("Top 10 Average Microplastics by Depth Band")
    plt.ylabel("Measurement")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_method_mesh(df, out_path):
    df.plot(kind="bar", x="sampling_method", y="avg_microplastics", figsize=(10,6))
    plt.title("Top 10 Measurement by Sampling Method")
    plt.ylabel("Average Measurement")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_critical_zones(df, out_path):
    df = df[df["region"].notna()]
    _save_table(df, out_path)

def plot_year_trend(df, out_path):
    """Línea temporal del total por año con el año pico resaltado."""
    if df.empty:
        return
    x = df["year"].astype(int)
    y = df["total_microplastics"].astype(float)

    # índice del valor máximo
    peak_idx = y.idxmax()
    peak_year = x.loc[peak_idx]
    peak_val  = y.loc[peak_idx]

    plt.figure(figsize=(10,5))
    plt.plot(x, y, marker="o", linewidth=2)
    
    # resaltar el pico
    plt.scatter([peak_year], [peak_val], s=160, edgecolor="k", zorder=3)
    plt.text(peak_year, peak_val, f"  pico: {int(peak_year)}\n  {peak_val:,.0f}", va="bottom", fontsize=9)
    plt.title("Total of microplastics by Year (Peak highlighted)")
    plt.xlabel("Year")
    plt.ylabel("Total")
    ax = plt.gca()
    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()
    


def plot_ocean_donut(df, out_path, top=6):
    """Dona de participación por océano sobre el total (Top N + 'Others')."""
    d = df.dropna(subset=["ocean", "total_microplastics"]).copy()
    if d.empty:
        return

    total = d["total_microplastics"].sum()
    d["share"] = d["total_microplastics"] / total
    d = d.sort_values("share", ascending=False)

    top_df = d.head(top)
    others_share = d["share"][top:].sum()
    pie_df = top_df[["ocean", "share"]].copy()
    if others_share > 0:
        pie_df = pd.concat([pie_df, pd.DataFrame([{"ocean": "Others", "share": others_share}])],
                           ignore_index=True)

    plt.figure(figsize=(7,7))
    wedges, _ = plt.pie(pie_df["share"], startangle=90, wedgeprops=dict(width=0.35))  # dona
    # leyenda
    labels = [f'{o} ({s*100:.1f}%)' for o, s in zip(pie_df["ocean"], pie_df["share"])]
    plt.legend(wedges, labels, loc="center left", bbox_to_anchor=(1, 0.5))
    plt.title("participation by Ocean (Top 6 + Others)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_org_lollipop(df, out_path, top=12):
    d = df.sort_values("n_samples", ascending=True).tail(top)
    y = range(len(d))
    plt.figure(figsize=(10,6))
    plt.hlines(y, 0, d["n_samples"], color="gray", alpha=0.6)
    plt.plot(d["n_samples"], y, "o")
    plt.yticks(y, d["organization"])
    plt.xlabel("Number of samples"); plt.title("Activy by organization(Top)")
    plt.tight_layout(); plt.savefig(out_path, dpi=140); plt.close()
    
def plot_critical_highhigh(df, out_path):
    """Regiones con alta biodiversidad y alta contaminación."""
    if df.empty: return
    plt.figure(figsize=(10,6))
    plt.bar(df["region"], df["n_high_high"], color="darkred")
    plt.xticks(rotation=45)
    plt.title("Zonas críticas: Alta biodiversidad + Alta contaminación")
    plt.ylabel("Número de ubicaciones")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_critical_lowbiodiv_highcont(df, out_path):
    """Regiones con baja biodiversidad y alta contaminación."""
    if df.empty: return
    plt.figure(figsize=(10,6))
    plt.bar(df["region"], df["n_low_high"], color="orange")
    plt.xticks(rotation=45)
    plt.title("Critical Zones: Low Biodiversity + High Pollution")
    plt.ylabel("number of locations")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_samples_per_year(df: pd.DataFrame, out_path: str):
    if df.empty:
        return
    d = df.copy()
    d["year"] = d["year"].astype(int)
    d = d.sort_values("year")

    plt.figure(figsize=(10,5))
    # barras
    plt.bar(d["year"], d["n_samples"], alpha=0.7)
    # línea suave por encima (misma escala)
    plt.plot(d["year"], d["n_samples"], marker="o", linewidth=2)
    plt.title("Número de muestreos por año")
    plt.xlabel("Año"); plt.ylabel("Muestreos (conteo)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_methods_by_year_area(df: pd.DataFrame, out_path: str, top_methods=6):
    """
    Área apilada con los métodos más usados (Top-N); el resto va a 'Others'.
    """
    if df.empty:
        return
    d = df.copy()
    d["year"] = d["year"].astype(int)

    # Top-N métodos por volumen total
    totals = d.groupby("sampling_method")["n_samples"].sum().sort_values(ascending=False)
    keep = set(totals.head(top_methods).index)
    d["method_grp"] = d["sampling_method"].where(d["sampling_method"].isin(keep), other="Others")

    # Pivot year x method_grp
    pvt = d.groupby(["year","method_grp"])["n_samples"].sum().reset_index()
    pvt = pvt.pivot(index="year", columns="method_grp", values="n_samples").fillna(0).sort_index()

    plt.figure(figsize=(12,6))
    pvt.plot.area(ax=plt.gca())
    plt.title("Métodos de muestreo por año (área apilada, Top métodos)")
    plt.xlabel("Año"); plt.ylabel("Muestreos (conteo)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()
    

def plot_depth_vs_method_heatmap(df, out_path, top_methods=8):
    if df.empty:
        return

       # orden lógico fijo
    order = ["0–5m","5–20m","20–50m","50–200m","200m+", "Unknown"]
    df = df.copy()
    df["depth_band"] = pd.Categorical(df["depth_band"], categories=order, ordered=True)
    df["sampling_method"] = df["sampling_method"].str.strip().str.lower()

    # top métodos
    top = (df.groupby("sampling_method")["n_samples"]
             .sum().sort_values(ascending=False).head(top_methods).index)
    d = df[df["sampling_method"].isin(top)]

    # matriz numérica
    pivot = (d.pivot_table(index="depth_band", columns="sampling_method",
                           values="n_samples", aggfunc="sum", fill_value=0)
               .sort_index())


    # Heatmap
    plt.figure(figsize=(12, 6))
    sns.heatmap(pivot, annot=True, fmt=".0f", cmap="Blues")
    plt.title("Métodos de muestreo usados por banda de profundidad")
    plt.xlabel("Método de muestreo")
    plt.ylabel("Banda de profundidad")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()
    
def plot_marine_setting_ranking(df, out_path):
    """Ranking de avg_microplastics por marine_setting."""
    if df.empty: return
    df = df.sort_values("avg_microplastics", ascending=True)
    plt.figure(figsize=(10, 7))
    plt.barh(df["marine_setting"], df["avg_microplastics"], color="teal")
    plt.xlabel("Average Microplastics Measurement")
    plt.ylabel("Marine Setting")
    plt.title("Top Marine Settings by Average Microplastic Contamination")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_monthly_trend(df, out_path):
    """Muestra la tendencia estacional de contaminación promedio."""
    if df.empty: return
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    df = df.set_index("month").reindex(range(1,13)).reset_index()
    
    plt.figure(figsize=(10, 5))
    plt.bar(df["month"], df["avg_microplastics"], color="skyblue")
    plt.xticks(ticks=df["month"], labels=[month_names[m-1] for m in df["month"]], rotation=45)
    plt.xlabel("Month")
    plt.ylabel("Average Microplastics Measurement")
    plt.title("Seasonal Trend of Average Microplastic Contamination")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()


def plot_region_hotspots(df, out_path):
    df = df[df["region"].notna()]

    # Agrupar por región y sumar mediciones literal
    df_plot = df.groupby("region")["measurement"].sum().sort_values(ascending=False).head(10).reset_index()

    plt.figure(figsize=(10,6))
    plt.bar(df_plot["region"], df_plot["measurement"], color="skyblue")
    plt.xticks(rotation=45)
    
    plt.ylabel("Total Microplastics Measurement")
    plt.xlabel("Region")
    plt.title("Top 10 Regions by Total Microplastics Measurement")
    
    # Formatear eje y para mostrar números grandes legibles
    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))

    # Mostrar los valores exactos encima de cada barra
    for idx, row in df_plot.iterrows():
        plt.text(idx, row["measurement"], f'{row["measurement"]:,.0f}', ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_conc_matrix(df, out_path):
    df = df.dropna(subset=["avg_measurement", "class_text", "region"])
    # Top 20 regiones por conteo total
    region_totals = df.groupby("region")["n_samples"].sum().sort_values(ascending=False)
    top_regions = region_totals.head(20).index
    df = df[df["region"].isin(top_regions)]

    # Orden fijo de concentración
    class_order = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
    df['class_text'] = pd.Categorical(df['class_text'], categories=class_order, ordered=True)

    df_pivot = df.pivot(index="region", columns="class_text", values="avg_measurement").fillna(0)

    plt.figure(figsize=(12,8))
    sns.heatmap(df_pivot, cmap="viridis", annot=True, fmt=".1f")
    plt.title("Top Concentration Class by Region (Top 20 regiones)")
    plt.ylabel("Region")
    plt.xlabel("Concentration Class")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

    csv_path = out_path.replace(".png", ".csv")
    _save_table(df, csv_path)

def plot_species_micro_map(df: pd.DataFrame, out_path: str, top_n=200):
    """
    Global map mostrando microplastics (rojo) y especies (verde) solo para los puntos más importantes.
    top_n: número máximo de puntos a mostrar por cada categoría
    """
    import numpy as np
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    df = df.dropna(subset=["latitude", "longitude"])
    if df.empty:
        print("No hay datos válidos para mostrar.")
        return

    # Convertir coordenadas a float y depurar
    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)
    df = df[(df["latitude"] >= -90) & (df["latitude"] <= 90)]
    df["longitude"] = df["longitude"].apply(lambda x: x if x <= 180 else x - 360)
    df = df[(df["longitude"] >= -180) & (df["longitude"] <= 180)]

    # Seleccionar top_n por species_count
    df_species = df[df["species_count"].notna()]
    df_species = df_species.sort_values("species_count", ascending=False).head(top_n)

    # Seleccionar top_n por measurement
    df_micro = df[df["measurement"].notna()]
    df_micro = df_micro.sort_values("measurement", ascending=False).head(top_n)

    # Tamaños proporcionales
    sizes_species = np.clip(df_species["species_count"], 0, None) * 5
    sizes_species = np.clip(sizes_species, 5, 150)

    sizes_species = np.log1p(df_species["species_count"]) * 20
    sizes_micro = np.log1p(df_micro["measurement"]) * 50

    # Crear mapa
    plt.figure(figsize=(16,8))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_global()
    ax.coastlines(linewidth=0.8)
    ax.add_feature(cfeature.LAND, facecolor="#f0f0f0")
    ax.add_feature(cfeature.OCEAN, facecolor="#a6cee3")
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.3, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Scatter especies (verde)
    ax.scatter(
        df_species["longitude"], df_species["latitude"],
        s=sizes_species,
        c="green",
        alpha=0.7,
        edgecolor='none',
        transform=ccrs.PlateCarree(),
        label="Species Count"
    )

    # Scatter microplastics (rojo)
    ax.scatter(
        df_micro["longitude"], df_micro["latitude"],
        s=sizes_micro,
        c="red",
        alpha=0.7,
        edgecolor='none',
        transform=ccrs.PlateCarree(),
        label="Microplastics"
    )

    plt.legend(loc="upper right")
    plt.title("Top Locations: Species (verde) vs Microplastics (rojo)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.show()
# ---------------------------
# Generate All Figures
# ---------------------------
def generate_all_figures(engine, start_date=None, end_date=None, save_dir="reports/figures", also_show=False):
    _ensure_dir(save_dir)
    params = {"start_date": start_date, "end_date": end_date}

    # 1) Region averages
    df_region = _run_df(engine, MICRO_BY_REGION_TOP10, params)
    plot_region_avgs(df_region, os.path.join(save_dir, "01_region_avgs.png"))

    # 2) Depth bands
    df_depth = _run_df(engine, DEPTH_BINS_EFFECT_TOP10, params)
    plot_depth_bands(df_depth, os.path.join(save_dir, "02_depth_bands.png"))

    # 3) Critical zones SUM
    df_hc = _run_df(engine, CRITICAL_ZONES_HIGH, params)
    plot_critical_zones(df_hc, os.path.join(save_dir, "03_critical_zones_high.csv"))

    # 4) Region hotspots
    df_hot = _run_df(engine, REGION_HOTSPOTS, params)
    plot_region_hotspots(df_hot, os.path.join(save_dir, "04_region_hotspots.png"))

    # 5) Method effects
    df_method = _run_df(engine, METHOD_EFFECTS_TOP10, params)
    plot_method_mesh(df_method, os.path.join(save_dir, "05_method_mesh.png"))

    # 6) Concentration class by region
    df_matrix = _run_df(engine, CONC_CLASS_BY_REGION_TOP10, params)
    plot_conc_matrix(df_matrix, os.path.join(save_dir, "06_conc_matrix.png"))

    # 7) Global map
    df_map = _run_df(engine, PAIRED_OBSERVATIONS, params)
    plot_species_micro_map(df_map, os.path.join(save_dir, "07_species_micro_map.png"))
    
    # 8) Trend by año
    df_year = _run_df(engine, YEAR_TREND, params)
    plot_year_trend(df_year, os.path.join(save_dir, "08_year_trend.png"))

    # 9) Ranking by ocean (dona)
    df_ocean = _run_df(engine, OCEAN_RANKING_TOTAL, params)
    plot_ocean_donut(df_ocean, os.path.join(save_dir, "09_ocean_donut.png"))
    
    #10) Activy by organization (lollipop)
    df_org = _run_df(engine, ORGANIZATION_ACTIVITY, params)
    plot_org_lollipop(df_org, os.path.join(save_dir, "10_org_lollipop.png"))
    
    
   # 11) Zonas críticas High-High
    df_highhigh = _run_df(engine, CRITICAL_ZONES_HIGHHIGH, params)
    plot_critical_highhigh(df_highhigh, os.path.join(save_dir, "11_critical_highhigh.png"))

    # 12) Zonas críticas Low-High
    df_lowhigh = _run_df(engine, CRITICAL_ZONES_LOWBIODIV_HIGHCONT, params)
    plot_critical_lowbiodiv_highcont(df_lowhigh, os.path.join(save_dir, "12_critical_lowhigh.png"))
    
     # 13) Muestreos por año
    df_year_samples = _run_df(engine, SAMPLES_PER_YEAR, params)
    plot_samples_per_year(df_year_samples, os.path.join(save_dir, "13_samples_per_year.png"))

    # 14) Métodos por año
    df_methods_year = _run_df(engine, METHODS_BY_YEAR_COUNTS, params)
    plot_methods_by_year_area(df_methods_year, os.path.join(save_dir, "14_methods_by_year_area.png"))
    
    #15 ) Métodos por banda de profundidad (heatmap)
    df_methods_depth = _run_df(engine, METHODS_BY_WATERSAMPLEDEPTH, params) 
    plot_depth_vs_method_heatmap(df_methods_depth, os.path.join(save_dir, "15_methods_by_depth_heatmap.png"))   

    #16) Ranking por marine setting
    df_marine_setting = _run_df(engine, MARINE_SETTING_RANKING, params)
    plot_marine_setting_ranking(df_marine_setting, os.path.join(save_dir, "16_marine_setting_ranking.png"))
    
    # 17) Tendencia mensual (barras)
    df_monthly = _run_df(engine, MONTHLY_TREND, params)
    plot_monthly_trend(df_monthly, os.path.join(save_dir, "17_monthly_trend.png"))
    
    if also_show:
        plt.show()

    return {
        "region_avgs": df_region,
        "depth": df_depth,
        "critical_high": df_hc,
        "hotspots": df_hot,
        "method": df_method,
        "conc_matrix": df_matrix,
        "species_micro_map": df_map,
        "year_trend": df_year,
        "ocean_donut": df_ocean,
        "org_lollipop": df_org,
        "critical_highhigh": df_highhigh,
        "critical_lowhigh": df_lowhigh

    }

if __name__ == "__main__":
    from DB.create_db import get_engine
    engine = get_engine()
    generate_all_figures(engine)