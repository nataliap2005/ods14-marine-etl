import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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

import matplotlib.ticker as mticker

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

def plot_species_micro_map(df: pd.DataFrame, out_path: str):
    df = df.dropna(subset=["latitude", "longitude", "species_count", "measurement"])
    if df.empty:
        return

    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)

    sizes = np.clip(df["measurement"], 0, None) * 10
    sizes = np.clip(sizes, 10, 300)
    colors = df["species_count"]

    plt.figure(figsize=(14,7))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_global()
    ax.coastlines()
    ax.add_feature(cfeature.LAND, facecolor="lightgray")
    ax.add_feature(cfeature.OCEAN, facecolor="lightblue")
    ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)

    scatter = ax.scatter(
        df["longitude"], df["latitude"],
        s=sizes,
        c=colors,
        cmap="viridis",
        alpha=0.7,
        edgecolor="k",
        linewidth=0.3,
        transform=ccrs.PlateCarree()
    )
    plt.colorbar(scatter, label="Species Count", orientation="vertical", shrink=0.5)
    plt.title("Species Count vs Microplastics Measurement by Location")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

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
    }

if __name__ == "__main__":
    from DB.create_db import get_engine
    engine = get_engine()
    generate_all_figures(engine)