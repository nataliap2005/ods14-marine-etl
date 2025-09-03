# reports/visualizations.py
import os
from datetime import date
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from DB.create_db import get_engine
from DB.queries import (
    SPECIES_MICRO_BY_REGION,
    CORR_SPECIES_MICRO_OVERALL,
    SPECIES_BY_CONC_CLASS,
    DEPTH_BINS_EFFECT,
    CRITICAL_ZONES_HIGH_HIGH,
    CRITICAL_ZONES_LOW_HIGH,
    REGION_HOTSPOTS,
    PAIRED_OBSERVATIONS,
    METHOD_MESH_EFFECTS,
    CONC_CLASS_BY_REGION,
)

# ---------------------------
# Helpers
# ---------------------------
def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _run_df(engine, query, params=None) -> pd.DataFrame:
    params = params or {}
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)

def _save_table(df: pd.DataFrame, out_csv: str):
    if df is None or df.empty:
        return
    df.to_csv(out_csv, index=False)

# ---------------------------
# Plotters (matplotlib)
# ---------------------------
def plot_region_avgs(df: pd.DataFrame, out_path: str):
    """Barras agrupadas: Avg Species vs Avg Microplastics por región."""
    if df.empty:
        return
    data = df.copy()
    regions = data["region"].fillna("Unknown").astype(str).values
    species = data["region_avg_species"].values
    micro   = data["region_avg_microplastics"].values

    x = np.arange(len(regions))
    width = 0.42

    plt.figure(figsize=(12, 6))
    plt.bar(x - width/2, species, width, label="Avg Species Count")
    plt.bar(x + width/2, micro,   width, label="Avg Microplastics")
    plt.xticks(x, regions, rotation=45, ha="right")
    plt.ylabel("Average")
    plt.title("Species vs Microplastics by Region")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_correlation_indicator(pearson_r: float, out_path: str):
    """Indicador simple de correlación: renderiza un texto grande con r."""
    plt.figure(figsize=(4.5, 3.5))
    plt.axis("off")
    text = "Pearson r\n"
    text += f"{pearson_r:.3f}" if pearson_r is not None else "N/A"
    plt.text(0.5, 0.5, text, ha="center", va="center", fontsize=22)
    plt.title("Overall Correlation (Species vs Microplastics)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_species_by_conc_class(df: pd.DataFrame, out_path: str):
    """Barras: promedio de especies por clase de concentración."""
    if df.empty:
        return
    d = df.copy()
    d["class_text"] = d["class_text"].fillna("Unknown")
    d = d.sort_values("avg_species_count", ascending=False)
    plt.figure(figsize=(12, 6))
    plt.bar(d["class_text"], d["avg_species_count"])
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Average Species Count")
    plt.title("Species vs Concentration Class")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_depth_bands(df: pd.DataFrame, out_path: str):
    """Barras agrupadas por banda de profundidad: Avg Species y Avg Microplastics."""
    if df.empty:
        return
    order = ["Unknown", "0-5m", "5-20m", "20-50m", "50-200m", "200m+"]
    d = df.copy()
    d["depth_band"] = pd.Categorical(d["depth_band"], categories=order, ordered=True)
    d = d.sort_values("depth_band")

    x = np.arange(len(d))
    width = 0.42

    plt.figure(figsize=(10, 5.5))
    plt.bar(x - width/2, d["avg_species_count"], width, label="Avg Species Count")
    plt.bar(x + width/2, d["avg_microplastics"], width, label="Avg Microplastics")
    plt.xticks(x, d["depth_band"].astype(str), rotation=0)
    plt.ylabel("Average")
    plt.title("Depth Effects (Water Sample Depth bands)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_hotspots(df: pd.DataFrame, out_path: str):
    """Scatter de z-scores por región con limpieza de NaN/None."""
    if df.empty:
        return

    d = df.copy()

    # Asegurar tipos numéricos y limpiar nulos
    for col in ["z_species", "z_micro", "avg_micro"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")

    d = d.dropna(subset=["z_species", "z_micro"])  # <- clave
    if d.empty:
        return

    # Tamaño de punto seguro
    sizes = d["avg_micro"].fillna(0).clip(lower=0)
    sizes = np.clip(sizes * 10, 20, 400)

    plt.figure(figsize=(7.5, 6.5))
    plt.scatter(d["z_species"], d["z_micro"], s=sizes)

    # Anotar solo puntos válidos
    for _, row in d.iterrows():
        reg = str(row.get("region", ""))
        x = row["z_species"]
        y = row["z_micro"]
        if pd.notnull(x) and pd.notnull(y) and reg:
            plt.annotate(reg, (float(x), float(y)), fontsize=8, alpha=0.7)

    plt.axhline(0, linestyle="--", linewidth=1)
    plt.axvline(0, linestyle="--", linewidth=1)
    plt.xlabel("Biodiversity (z-score)")
    plt.ylabel("Contamination (z-score)")
    plt.title("Region Hotspots (z-scores)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_paired_scatter(df: pd.DataFrame, out_path: str):
    """Scatter species_count vs measurement (color por región si existe)."""
    if df.empty:
        return
    d = df.copy()
    # Elegir columnas
    x = "measurement"; y = "species_count"
    plt.figure(figsize=(8, 6))
    # Si hay regiones, hacer grupos
    if "region" in d.columns and d["region"].notna().any():
        for reg, g in d.groupby(d["region"].fillna("Unknown")):
            plt.scatter(g[x], g[y], label=str(reg), alpha=0.7, s=18)
        plt.legend(fontsize=8, ncol=2, frameon=False)
    else:
        plt.scatter(d[x], d[y], alpha=0.7, s=18)
    plt.xlabel("Microplastics (measurement)")
    plt.ylabel("Species Count")
    plt.title("Paired Observations")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_method_mesh(df: pd.DataFrame, out_path: str):
    """Barras: métodos de muestreo y promedios (species y microplastics)."""
    if df.empty:
        return
    d = df.copy()
    d["sampling_method"] = d["sampling_method"].fillna("Unknown")

    # Convertir a long para dos métricas
    d_long = d.melt(
        id_vars=["sampling_method", "method_id", "n_samples"],
        value_vars=["avg_microplastics", "avg_species_count"],
        var_name="metric",
        value_name="value",
    )

    # Orden por valor
    d_long = d_long.sort_values(["sampling_method", "metric"])
    methods = d_long["sampling_method"].unique()
    x = np.arange(len(methods))
    width = 0.42

    # Valores por método
    avg_micro = d_long[d_long["metric"]=="avg_microplastics"].set_index("sampling_method")["value"].reindex(methods)
    avg_spec  = d_long[d_long["metric"]=="avg_species_count"].set_index("sampling_method")["value"].reindex(methods)

    plt.figure(figsize=(12, 6))
    plt.bar(x - width/2, avg_micro.values, width, label="Avg Microplastics")
    plt.bar(x + width/2, avg_spec.values,  width, label="Avg Species Count")
    plt.xticks(x, methods, rotation=45, ha="right")
    plt.ylabel("Average")
    plt.title("Method Effects (Measurement & Species)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def plot_conc_matrix(df: pd.DataFrame, out_path: str):
    """Heatmap: avg_measurement por región × clase de concentración."""
    if df.empty:
        return
    d = df.copy()
    d["region"] = d["region"].fillna("Unknown")
    d["class_text"] = d["class_text"].fillna("Unknown")
    mat = d.pivot_table(index="region", columns="class_text", values="avg_measurement", aggfunc="mean")
    plt.figure(figsize=(12, 6))
    plt.imshow(mat.values, aspect="auto")
    plt.xticks(range(len(mat.columns)), mat.columns, rotation=45, ha="right")
    plt.yticks(range(len(mat.index)),   mat.index)
    plt.colorbar(label="Avg Measurement")
    plt.title("Concentration Class by Region")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

# ---------------------------
# Public API
# ---------------------------
def generate_all_figures(
    engine=None,
    start_date=None,
    end_date=None,
    save_dir: str = "reports/figures",
    also_show: bool = False  # si quieres mostrar en pantalla (no recomendado para ejecución automática)
):
    """
    Ejecuta las consultas y guarda gráficos/CSVs en save_dir.
    """
    engine = engine or get_engine()
    _ensure_dir(save_dir)
    params = {"start_date": start_date, "end_date": end_date}

    # 1) Region averages
    df_region = _run_df(engine, SPECIES_MICRO_BY_REGION, params)
    plot_region_avgs(df_region, os.path.join(save_dir, "01_region_avgs.png"))

    # 2) Correlation indicator
    df_corr = _run_df(engine, CORR_SPECIES_MICRO_OVERALL, params)
    pearson_r = float(df_corr.iloc[0]["pearson_r"]) if not df_corr.empty and df_corr.iloc[0]["pearson_r"] is not None else None
    plot_correlation_indicator(pearson_r, os.path.join(save_dir, "02_correlation_indicator.png"))

    # 3) Species by concentration class
    df_class = _run_df(engine, SPECIES_BY_CONC_CLASS, params)
    plot_species_by_conc_class(df_class, os.path.join(save_dir, "03_species_by_conc_class.png"))

    # 4) Depth bands
    df_depth = _run_df(engine, DEPTH_BINS_EFFECT, params)
    plot_depth_bands(df_depth, os.path.join(save_dir, "04_depth_bands.png"))

    # 5) Critical zones — high-high
    df_hh = _run_df(engine, CRITICAL_ZONES_HIGH_HIGH, params)
    _save_table(df_hh, os.path.join(save_dir, "05_critical_zones_high_high.csv"))

    # 6) Critical zones — low-high
    df_lh = _run_df(engine, CRITICAL_ZONES_LOW_HIGH, params)
    _save_table(df_lh, os.path.join(save_dir, "06_critical_zones_low_high.csv"))

    # 7) Region hotspots
    df_hot = _run_df(engine, REGION_HOTSPOTS, params)
    plot_hotspots(df_hot, os.path.join(save_dir, "07_region_hotspots.png"))

    # 8) Paired observations
    df_pair = _run_df(engine, PAIRED_OBSERVATIONS, params)
    plot_paired_scatter(df_pair, os.path.join(save_dir, "08_paired_scatter.png"))

    # 9) Method & mesh effects
    df_method = _run_df(engine, METHOD_MESH_EFFECTS, params)
    plot_method_mesh(df_method, os.path.join(save_dir, "09_method_mesh.png"))

    # 10) Concentration class by region (matrix)
    df_matrix = _run_df(engine, CONC_CLASS_BY_REGION, params)
    plot_conc_matrix(df_matrix, os.path.join(save_dir, "10_conc_matrix.png"))

    if also_show:
        plt.show()  # mostrará el último gráfico si se llama interactivo

    return {
        "region_avgs": df_region,
        "correlation": pearson_r,
        "class_species": df_class,
        "depth": df_depth,
        "critical_high_high": df_hh,
        "critical_low_high": df_lh,
        "hotspots": df_hot,
        "paired": df_pair,
        "method": df_method,
        "conc_matrix": df_matrix,
    }
