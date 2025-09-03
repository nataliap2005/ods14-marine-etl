# debug_map.py
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from sqlalchemy import create_engine, text
import numpy as np

# ---------------------------
# Configura tu conexión a la base de datos
# ---------------------------
# Ejemplo MySQL: 
# engine = create_engine("mysql+pymysql://usuario:password@host:puerto/dbname")
engine = create_engine("mysql+pymysql://root:root@localhost:3306/ods14")  # Cambia esto

# ---------------------------
# Query: Extrae todas las coordenadas y datos
# ---------------------------
PAIRED_OBSERVATIONS_DEBUG = text("""
SELECT
  r.region,
  o.ocean,
  s.location_id,
  s.species_count,
  m.measurement,
  m.water_sample_depth,
  l.latitude,
  l.longitude,
  d.full_date
FROM fact_species s
JOIN fact_microplastics m ON m.location_id = s.location_id
LEFT JOIN dim_region r ON m.region_id = r.region_id
LEFT JOIN dim_ocean o ON m.ocean_id = o.ocean_id
LEFT JOIN dim_location l ON s.location_id = l.location_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE l.latitude IS NOT NULL
  AND l.longitude IS NOT NULL;
""")

# ---------------------------
# Función para depurar y mostrar mapa
# ---------------------------
def debug_species_micro_map():
    # Leer datos
    df = pd.read_sql(PAIRED_OBSERVATIONS_DEBUG, con=engine)
    
    # Mostrar algunas filas y tipos para depuración
    print("Datos crudos extraídos:")
    print(df.head(10))
    print("\nTipos de columnas:")
    print(df.dtypes)
    print("\nNúmero de filas:", len(df))

    # Convertir coordenadas a float
    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)

    # Depurar coordenadas fuera de rango
    df = df[(df["latitude"] >= -90) & (df["latitude"] <= 90)]
    df["longitude"] = df["longitude"].apply(lambda x: x if x <= 180 else x - 360)
    df = df[(df["longitude"] >= -180) & (df["longitude"] <= 180)]

    print("\nDatos depurados:")
    print(df.head(10))
    print("\nNúmero de filas después de limpiar coordenadas:", len(df))

    if df.empty:
        print("No hay datos válidos para mostrar.")
        return

    # Preparar tamaños y colores
    sizes = np.clip(df["measurement"], 0, None) * 10
    sizes = np.clip(sizes, 10, 300)
    colors = df["species_count"]

    # Crear mapa
    plt.figure(figsize=(16,8))
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
    plt.title("Species Count vs Microplastics Measurement (Debug)")
    plt.show()

# ---------------------------
# Ejecutar depuración
# ---------------------------
if __name__ == "__main__":
    debug_species_micro_map()