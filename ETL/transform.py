import pandas as pd
import numpy as np

def transform(df_microplastics, df_species):
    df_microplastics = df_microplastics.rename(
        columns={'Latitude (degree)': 'Latitude', 'Longitude(degree)': 'Longitude'}
    )

    df = pd.merge(
        df_microplastics,
        df_species,
        on=["Latitude", "Longitude"],
        how="outer",
        suffixes=("_micro", "_species")
    )

    cols_to_drop = [
        'Subregion', 'Country', 'State', 'Beach Location',
        'Ocean Bottom Depth (m)', 'Sediment Sample Depth (m)',
        'Mesh size (mm)', 'Transect No', 'Sampling point on beach',
        'Volunteers Number', 'Collecting Time (min)',
        'Standardized Nurdle  Amount', 'Short Reference',
        'Long Reference', 'DOI', 'KEYWORDS',
        'NCEI Accession No', 'NCEI Accession No. Link',
        'Symbology', 'GlobalID', 'x', 'y'
    ]

    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    df.head()
    return df

def latlon_to_csquare(lat, lon, res=0.5):
    """
    Convierte Lat/Lon a un código C-Square (grid simple).
    res: tamaño de la celda en grados (ej. 0.5°, 0.25°, 1°)
    """
    if pd.isnull(lat) or pd.isnull(lon):
        return np.nan
    
    lat_band = np.floor(lat / res) * res
    lon_band = np.floor(lon / res) * res
    
    return f"{lat_band:.2f}_{lon_band:.2f}_{res}"
