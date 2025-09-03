import pandas as pd
import numpy as np

def _parse_dates_multi(series: pd.Series) -> pd.Series:
    """
    Intenta parsear fechas con múltiples formatos comunes.
    Orden:
      1) %m-%d-%Y   (ej. 04-25-1972)
      2) %m/%d/%Y   (ej. 04/25/1972)
      3) %Y-%m-%d   (ej. 1972-04-25)
      4) fallback   (dateutil)
    """
    s = series.astype(str).str.strip().replace({"": np.nan, "nan": np.nan, "NaN": np.nan})
    out = pd.to_datetime(s, format="%m-%d-%Y", errors="coerce")
    m = out.isna()
    if m.any():
        out.loc[m] = pd.to_datetime(s[m], format="%m/%d/%Y", errors="coerce")
        m = out.isna()
    if m.any():
        out.loc[m] = pd.to_datetime(s[m], format="%Y-%m-%d", errors="coerce")
        m = out.isna()
    if m.any():
        out.loc[m] = pd.to_datetime(s[m], errors="coerce")  # fallback flexible
    return out

def transform(df_microplastics, df_species):
    # ===== LIMPIEZA =====
    df_microplastics = df_microplastics.rename(
        columns={
            'Latitude (degree)': 'Latitude',
            'Longitude(degree)': 'Longitude',
            'Water Sample Depth (m)': 'Water Sample Depth'
        }
    )

    df_microplastics_clean = df_microplastics.copy()

    # Ocean / Region: strip + vacíos y variantes -> NaN
    for col in ["Ocean", "Region"]:
        if col in df_microplastics_clean.columns:
            df_microplastics_clean[col] = (
                df_microplastics_clean[col]
                .astype(str)
                .str.strip()
                .replace({"": np.nan, "nan": np.nan, "NaN": np.nan, "None": np.nan})
            )

    # Normalización de regiones conocidas
    region_map = {
        "Rio de la Plata": "Río de la Plata",
        "Rio de La Plata": "Río de la Plata",
        "Eastern China Sea": "East China Sea",
        "The Coastal Waters of Southeast Alaska and British Columbia":
            "Coastal Waters of Southeast Alaska and British Columbia",
        "Barentsz Sea": "Barents Sea"
    }
    if "Region" in df_microplastics_clean.columns:
        df_microplastics_clean["Region"] = df_microplastics_clean["Region"].replace(region_map)

    # Marine Setting
    if "Marine Setting" in df_microplastics_clean.columns:
        df_microplastics_clean["Marine Setting"] = df_microplastics_clean["Marine Setting"].str.strip()

    # Sampling Method
    if "Sampling Method" in df_microplastics_clean.columns:
        df_microplastics_clean["Sampling Method"] = df_microplastics_clean["Sampling Method"].str.strip().str.title()
        sampling_map = {
            "Manta Net": "Manta net",
            "Neuston Net": "Neuston net",
            "Stainless-Steel Sampler": "Stainless steel sampler",
            "Stainless Steel Spatula": "Stainless steel spatula",
            " Stainless Steel Spatula": "Stainless steel spatula"
        }
        df_microplastics_clean["Sampling Method"] = df_microplastics_clean["Sampling Method"].replace(sampling_map)

    # Unit
    unit_map = {
        "pieces kg-1 d.w.": "pieces/kg dry weight",
        "pieces/10 mins": "pieces/10 min",
        "pieces/10min": "pieces/10 min"
    }
    if "Unit" in df_microplastics_clean.columns:
        df_microplastics_clean["Unit"] = df_microplastics_clean["Unit"].replace(unit_map)

    # Concentration class range
    conc_range_map = {">10": ">=10", "0": "0-0.0005", ">200": ">=200", ">40000": ">=40000"}
    if "Concentration class range" in df_microplastics_clean.columns:
        df_microplastics_clean["Concentration class range"] = df_microplastics_clean["Concentration class range"].replace(conc_range_map)

    # Concentration class text
    if "Concentration class text" in df_microplastics_clean.columns:
        df_microplastics_clean["Concentration class text"] = df_microplastics_clean["Concentration class text"].str.title()

    # Organization
    if "ORGANIZATION" in df_microplastics_clean.columns:
        df_microplastics_clean["ORGANIZATION"] = df_microplastics_clean["ORGANIZATION"].str.strip()

    # Date: parseo robusto (multi formato)
    if "Date (MM-DD-YYYY)" in df_microplastics_clean.columns:
        df_microplastics_clean["Date (MM-DD-YYYY)"] = _parse_dates_multi(
            df_microplastics_clean["Date (MM-DD-YYYY)"]
        )

    # ===== MERGE BASE (para cruzar con especies por lat/lon si hace falta) =====
    df = pd.merge(
        df_microplastics_clean,
        df_species,
        on=["Latitude", "Longitude"],
        how="outer",
        suffixes=("_micro", "_species")
    )

    # ===== DROP columnas que no van =====
    cols_to_drop = [
        'Subregion', 'Country', 'State', 'Beach Location',
        'Ocean Bottom Depth (m)', 'Sediment Sample Depth (m)',
        'Mesh size (mm)', 'Transect No', 'Sampling point on beach',
        'Volunteers Number', 'Collecting Time (min)',
        'Standardized Nurdle  Amount', 'Short Reference',
        'Long Reference', 'DOI', 'KEYWORDS',
        'NCEI Accession No', 'NCEI Accession No. Link',
        'Symbology', 'GlobalID', 'x', 'y', 'C-Square Code'
    ]
    df_microplastics_clean = df_microplastics_clean.drop(columns=[c for c in cols_to_drop if c in df_microplastics_clean.columns])
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    # ===== DIMENSIONES =====
    # Location
    dim_location = df[["Latitude", "Longitude"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_location["LocationID"] = dim_location.index + 1

    # Region (desde micro, así matchea lo que realmente existe en micro)
    dim_region = (
        df_microplastics_clean[["Ocean", "Region"]]
        .dropna(subset=["Region"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_region["RegionID"] = dim_region.index + 1

    # Marine
    dim_marine = df[["Marine Setting"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_marine["MarineID"] = dim_marine.index + 1

    # Sampling
    dim_sampling = df[["Sampling Method"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_sampling["SamplingMethodID"] = dim_sampling.index + 1

    # Unit
    dim_unit = df[["Unit"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_unit["UnitID"] = dim_unit.index + 1

    # Concentration
    dim_conc = df[["Concentration class range", "Concentration class text"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_conc["ConcentrationClassID"] = dim_conc.index + 1

    # Date (solo de micro; evitas fechas vacías)
    dim_date = pd.DataFrame({"fullDate": df_microplastics_clean["Date (MM-DD-YYYY)"].dropna().unique()})
    dim_date["year"] = dim_date["fullDate"].dt.year
    dim_date["month"] = dim_date["fullDate"].dt.month
    dim_date["day"] = dim_date["fullDate"].dt.day
    dim_date["DateID"] = dim_date["fullDate"].dt.strftime("%Y%m%d").astype(int)
    dim_date = dim_date.sort_values("fullDate").reset_index(drop=True)

    # Organization
    dim_org = df[["ORGANIZATION"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_org["OrganizationID"] = dim_org.index + 1

    # ===== HECHOS =====
    # fact_micro con merge por Ocean+Region y fallback por Region si faltan
    fact_micro = (
        df_microplastics_clean
        .merge(dim_location, on=["Latitude", "Longitude"], how="left")
        .merge(dim_region, on=["Ocean", "Region"], how="left")  # primer intento (Region+Ocean)
        .merge(dim_marine, on="Marine Setting", how="left")
        .merge(dim_sampling, on="Sampling Method", how="left")
        .merge(dim_unit, on="Unit", how="left")
        .merge(dim_conc, on=["Concentration class range", "Concentration class text"], how="left")
        .merge(dim_date[["DateID", "fullDate"]], left_on="Date (MM-DD-YYYY)", right_on="fullDate", how="left")
        .merge(dim_org, on="ORGANIZATION", how="left")
    )

    # Fallback: si RegionID sigue NaN (por Ocean faltante), intentar por Region solamente
    if fact_micro["RegionID"].isna().any():
        fact_micro = fact_micro.drop(columns=["RegionID"]).merge(
            dim_region[["Region", "RegionID"]].drop_duplicates(),
            on="Region", how="left"
        )

    fact_micro = fact_micro[[
        "LocationID",
        "RegionID",
        "MarineID",
        "SamplingMethodID",
        "UnitID",
        "ConcentrationClassID",
        "DateID",
        "OrganizationID",
        "Microplastics measurement",
        "Water Sample Depth"
    ]]

    # fact_species
    fact_species = df.merge(dim_location, on=["Latitude", "Longitude"], how="left")[["LocationID", "Species Count"]]

    return {
        "dim_location": dim_location,
        "dim_region": dim_region,
        "dim_marine": dim_marine,
        "dim_sampling": dim_sampling,
        "dim_unit": dim_unit,
        "dim_conc": dim_conc,
        "dim_date": dim_date,
        "dim_org": dim_org,
        "fact_micro": fact_micro,
        "fact_species": fact_species
    }
