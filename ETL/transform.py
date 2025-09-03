import pandas as pd
import numpy as np

def _parse_dates_multi(series: pd.Series) -> pd.Series:
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
    # ===== RENOMBRADO INICIAL =====
    df_microplastics = df_microplastics.rename(
        columns={
            'Latitude (degree)': 'latitude',
            'Longitude(degree)': 'longitude',
            'Water Sample Depth (m)': 'water_sample_depth',
            'Ocean': 'ocean',
            'Region': 'region',
            'Marine Setting': 'marine_setting',
            'Sampling Method': 'sampling_method',
            'Unit': 'unit',
            'Concentration class range': 'concentration_class_range',
            'Concentration class text': 'concentration_class_text',
            'ORGANIZATION': 'organization',
            'Date (MM-DD-YYYY)': 'full_date',
            'Microplastics measurement': 'measurement'
        }
    )

    df_species = df_species.rename(
        columns={
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Species Count': 'species_count'
        }
    )

    df_microplastics_clean = df_microplastics.copy()

    # ===== CLEANING =====
    # Ocean / Region: limpieza inicial
    for col in ["ocean", "region"]:
        if col in df_microplastics_clean.columns:
            df_microplastics_clean[col] = (
                df_microplastics_clean[col]
                .astype(str)
                .str.strip()
                .replace({"": np.nan, "nan": np.nan, "NaN": np.nan, "None": np.nan})
            )

    # Normalization
    region_map = {
        "Rio de la Plata": "Río de la Plata",
        "Rio de La Plata": "Río de la Plata",
        "Eastern China Sea": "East China Sea",
        "The Coastal Waters of Southeast Alaska and British Columbia":
            "Coastal Waters of Southeast Alaska and British Columbia",
        "Barentsz Sea": "Barents Sea"
    }
    df_microplastics_clean["region"] = df_microplastics_clean["region"].replace(region_map)

    # Marine Setting
    df_microplastics_clean["marine_setting"] = df_microplastics_clean["marine_setting"].str.strip()

    # Sampling Method
    df_microplastics_clean["sampling_method"] = (
        df_microplastics_clean["sampling_method"].str.strip().str.title()
    )
    sampling_map = {
        "Manta Net": "Manta net",
        "Neuston Net": "Neuston net",
        "Stainless-Steel Sampler": "Stainless steel sampler",
        "Stainless Steel Spatula": "Stainless steel spatula",
        " Stainless Steel Spatula": "Stainless steel spatula"
    }
    df_microplastics_clean["sampling_method"] = df_microplastics_clean["sampling_method"].replace(sampling_map)

    # Unit
    unit_map = {
        "pieces kg-1 d.w.": "pieces/kg dry weight",
        "pieces/10 mins": "pieces/10 min",
        "pieces/10min": "pieces/10 min"
    }
    df_microplastics_clean["unit"] = df_microplastics_clean["unit"].replace(unit_map)

    # Concentration class range
    conc_range_map = {">10": ">=10", "0": "0-0.0005", ">200": ">=200", ">40000": ">=40000"}
    df_microplastics_clean["concentration_class_range"] = df_microplastics_clean["concentration_class_range"].replace(conc_range_map)

    # Concentration class text
    df_microplastics_clean["concentration_class_text"] = df_microplastics_clean["concentration_class_text"].str.title()

    # Organization
    df_microplastics_clean["organization"] = df_microplastics_clean["organization"].str.strip()

    # Date
    df_microplastics_clean["full_date"] = _parse_dates_multi(df_microplastics_clean["full_date"])

    # ===== MERGE BASE =====
    df = pd.merge(
        df_microplastics_clean,
        df_species,
        on=["latitude", "longitude"],
        how="outer",
        suffixes=("_micro", "_species")
    )

    # ===== DROP =====
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
    df_microplastics_clean.drop(columns=[c for c in cols_to_drop if c in df_microplastics_clean.columns], inplace=True)
    df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

    # ===== DIMENSIONS =====
    # Locations
    dim_location = df[["latitude", "longitude"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_location["location_id"] = dim_location.index + 1

    # Ocean
    dim_ocean = df_microplastics_clean[["ocean"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_ocean["ocean_id"] = dim_ocean.index + 1

    # Region
    dim_region = df_microplastics_clean[["region"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_region["region_id"] = dim_region.index + 1

    # Marine
    dim_marine = df[["marine_setting"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_marine["marine_setting_id"] = dim_marine.index + 1

    # Sampling
    dim_sampling = df[["sampling_method"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_sampling["method_id"] = dim_sampling.index + 1

    # Unit
    dim_unit = df[["unit"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_unit["unit_id"] = dim_unit.index + 1

    # Concentration
    dim_conc = df[["concentration_class_range", "concentration_class_text"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_conc["concentration_id"] = dim_conc.index + 1

    # Date
    dim_date = pd.DataFrame({"full_date": df_microplastics_clean["full_date"].dropna().unique()})
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["date_id"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date = dim_date.sort_values("full_date").reset_index(drop=True)

    # Organization
    dim_org = df[["organization"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_org["organization_id"] = dim_org.index + 1

    # ===== FACTS =====
    fact_micro = (
        df_microplastics_clean
        .merge(dim_location, on=["latitude", "longitude"], how="left")
        .merge(dim_ocean, on="ocean", how="left")
        .merge(dim_region, on="region", how="left")
        .merge(dim_marine, on="marine_setting", how="left")
        .merge(dim_sampling, on="sampling_method", how="left")
        .merge(dim_unit, on="unit", how="left")
        .merge(dim_conc, on=["concentration_class_range", "concentration_class_text"], how="left")
        .merge(dim_date[["date_id", "full_date"]], left_on="full_date", right_on="full_date", how="left")
        .merge(dim_org, on="organization", how="left")
    )
    fact_micro["ocean_id"] = fact_micro["ocean_id"].astype("Int64")

    fact_micro = fact_micro[[
        "location_id",
        "ocean_id",
        "region_id",
        "marine_setting_id",
        "method_id",
        "unit_id",
        "concentration_id",
        "date_id",
        "organization_id",
        "measurement",
        "water_sample_depth"
    ]]

    fact_species = df.merge(dim_location, on=["latitude", "longitude"], how="left")[["location_id", "species_count"]]

    return {
        "dim_location": dim_location,
        "dim_ocean": dim_ocean,  
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