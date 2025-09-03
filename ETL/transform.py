import pandas as pd
import numpy as np

def transform(df_microplastics, df_species):
    # LIMPIEZA
    df_microplastics = df_microplastics.rename(
        columns={
            'Latitude (degree)': 'Latitude',
            'Longitude(degree)': 'Longitude',
            'Water Sample Depth (m)': 'Water Sample Depth' 
        }
    )

    df_microplastics_clean = df_microplastics.copy()

    # Ocean
    df_microplastics_clean["Ocean"] = df_microplastics_clean["Ocean"].replace({"nan": np.nan})

    # Region
    region_map = {
        "Rio de la Plata": "Río de la Plata",
        "Rio de La Plata": "Río de la Plata",
        "Eastern China Sea": "East China Sea",
        "The Coastal Waters of Southeast Alaska and British Columbia":
            "Coastal Waters of Southeast Alaska and British Columbia",
        "Barentsz Sea": "Barents Sea"
    }
    df_microplastics_clean["Region"] = df_microplastics_clean["Region"].replace(region_map)

    # Marine Setting
    df_microplastics_clean["Marine Setting"] = df_microplastics_clean["Marine Setting"].str.strip()

    # Sampling Method
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
    df_microplastics_clean["Unit"] = df_microplastics_clean["Unit"].replace(unit_map)

    # Concentration class range
    conc_range_map = {
        ">10": ">=10",
        "0": "0-0.0005",
        ">200": ">=200",
        ">40000": ">=40000"
    }
    df_microplastics_clean["Concentration class range"] = df_microplastics_clean["Concentration class range"].replace(conc_range_map)

    # Concentration class text
    df_microplastics_clean["Concentration class text"] = df_microplastics_clean["Concentration class text"].str.title()

    # Organization
    df_microplastics_clean["ORGANIZATION"] = df_microplastics_clean["ORGANIZATION"].str.strip()

    # Date
    df_microplastics_clean["Date (MM-DD-YYYY)"] = pd.to_datetime(
    df_microplastics_clean["Date (MM-DD-YYYY)"],
    format="%m-%d-%Y",
    errors="coerce"
)

    df = pd.merge(
        df_microplastics_clean,
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
        'Symbology', 'GlobalID', 'x', 'y', 'C-Square Code'
    ]
    df_microplastics_clean = df_microplastics_clean.drop(columns=[c for c in cols_to_drop if c in df_microplastics_clean.columns])
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    # DIMENSIONS
    dim_location = df[["Latitude", "Longitude"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_location["LocationID"] = dim_location.index + 1

    dim_region = df[["Ocean", "Region"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_region["RegionID"] = dim_region.index + 1

    dim_marine = df[["Marine Setting"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_marine["MarineID"] = dim_marine.index + 1

    dim_sampling = df[["Sampling Method"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_sampling["SamplingMethodID"] = dim_sampling.index + 1

    dim_unit = df[["Unit"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_unit["UnitID"] = dim_unit.index + 1

    dim_conc = df[["Concentration class range", "Concentration class text"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_conc["ConcentrationClassID"] = dim_conc.index + 1

    # Dim Date
    dim_date = pd.DataFrame({"fullDate": df["Date (MM-DD-YYYY)"].dropna().unique()})
    dim_date["year"] = dim_date["fullDate"].dt.year
    dim_date["month"] = dim_date["fullDate"].dt.month
    dim_date["day"] = dim_date["fullDate"].dt.day
    dim_date["DateID"] = dim_date["fullDate"].dt.strftime("%Y%m%d").astype(int)
    dim_date = dim_date.sort_values("fullDate").reset_index(drop=True)

    dim_org = df[["ORGANIZATION"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_org["OrganizationID"] = dim_org.index + 1

    # FACTS
    fact_micro = df_microplastics_clean \
        .merge(dim_location, on=["Latitude", "Longitude"], how="left") \
        .merge(dim_region, on=["Ocean", "Region"], how="left") \
        .merge(dim_marine, on="Marine Setting", how="left") \
        .merge(dim_sampling, on="Sampling Method", how="left") \
        .merge(dim_unit, on="Unit", how="left") \
        .merge(dim_conc, on=["Concentration class range", "Concentration class text"], how="left") \
        .merge(dim_date, left_on="Date (MM-DD-YYYY)", right_on="fullDate", how="left") \
        .merge(dim_org, on="ORGANIZATION", how="left") \
        [[
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

    fact_species = df \
        .merge(dim_location, on=["Latitude", "Longitude"]) \
        [[
            "LocationID", "Species Count"
        ]]

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