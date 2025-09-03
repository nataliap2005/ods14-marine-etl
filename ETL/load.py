# ETL/load.py
import pandas as pd

def _none_na(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notnull(df), None)

def _rename_for_mysql(dfs: dict) -> dict:
    """Renombra columnas para que coincidan con el DDL MySQL."""
    out = dfs.copy()

    # Dimensiones
    out["dim_location"] = out["dim_location"].rename(
        columns={"LocationID": "location_id", "Latitude": "latitude", "Longitude": "longitude"}
    )
    out["dim_region"] = out["dim_region"].rename(
        columns={"RegionID": "region_id", "Region": "region", "Ocean": "ocean"}
    )
    out["dim_marine"] = out["dim_marine"].rename(
        columns={"MarineID": "marine_setting_id", "Marine Setting": "marine_setting"}
    )
    out["dim_sampling"] = out["dim_sampling"].rename(
        columns={"SamplingMethodID": "method_id", "Sampling Method": "sampling_method"}
    )
    out["dim_unit"] = out["dim_unit"].rename(
        columns={"UnitID": "unit_id", "Unit": "unit"}
    )
    out["dim_conc"] = out["dim_conc"].rename(
        columns={
            "ConcentrationClassID": "concentration_id",
            "Concentration class range": "concentration_class_range",
            "Concentration class text": "concentration_class_text",
        }
    )
    out["dim_date"] = out["dim_date"].rename(
        columns={"DateID": "date_id", "fullDate": "full_date"}
    )
    out["dim_org"] = out["dim_org"].rename(
        columns={"OrganizationID": "organization_id", "ORGANIZATION": "organization"}
    )

    # Hechos
    out["fact_micro"] = out["fact_micro"].rename(
        columns={
            "LocationID": "location_id",
            "RegionID": "region_id",
            "MarineID": "marine_setting_id",
            "SamplingMethodID": "method_id",
            "UnitID": "unit_id",
            "ConcentrationClassID": "concentration_id",
            "DateID": "date_id",
            "OrganizationID": "organization_id",
            "Microplastics measurement": "measurement",
            "Water Sample Depth": "water_sample_depth",
        }
    )
    out["fact_species"] = out["fact_species"].rename(
        columns={"LocationID": "location_id", "Species Count": "species_count"}
    )
    return out

def load(dfs: dict, engine):
    """
    Inserta en MySQL en el orden correcto. Usa append para respetar PKs/FKs del DDL.
    """
    dfs = _rename_for_mysql(dfs)

    # Normaliza NULLs y tipos de fecha
    if "dim_date" in dfs and "full_date" in dfs["dim_date"].columns:
        dfs["dim_date"]["full_date"] = pd.to_datetime(dfs["dim_date"]["full_date"]).dt.date

    with engine.begin() as conn:
        # Dimensiones
        order_dims = [
            ("dim_location", "dim_location"),
            ("dim_region", "dim_region"),
            ("dim_marine", "dim_marine_setting"),
            ("dim_sampling", "dim_sampling_method"),
            ("dim_unit", "dim_unit"),
            ("dim_conc", "dim_concentration_class"),
            ("dim_date", "dim_date"),
            ("dim_org", "dim_organization"),
        ]
        for key, table in order_dims:
            _none_na(dfs[key]).to_sql(
                table, con=conn, if_exists="append", index=False, method="multi", chunksize=1000
            )

        # Hechos
        _none_na(dfs["fact_micro"]).to_sql(
            "fact_microplastics", con=conn, if_exists="append", index=False, method="multi", chunksize=1000
        )
        _none_na(dfs["fact_species"]).to_sql(
            "fact_species", con=conn, if_exists="append", index=False, method="multi", chunksize=1000
        )
