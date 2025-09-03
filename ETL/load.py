import pandas as pd

def _none_na(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notnull(df), None)

def load(dfs: dict, engine):
    """
    Inserta en MySQL en el orden correcto.
    """
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
                table,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )

        # Hechos
        _none_na(dfs["fact_micro"]).to_sql(
            "fact_microplastics",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        _none_na(dfs["fact_species"]).to_sql(
            "fact_species",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )