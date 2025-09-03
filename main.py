from ETL.extract import extract
from ETL.transform import transform
from ETL.load import load
from DB.create_db import create_database, get_engine
from sqlalchemy import text

def print_db_state(engine):
    with engine.begin() as conn:
        active_db = conn.execute(text("SELECT DATABASE();")).scalar_one()
        print(f"\n Conectado a la BD: {active_db}\n")
        for tbl in [
            "dim_region","dim_location","dim_marine_setting","dim_sampling_method",
            "dim_unit","dim_concentration_class","dim_date","dim_organization",
            "fact_microplastics","fact_species"
        ]:
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM {tbl};")).scalar_one()
            print(f"{tbl:28s} -> {cnt:>8d} filas")

def main():
    # 0) Create/verify DB and tables
    create_database()
    engine = get_engine()

    # 1) Paths
    file_path_microplastics = 'data/MarineMicroplastics.csv'
    file_path_species = 'data/MarineSpeciesRichness.csv'

    # 2) Extract
    df_microplastics = extract(file_path_microplastics)
    df_species = extract(file_path_species)

    # 3) Transform
    dfs = transform(df_microplastics, df_species)

    # see first rows of each table
    for name, table in dfs.items():
        print(f"\n{name}:")
        print(table.head())

    # 4) Load
    load(dfs, engine)
    print("ETL cOMPLETED. DATA WERE LOADED INTO MySQL.")

if __name__ == '__main__':
    main()