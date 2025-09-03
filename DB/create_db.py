# DB/create_db.py
from sqlalchemy import create_engine, text

USER = "root"
PASSWORD = "root"
HOST = "localhost"
PORT = 3306

BASE_URL = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/?charset=utf8mb4"
DB_URL   = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/ods14?charset=utf8mb4"


TABLES_DDL = """
CREATE TABLE IF NOT EXISTS dim_region (
  region_id INT PRIMARY KEY,
  region VARCHAR(255),
  ocean  VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_location (
  location_id INT PRIMARY KEY,
  latitude  DOUBLE,
  longitude DOUBLE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_marine_setting (
  marine_setting_id INT PRIMARY KEY,
  marine_setting VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_sampling_method (
  method_id INT PRIMARY KEY,
  sampling_method VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_unit (
  unit_id INT PRIMARY KEY,
  unit VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_concentration_class (
  concentration_id INT PRIMARY KEY,
  concentration_class_range VARCHAR(64),
  concentration_class_text  VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_date (
  date_id INT PRIMARY KEY,
  full_date DATE,
  year SMALLINT,
  month TINYINT,
  day TINYINT
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_organization (
  organization_id INT PRIMARY KEY,
  organization VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS fact_microplastics (
  unique_id INT AUTO_INCREMENT PRIMARY KEY,
  location_id INT,
  region_id INT,
  marine_setting_id INT,
  method_id INT,
  unit_id INT,
  concentration_id INT,
  date_id INT,
  organization_id INT,
  measurement DOUBLE,
  water_sample_depth DOUBLE,
  CONSTRAINT fk_micro_loc  FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
  CONSTRAINT fk_micro_reg  FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
  CONSTRAINT fk_micro_mar  FOREIGN KEY (marine_setting_id) REFERENCES dim_marine_setting(marine_setting_id),
  CONSTRAINT fk_micro_met  FOREIGN KEY (method_id) REFERENCES dim_sampling_method(method_id),
  CONSTRAINT fk_micro_unit FOREIGN KEY (unit_id) REFERENCES dim_unit(unit_id),
  CONSTRAINT fk_micro_conc FOREIGN KEY (concentration_id) REFERENCES dim_concentration_class(concentration_id),
  CONSTRAINT fk_micro_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
  CONSTRAINT fk_micro_org  FOREIGN KEY (organization_id) REFERENCES dim_organization(organization_id),
  KEY idx_micro_loc (location_id)               -- índice aquí (sin IF NOT EXISTS)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS fact_species (
  species_id INT AUTO_INCREMENT PRIMARY KEY,
  location_id INT,
  species_count INT,
  CONSTRAINT fk_species_loc FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
  KEY idx_spec_loc (location_id)                -- índice aquí (sin IF NOT EXISTS)
) ENGINE=InnoDB;
"""

def create_database():
    # 1) Drop y create de la BD en conexión de servidor
    server = create_engine(BASE_URL, future=True)
    with server.begin() as conn:
        conn.execute(text("DROP DATABASE IF EXISTS ods14;"))
        conn.execute(text("CREATE DATABASE ods14 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"))
        print("Base de datos 'ods14' recreada.")

    # 2) Crear tablas ya conectados a la BD nueva
    engine = create_engine(DB_URL, future=True)
    with engine.begin() as conn:
        for stmt in [s.strip() for s in TABLES_DDL.strip().split(";") if s.strip()]:
            conn.execute(text(stmt))
        print("Tablas creadas/verificadas en 'ods14'.")

def get_engine():
    return create_engine(DB_URL, future=True)

if __name__ == "__main__":
    create_database()
