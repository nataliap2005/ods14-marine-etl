# ODS 14 - Marine ETL Project

**Made by:** Natalia Paredes Cambindo, Fabián Gomezcasseres and Estefanía Hernández Rojas
## ETL Pipeline
<p align="center">
  <img src="https://github.com/nataliap2005/ods14-marine-etl/raw/698ff3a24e358cb3040a9a9c0dbccd79c700dc32/diagrams/Process%20Diagram.png" alt="Process Diagram" width="700">
</p>

The goal of this project is to develop an **ETL pipeline** to integrate **marine biodiversity data** with **microplastic pollution data**.  

Data is extracted from CSV files, transformed into a **dimensional data model**, and loaded into a **MySQL Data Warehouse**, enabling the creation of reports and visualizations that support the preservation of marine life (**SDG 14: Life Below Water**).

1. **Extract**  
   - Sources:
     - Marine biodiversity dataset (species richness).
     - Marine microplastics dataset.
   - Format: CSV files.
2. **Transform**  
   - Data cleaning and normalization using **Python** and **Pandas**.  
   - Removal of irrelevant columns or those with more than **10,000 null values**.  
   - Standardization of categorical variables (`Region`, `Sampling Method`, `Unit`).  
   - Uniform conversion of date formats for the `dim_date` table.  
   - Creation of **surrogate keys** for each dimension.  
   - Separation of data into **dimension tables** and **fact tables** (`fact_microplastics` and `fact_species`).
3. **Load**  
   - Data loaded into the **MySQL Data Warehouse**.  
   - Dimensions are loaded first, followed by fact tables with their respective foreign keys.

## Star Schema
The dimensional model was designed to support analytical queries efficiently using a Star Schema.
This model integrates biodiversity and pollution data, enabling the analysis of relationships between marine species richness and microplastic concentrations from multiple perspectives such as location, time, and sampling methods.

**Key Design Decisions:**

Two separate fact tables were created instead of a single unified table:
  - fact_microplastics: stores quantitative measurements of microplastic concentrations, along with depth and related sampling information.
  - fact_species: contains species count data representing marine biodiversity levels per location.
This separation was necessary because each dataset represents different types of events:
  - Microplastics data is continuous and measured in units, often collected at specific depths and times.
  - Species richness data is discrete and predictive, representing counts of species derived from environmental modeling.
    
Combining them into one table would have resulted in a sparse, inconsistent structure with many null values. By keeping them separate, each fact table remains clean, well-defined, and optimized for analysis, while still allowing cross-analysis through shared dimensions such as location and date.

- Dimension tables provide contextual attributes such as geographic information, temporal references, and methodological details.
- The dim_date table enables time-based analysis by breaking down dates into year, month, and day.
- Foreign key relationships ensure referential integrity and facilitate complex queries across both datasets.

<p align="center">
  <img src="https://github.com/nataliap2005/ods14-marine-etl/blob/698ff3a24e358cb3040a9a9c0dbccd79c700dc32/diagrams/Star%20Schema%20DDM.png" alt="Star Schema DDM" width="700">
</p>


**Fact Tables:**
  - `fact_microplastics`: Contains measurements of microplastics, water sampling depth, and links to related dimensions.
  - `fact_species`: Contains the count of species observed per location.
**Dimension Tables:**
  - `dim_location` (latitude, longitude)
  - `dim_region` (region)
  - `dim_ocean` (ocean)
  - `dim_marine_setting` (marine environment type)
  - `dim_sampling_method` (sampling method)
  - `dim_unit` (measurement unit)
  - `dim_concentration_class` (concentration categories)
  - `dim_date` (date, year, month, day)
  - `dim_organization` (responsible organization)
## Project Structure
```
.
├── data
│   ├── MarineMicroplastics.csv
│   └── MarineSpeciesRichness.csv
├── DB
│   ├── create_db.py
│   └── queries.py
├── diagrams
├── EDA.ipynb
├── ETL
│   ├── extract.py
│   ├── load.py
│   └── transform.py
├── main.py
├── README.md
├── reports
│   └── visualizations.py
└── requirements.txt
```

- **`data/`**: Raw CSV datasets.  
- **`DB/`**: Database scripts.
  - `create_db.py`: Creates the database and tables in MySQL.  
  - `queries.py`: SQL queries for reporting and analysis.  
- **`ETL/`**: Complete ETL implementation.
  - `extract.py`: Extracts raw data from CSV files.  
  - `transform.py`: Cleans and transforms data to fit the dimensional model.  
  - `load.py`: Loads transformed data into MySQL.  
- **`main.py`**: Orchestrates the entire ETL process.  
- **`reports/`**: Scripts for KPI generation and visualizations.  

## KPIs and Analysis

These **key indicators** help answer the project's main analytical questions:
- **Relationship between biodiversity and pollution**  
	Compare `Species Count` vs `Microplastics Measurement`.
- **Identification of critical zones**  
  - High biodiversity + high pollution.  
  - Low biodiversity + high pollution.  
- **Pollution over time**  
  Track historical trends in microplastics concentration.
- **Geographic distribution of pollution**  
  Maps and visualizations by **region**, **ocean**, and **country**.

## How to Run the Project
### 1. Clone the repository
```bash
git clone https://github.com/nataliap2005/ods14-marine-etl
cd ods14-marine-etl
```
### 2. Install dependencies
It is recommended to use a virtual environment:
```
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows

pip install -r requirements.txt
```
### 3. Configure MySQL connection
Edit the DB/create_db.py file and update credentials if needed:
```
USER = "root"
PASSWORD = "root"
HOST = "localhost"
PORT = 3306
```
### 5. Run the full ETL pipeline
```
python main.py
```
## Datasets
- Marine Species Richness: Predictive models of marine biodiversity based on AquaMaps and environmental parameters.
- Marine Microplastics: Historical data of microplastic sampling collected by NOAA since 1972.
Both datasets come from trusted sources such as SPREP and NOAA, ensuring quality and reliability of the information used.
