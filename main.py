from ETL.extract import extract
from ETL.transform import transform
def main():
    file_path_microplastics = 'data/MarineMicroplastics.csv'
    file_path_species = 'data/MarineSpeciesRichness.csv'

    df_microplastics = extract(file_path_microplastics)
    df_species = extract(file_path_species)

    df = transform(df_microplastics, df_species)
    df.info()

if __name__ == '__main__':
    main()