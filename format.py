#!/usr/bin/env python

import pandas as pd
import unicodedata
from tqdm import tqdm
import re
import os

from common import load_conf

def get_municipalities():
    """
    Returns dictionaries to map municipality names and codes
    for every year between 2005 and 2017, and one for years since 2018.
    
    The source are the "Estructura de Establecimientos" tables published by
    the health ministry, which are used in the software where original forms are filled. 
    Some can be accesed at https://snis.minsalud.gob.bo/publicaciones/category/20-estructura.
    """
    
    def format_sheet(sheet):
        """
        Expects a table with 2 columns: municipalities and codes.
        Cleans them up and returns a dictionary.
        """
        
        sheet.columns = ['municipality', 'municipality_id']
        sheet = sheet.drop_duplicates()
        sheet = sheet[(sheet.municipality.notna()) & (sheet.municipality_id.notna())]
        sheet.municipality = sheet.municipality.apply(lambda _: ' '.join(_.split()).strip())
        sheet.municipality_id = sheet.municipality_id.astype(int)
        
        return sheet.set_index('municipality').municipality_id.to_dict()
        
    municipalities = {}
    
    # Dictionaries for years between 2005 and 2017.
    excel = pd.ExcelFile('supplements/Establecimientos 2005_2017.xlsx')
    for year in excel.sheet_names:
        sheet = pd.read_excel(excel, sheet_name=year, header=2, usecols=['MUNICIPIO', 'COD_MUNICIPIO'])
        sheet = sheet[['MUNICIPIO', 'COD_MUNICIPIO']].dropna()
        municipalities[year] = format_sheet(sheet)
        
    # 2018.
    sheet = pd.read_excel(
        'supplements/ESTRUCTURA CERRADA_2018_SNIS.xlsx', 
        sheet_name='estructura EESS 2018',
        header=5,
        usecols=['MUNICIPIO', 'COD_MUN'])
    sheet = sheet[['MUNICIPIO', 'COD_MUN']].dropna()
    municipalities['2018'] = format_sheet(sheet)
    
     # 2019.
    sheet = pd.read_excel(
        'supplements/ESTRUCTURA DE EE.SS. GESTION 2019 CERRADO.xlsx', 
        sheet_name='BASE DE DATOS',
        header=3,
        usecols=['MUN', 'COD_MUN'])
    sheet = sheet[['MUN', 'COD_MUN']].dropna()
    municipalities['2019'] = format_sheet(sheet)
    
    # 2020
    sheet = pd.read_excel(
        'supplements/ESTRUCTURA DE EE.SS. GESTION 2020_DASHBOARDcerrado oficial (13)PUBLICADO.xlsx', 
        sheet_name='BASE DE DATOS',
        header=3,
        usecols=['MUN', 'COD_MUN'])
    sheet = sheet[['MUN', 'COD_MUN']].dropna()
    municipalities['2020'] = format_sheet(sheet)
    
    # 2021.
    sheet = pd.read_excel(
        'supplements/ESTRUCTURA DE EE.SS. GESTION 2021_DASHBOARDcerrado oficial.xlsx', 
        sheet_name='BASE DE DATOS',
        header=3,
        usecols=['MUN', 'COD_MUN'])
    sheet = sheet[['MUN', 'COD_MUN']].dropna()
    municipalities['2020'] = format_sheet(sheet)
    municipalities['2021'] = format_sheet(sheet)
    
    # 2022
    sheet = pd.read_excel(
        'supplements/ESTRUCTURA DE EE.SS. GESTION 2022_DASHBOARDcerrado oficial.xlsx', 
        sheet_name='BASE DE DATOS',
        header=3,
        usecols=['MUN', 'COD_MUN'])
    sheet = sheet[['MUN', 'COD_MUN']].dropna()
    municipalities['2022'] = format_sheet(sheet)
    
    return municipalities

def format_table(filename):
    """
    Transforms a `raw` table into one with columns:
    - `department` and `municipality` of observations.
    - `municipality_id`: municipality identifier, 
    useful to join observations across years.
    - `year` and `month` of observations.
    - `population`: the group of people the observations refer to. 
    - and `value`: values observed.
    """
    
    known_fields = ['municipality', 'month']
    departments = {
        '1': 'Chuquisaca',
        '2': 'La Paz',
        '3': 'Cochabamba',
        '4': 'Oruro',
        '5': 'Potosí',
        '6': 'Tarija',
        '7': 'Santa Cruz',
        '8': 'Beni',
        '9': 'Pando'
    }
    
    def format_field(field):
        """
        Fixes strings in original column names.
        """
    
        unicode = (
            unicodedata.
            normalize('NFKD', field).
            encode('ascii', 'ignore').
            decode('ascii')
        )
        
        return re.sub('\.[0-9]$', '', unicode)
    
    def format_header(dataframe):
        """
        Fixes column headers, joins multi-row headers with `/`.
        """
        
        df = dataframe.copy()
        clean_header = []
        header = (df[df.month.isna()].
                   T.
                   reset_index().
                   values.
                   tolist())
        
        for column in header:
            
            known = [field for field in column if field in known_fields]
            if known:
                clean_header.append(known[0])
            
            else:
                fields = [format_field(field) for field in column if type(field) == str]
                clean_header.append(' / '.join(fields))
                
        return clean_header
        
    def reshape(dataframe):
        """
        Squezes tables into the expected columns.
        """
        
        return pd.melt(
            df, 
            id_vars=known_fields, 
            value_vars=[col for col in df.columns if col not in known_fields], 
            var_name='population', 
            value_name='value'
        )
    
    # Read the `raw` csv,
    # drop fields that would repeat across the whole table
    # and would fit better in a separate index.
    df = pd.read_csv(filename, parse_dates=['month'])
    df = df.drop(columns=['group', 'variable'])

    # Format column headers.
    df.columns = format_header(df)
    df = df[df.month.notna()]
    
    # If the table isn't empty
    if df.shape[0] > 0:
        
        # Squeze it into the expected shape
        df = reshape(df)

        # Insert columns for year and month.
        df.insert(1, 'year', df.month.dt.year)
        df['month'] = df.month.dt.month

        # Insert a column for the municipality identifier.
        year = df.iloc[0].year
        df.insert(
            0, 
            'municipality_id', 
            df.municipality.map(municipalities[str(year)])
        )

        # Make the municipality name more legible.
        df.municipality = df.municipality.apply(lambda _: ' '.join([w[0].upper() + w[1:].lower() for w in _.split()]))

        # Insert a column for the department.
        df.insert(
                0, 
                'department', 
                df.municipality_id.astype(str).apply(lambda _: departments[_[0]])
            )

        # If there's no empty value, make sure all values are integers.
        # Empty values are left empty.
        if df.value.isna().sum() == 0:
            df.value = df.value.astype(float).astype(int)

    else:
        
        # If the table is empty, leave it empty but with similar columns.
        df = pd.DataFrame(columns=['department', 'municipality_id', 'municipality', 'year', 'month', 'population', 'value'])
        
    return df

filenames = load_conf(['filenames'])

# Make municipality dictionaries
municipalities = get_municipalities()

# Load the index of downloaded raw files.
downloaded = pd.read_csv(
    'indexes/raw.csv',
    dtype={'filename': str, 'year':int, 'group_id':str, 'group':str, 'variable_id':str, 'variable':str}
)

# Prepare filenames for new clean files.
clean = downloaded[['year', 'group', 'variable', 'filename']].copy()
clean['clean_filename'] = clean.filename.apply(lambda _: f"clean/{'/'.join(_.split('/')[1:])}")
clean['clean_directory'] = clean.filename.apply(lambda _: f"clean/{'/'.join(_.split('/')[1:-1])}")

# Format each raw file,
# extract some additional metadata for an index,
# and save as csv.

unique_municipalities = []
nonempty_values = []

for i, row in tqdm(clean.iterrows(), total=downloaded.shape[0]):
    df = format_table(row['filename'])
    unique_municipalities.append(
        len(df.municipality_id.unique())
    )
    nonempty_values.append(
        df.value.notna().sum()
    )
    os.makedirs(row['clean_directory'], exist_ok=True)
    df.to_csv(row['clean_filename'], index=False)

# Prepare and save an index
clean['municipalities'] = unique_municipalities
clean['values'] = nonempty_values
clean = clean[['year', 'group', 'variable', 'municipalities', 'values', 'clean_filename']]
clean.columns = ['year', 'disease_group', 'disease', 'municipalities', 'values', 'file']
clean.to_csv(filenames['clean'], index=False)
clean.file = clean.file.apply(lambda _: f"[csv]({_})")
clean_index = clean.to_markdown(index=False)
with open('datasets.md', 'w+') as f:
    f.write(f"# Available datasets\n\n{clean_index}")
