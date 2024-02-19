#!/usr/bin/env python

import pandas as pd
from tqdm import tqdm
from common import load_conf

filenames = load_conf(['filenames'])
index = pd.read_csv(filenames['clean'])

def read_file(filename, disease_group, disease):
    """
    Read a single `clean` file, append disease columns.
    """

    df = pd.read_csv(
        filename, 
        dtype={'department': 'category', 'municipality_id': int, 'municipality': 'category', 'year':int, 'month':int, 'population':str, 'value':float}
    )
    if df.shape[0] > 0:
        for col, value in zip(['disease', 'disease_group'], [disease, disease_group]):
            df.insert(0, col, value)
        return df

def create_release(index_rows, filename):
    """
    Read, concatenate and apply correct types to a collection
    of rows in the `clean` index, then save all as a parquet file.
    """

    complete = []
    
    for i, row in tqdm(index_rows.iterrows(), total=index_rows.shape[0]):
        
        complete.append(
            read_file(row['file'], row['disease_group'], row['disease'])
        )

    complete = pd.concat(complete)
    complete = complete.astype({
        'disease_group':'category', 'disease':'category', 'department': 'category', 'municipality_id': int, 'municipality': 'category', 'year':int, 'month':int, 'population':str, 'value':float
    })
    
    complete.sort_values(['year', 'month', 'department', 'disease_group', 'disease', 'population', 'municipality_id']).to_parquet(
        filename,
        engine='pyarrow',
        compression='zstd',
        index=False,
        row_group_size=1e7
    )

# Make a release for each year.
for year in index.year.unique():
    print(f"Release for {year} ...")
    create_release(index[index.year == year], f"releases/snis_{year}.parquet")
    
# And a release for all.
print("Release for all ...")
create_release(index, f"releases/snis_complete.parquet")