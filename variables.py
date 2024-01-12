#!/usr/bin/env python

# Creates or updates the index of variables 
# for years available in the configuration file.

# Libraries

import pandas as pd
import requests
from bs4 import BeautifulSoup
import unicodedata
import os
import json
from tqdm import tqdm

from common import load_conf, load_headers, base_data

# SETUP
# --------------------------------------------

# Load the cookie, filenames and page definitions from a configuration file.
# Place the cookie in a convenient dictionary to be used in a requests call.

cookie, filenames, pages = load_conf(['cookie', 'filenames', 'pages'])
cookies = {
    'ASP.NET_SessionId': cookie
}
filename = filenames['variables']

# These are basic parameters that the server expects.
# We'll re-use the headers and tweak data for each call.

headers = load_headers()
data = base_data()

# DOWNLOAD, FORMAT AND SAVE FUNCTIONS
# --------------------------------------------

def initialize_state(url):
    """
    A first call to gather initial state parameters.
    """
    
    response = requests.get(url, cookies=cookies, headers=headers)
    html = BeautifulSoup(response.text, 'html.parser')
    update_state(html)

def update_state(html):
    """
    Gather state parameters from an html and update the global state variable.
    """
    
    global state
    state_nodes = ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']
    for node in state_nodes:
        state[node] = html.select(f'#{node}')[0]['value']

def get_variable_group(url, year, group_id):
    """
    Collect variables in a variable group and year.
    """

    # Get the page for a variable group.
    
    global data
    call_data = {
        '__EVENTTARGET': 'ctl00$MainContent$WebPanel2$List_grvar',
        'ctl00$MainContent$WebPanel2$List_gestion': str(year),
        'ctl00$MainContent$WebPanel2$List_grvar': group_id
    }
    data = {**data, **state}
    
    response = requests.post(
        url, 
        cookies=cookies, 
        headers=headers, 
        data={**data, **call_data}
    )
    
    html = BeautifulSoup(response.text, 'html.parser')
    update_state(html)

    # Collect and return all variables found.
    
    variable_group = ([
        {'variable_id': option['value'], 'variable': option.get_text().strip()} 
        for option in html.select('#MainContent_WebPanel2_Lista_subvar option')
    ])
    
    return variable_group
        
def get_year(year, page):
    """
    Collect variable groups and variables in a year.
    """

    try:
    
        global data
        call_data = {
            '__EVENTTARGET': 'ctl00$MainContent$WebPanel2$List_gestion',
            'ctl00$MainContent$WebPanel2$List_gestion': str(year),
            'ctl00$MainContent$WebPanel2$List_grvar': '01'
        }
        
        # Initialize state on first call.

        url = f'https://estadisticas.minsalud.gob.bo/Reportes_Vigilancia/{page}'
        initialize_state(url)
        data = {**data, **state}
            
        # Get the initial page in a year.

        response = requests.post(
            url, 
            cookies=cookies, 
            headers=headers, 
            data={**data, **call_data}
        )

        # Proceed if the page seems right.
        if response.status_code == 200:

            html = BeautifulSoup(response.text, 'html.parser')
            update_state(html)

            # A temporary list.

            variables_year = []
                
            # In each variable group

            for option in html.select('#MainContent_WebPanel2_List_grvar option'):
                
                # Collect definitions for the group and its variables.

                group = option.get_text().strip()
                group_id = option['value']
                variable_group = get_variable_group(url, year, group_id)
                
                # Add them to the global variables dictionary

                for variable in variable_group:
                    
                    variables_year.append({'group_id': group_id, 
                                            'group': group,
                                            'variable_id': variable['variable_id'],
                                            'variable': variable['variable']})
        
            # Update the variables dictionary once all data is collected.

            variables[year] = variables_year
        
        else:

            tqdm.write(f"Failed on year {year}: page looks wrong!")

    except Exception as e:

        tqdm.write(f"Failed on year {year}: {e}")

# Format function

def format_variables(variables):
    """
    Convert the variables dictionary into a nice dataframe.
    """
    
    variables_table = []
    
    for year in variables.keys():
        year_table = pd.DataFrame(variables[year])
        year_table.insert(0, 'year', int(year))
        variables_table.append(year_table)
        
    df = pd.concat(variables_table)

    for column in ['variable', 'group']:
        df[column] = df[column].str.replace('^[0-9\.\-]+ ', '', regex=True)
        df[column] = df[column].str.lower()
        df[column] = df[column].apply(
            lambda _: unicodedata.normalize('NFKD', _).encode('ascii', 'ignore').decode('ascii')
        )

    df['year'] = df['year'].astype(int)
    for column in ['group_id', 'variable_id']:
        df[column] = df[column].astype(str)
    
    return df.reset_index(drop=True)

def update_variables(filename, pages, force_new=False, selection=[]):
    """
    Get a table of variables and variable groups in a list of years.
    Available years are defined in the configuration file under `pages`.

    - If `force_new` is True, collect definitions for all years.
    - Else if `selection` is a list of years, collect definitions only for them.
    - Else if there's already a previous table of definitions, find available years 
    with no definitions and collect for them.
    - Else if this is the first run, collect definitions for all years.

    Then save the table as `filename`. 
    If there's a previous file, consolidate first. 

    """

    def get_data(years):
        """
        Get definitions for a list of years.
        """

        print(f"Collecting defintions for: {', '.join(list([str(y) for y in years]))}")

        for year in tqdm(years):
            get_year(year, pages[str(year)])
        
        if variables:
            return format_variables(variables)

    def quality_control(df):
        """
        Some simple quality controls like
        - Remove entries where groups or variables are empty.
        - Or duplicated entries.
        - And make sure years are integers
        """
        
        df = df[(df.group.notna()) & (df.variable.notna())].copy()
        df = df.drop_duplicates(subset=['year', 'group', 'variable'])
        df['year'] = df['year'].astype(int)
        return df

    # Get all definitions anew if force_new is True.

    if force_new:

        df = get_data(pages.keys())

    # Get only a selection of years.

    elif selection:

        df = get_data(selection)

    # Get only those remaining.

    elif os.path.exists(filename):

        previously = pd.read_csv(filename)
        years = [year for year in pages.keys() if int(year) not in previously.year.unique()]
        df = get_data(years)

    # Or, if this never ran before, get every year.

    else:

        df = get_data(pages.keys())

    # If there's any output.    

    if type(df) == pd.core.frame.DataFrame:

    # If there's already a previous file, consolidate all definitions.

        if os.path.exists(filename):

            previously = pd.read_csv(
                filename,
                dtype={'year':int, 'group_id':str, 'group':str, 'variable_id':str, 'variable':str}
                )
            df = pd.concat([previously, df])
            df = df.drop_duplicates(subset=['year', 'group_id', 'variable_id'])

        # A little quality control
        df = quality_control(df)

        # And finally save as csv.
        df.sort_values(['year', 'group_id', 'variable_id']).to_csv(filename, index=False)

# RUN
# --------------------------------------------

# Initialize state

state = {}
variables = {}

# Get definitions

update_variables(filename, pages)