#!/usr/bin/env python

import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import datetime as dt
import os
from slugify import slugify
from tqdm import tqdm

from common import load_conf, load_headers, base_data, requests_session

cookie, filenames, pages = load_conf(['cookie', 'filenames', 'pages'])
cookies = {
    'ASP.NET_SessionId': cookie
}

headers = load_headers()
data = base_data()
session = requests_session()

def initialize_state(url, headers, cookies):
    """
    A first call to gather initial state parameters.
    """
    
    response = session.get(url, cookies=cookies, headers=headers, timeout=30)
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

def initialize_recollection(url, year, group_id):
    """
    Necessary dance before collecting data.
    """

    def initial_post_call(url, call_data):

        response = session.post(
            url,
            cookies=cookies,
            headers=headers,
            data={**data, **call_data, **state},
            timeout=30
        )

        html = BeautifulSoup(response.text, 'html.parser')
        update_state(html)
    
    initial_post_call(
        url,
        {
            '__EVENTTARGET': 'ctl00$MainContent$WebPanel2$List_gestion',
            'ctl00$MainContent$WebPanel2$List_grvar': '01',
            'ctl00$MainContent$WebPanel2$List_gestion': str(year),
        })

    initial_post_call(
        url,
        {
            '__EVENTTARGET': 'ctl00$MainContent$WebPanel2$List_grvar',
            'ctl00$MainContent$WebPanel2$List_grvar': str(group_id),
            'ctl00$MainContent$WebPanel2$List_gestion': str(year),
            'ctl00$MainContent$WebPanel2$List_mes': '1',
            'ctl00$MainContent$WebPanel2$Grupo': 'nomMunicip',
        })

def get_month(url, year, month, group_id, variable_id):
    """
    Downloads data for a month. Returns a dataframe 
    with all original columns plus one for the month.
    """

    def parse_table(html):
        """
        Parse the html table, remove rows representing totals.
        """
        
        table = html.select('#G_MainContentxWebPanel3xmydatagrid')[0]
        df = pd.read_html(StringIO(str(table)))[0]
        df = df.set_index(df.columns[0])
        df = df[~df.index.str.lower().str.contains('total')]
        df.index.name = 'municipality'
        return df

    call_data = {
        '__EVENTTARGET': '',
        'ctl00$MainContent$WebPanel2$List_grvar': str(group_id),
        'ctl00$MainContent$WebPanel2$List_gestion': str(year),
        'ctl00$MainContent$WebPanel2$Grupo': 'nomMunicip',
        'ctl00$MainContent$WebPanel2$Button1': ' Procesar',
        'ctl00$MainContent$WebPanel2$List_mes': str(month),
        'MainContentxWebPanel3xmydatagrid': '',
        'MainContentxWebPanel3xmydatagrid2': '',
        'ctl00$MainContent$WebPanel2$Lista_subvar': str(variable_id)
    }

    response = session.post(
        url, 
        cookies=cookies, 
        headers={**headers, **{'Referer': url}}, 
        data={**data, **call_data, **state},
        timeout=30
        )
    html = BeautifulSoup(response.text, 'html.parser')
    
    df = parse_table(html)
    df.insert(0, 'month', dt.date(year, month, 1))
    
    return df

def get_data(row_dict):
    """
    Downloads data by month and municipality from `url` 
    with parameteres `row_dict`. Returns a single dataframe with
    columns for municipality, group, variable, month plus all 
    other columns in the original table.
    """

    filename, year, group_id, group, variable_id, variable = row_dict.values()
    url = f'https://estadisticas.minsalud.gob.bo/Reportes_Vigilancia/{pages[str(year)]}'

    try:
    
        months_data = []

        # Initial dance.
        initialize_state(url, headers, cookies)
        initialize_recollection(url, year, group_id)
        
        # Download data for each month
        for month in range(1,13):
            month_data = get_month(url, year, month, group_id, variable_id)
            months_data.append(month_data)

        # Concatenate into a single yearly table
        year_data = pd.concat(months_data)
        year_data.insert(0, 'variable', variable)
        year_data.insert(0, 'group', group)

        return year_data
    
    except Exception as e:
        pass

def slug(text):
    """
    Fix text for file and directory names.
    """
    
    return slugify(text, replacements=[['+', 'mas']])

def save_raw(variable_data, row_dict):
    """
    Save data, update an index with all saved files.
    """

    def update_index(row_dict):

        if os.path.exists(filenames['raw']):

            raw_index = pd.read_csv(
                filenames['raw'],
                dtype={'filename': str, 'year':int, 'group_id':str, 'group':str, 'variable_id':str, 'variable':str}
                )
            
            raw_index.loc[len(raw_index)] = row_dict
            raw_index = raw_index.drop_duplicates(subset=['filename'])

        else:

            raw_index = pd.DataFrame([row_dict])
            
        raw_index.to_csv(filenames['raw'], index=False)

    filename, year, group_id, group, variable_id, variable = row_dict.values()
    directory = f"raw/{year}/{slug(group)}"
    os.makedirs(directory, exist_ok=True)
    variable_data.to_csv(filename)
    
    update_index({**{'filename': filename}, **row_dict})

def download_data():

    def get_remaining():

        variables = pd.read_csv(
            filenames['variables'],
            dtype={'year':int, 'group_id':str, 'group':str, 'variable_id':str, 'variable':str}
        )
        variables.insert(
            0, 
            'filename', 
            variables.apply(lambda _: f"raw/{_['year']}/{slug(_['group'])}/{slug(_['variable'])}.csv", axis=1)
        )

        downloaded = pd.read_csv(
            filenames['raw'],
            dtype={'filename': str, 'year':int, 'group_id':str, 'group':str, 'variable_id':str, 'variable':str}
        )

        return pd.concat([variables, downloaded]).drop_duplicates(subset=['filename'], keep=False)


    def download_row(variable_dict, variable_row):

        variable_data = get_data(variable_dict)
        if type(variable_data) == pd.core.frame.DataFrame:
            save_raw(variable_data, variable_dict)
        else:
            with open(filenames['download_errors'], 'a+') as f:
                f.write(f'\n{str(variable_row)}')

    remaining = get_remaining()
    print(f"{remaining.shape[0]} variables remaining ...")
    for i, variable in tqdm(remaining.iterrows(), total=remaining.shape[0]):
        download_row(variable.to_dict(), i)

state = {}
download_data()