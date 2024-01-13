#!/usr/bin/env python

import json
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

def load_conf(entries, filename="conf.json"):
    """
    Load a list of configuration entries.
    """

    with open(filename) as f:
        conf = json.load(f)
        conf_entries = [conf[i] for i in entries]
        if len(conf_entries) > 1:
            return conf_entries
        else:
            return conf_entries[0]

def load_headers():
    """
    Headers used in all network calls.
    """
    
    return {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Origin': 'https://estadisticas.minsalud.gob.bo',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'
    }

def base_data():
    """
    Common data parameters used in all POST calls.
    """

    return {
        'ctl00$MainContent$WebPanel2_hidden': '',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        'ctl00$MainContent$WebPanel3_hidden': '%3CWebPanel%20Expanded%3D%22false%22%3E%3C/WebPanel%3E',
        'ctl00$MainContent$WebPanel2$List_fomulario': '302',
        'ctl00$MainContent$WebPanel2$Grupo': 'nomDepto',
        'ctl00$MainContent$WebPanel2$seleccion': '0'
    }

def requests_session(retries=20) -> requests.sessions.Session:

    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session