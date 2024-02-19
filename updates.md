To add data for a new year

## Prepare some information

- Make sure the page is complete (it'll be labelled as `CERRADO` in the [main index](https://estadisticas.minsalud.gob.bo/)) and it points to the `Form 302a`. 
- Add the new page url to `conf.json` as a new year under `pages`.
- Find the `Estructura de Establecimientos` excel file for the year, which will sometimes be listed [here](https://snis.minsalud.gob.bo/publicaciones/category/20-estructura). Otherwise search for it under the `snis.misalud.gob.bo` domain. This file contains a list of all health facilities with municipality names and identifiers, which are used by `format.py` to add a column for municipality identifiers. Municipality names can be written in many different ways and often change across years, so these identifiers are important to construct time series from these data. Place the file under the `supplements` directory and edit the `get_municipalities` function in `format.py` to handle it.

## Then run

- Run [variables.py](variables.py) to construct the [inventory of available datasets](indexes/variables.csv).
- Run [data.py](data.py) to download all data in the inventory. These data will be saved under `raw`. This download may take many hours or fail completely if the server refuses our connections, but `data.py` will pick up where it left off any time it's retried until it finishes
- Run [format.py](format.py) to make `raw` data tidy and save it under the `clean` directory.
- And finally run [release.py](release.py) to package data in parquet files ready to publish under [releases](https://github.com/mauforonda/epidemiological_surveillance_bolivia/releases/latest).

