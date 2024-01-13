## Open epidemiological surveillance data for Bolivia

See all [available datasets](datasets.md).

You can also download everything in [lightweight parquet files available under releases](https://github.com/mauforonda/epidemiological_surveillance_bolivia/releases/latest).

The Bolivian health ministry collects periodic data on case frequencies for all epidemiologically relevant diseases (`Form 302a`). And while most of these data are [accessible online](https://estadisticas.minsalud.gob.bo/), they're not easy to download and work with. This repository includes code to collect and format all available data:

- [conf.json](conf.json) features pointers to yearly pages, which are meant to be added once a year finishes.
- [variables.py](variables.py) builds an index of all diseases available in yearly pages.
- [data.py](data.py) downloads data in the `variables` index as minimally formatted tables stored under the `raw` directory. 
- [format.py](format.py) makes every `raw` table tidy and enriched with additional attributes like municipality identifiers, useful to compare observations across years, and saves them under the `clean` directory.

This workflow should make yearly updates relatively easy. Here are [some instructions](updates.md).

---

As of early January 2024, data before 2005 can't be accessed at the original website, but a copy is still in available in [a previous version of this project](https://github.com/mauforonda/vigilancia-epidemiologica).
