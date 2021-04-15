# San Diego RIPA Police Database

Visualize RIPA data and upload to Elastic Cloud for visualization.

## Steps

- [Create Cloud Account](https://cloud.elastic.co) create deployment (or choose an existing one)
- Store `Username`/`Password` as environment variables `ES_CLIENT_USERNAME`, `ES_CLIENT_PASSWORD` (Respectively)
- Store cloud_id as `CLOUD_ID`
- Download RIPA Data (<https://data.sandiego.gov/datasets/police-ripa-stops/>) store in project root.
- Create a [virtual environment](https://docs.python.org/3/tutorial/venv.html) and install requirements `pip install -r requirements.txt`
- run `python upload_to_es.py`

## License

This is [MIT Licensed](https://github.com/kjaymiller/sd-ripa-data/blob/main/LICENSE)

## Support and Disclaimers

This is a project for work (I work with Elastic). I am paid to work on this but not specifically this topic.
