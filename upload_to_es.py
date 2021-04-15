import os

import eland
import pandas
from elasticsearch import Elasticsearch

client = Elasticsearch(
cloud_id=os.environ.get('CLOUD_ID'),
http_auth=(os.environ.get('ES_CLIENT_USERNAME'),
	 os.environ.get('ES_CLIENT_PASSWORD')
	 )
)

base_data = pandas.read_csv(
	'ripa_stops_datasd.csv',
	keep_default_na=False,
	parse_dates=["date_stop"],
	)

base_data[['beat_name', 'beat']] = base_data['beat_name'].str.rsplit(' ', 1, expand=True)
base_dfs = [base_data]

files = [
	'ripa_race_datasd.csv',
	'ripa_actions_taken_datasd.csv',
	'ripa_search_basis_datasd.csv',
	'ripa_stop_result_datasd.csv',
	'ripa_prop_seize_type_datasd.csv',
	'ripa_stop_reason_datasd.csv',
	'ripa_stop_reason_datasd.csv',
	'ripa_disability_datasd.csv',
]

for filename in files:
	base_dfs.append(
		pandas.read_csv(
			filename,
			keep_default_na=False,
			)
	)
	
for df in base_dfs:
	df['stop_pid_id'] = df['stop_id'].astype(str) + df['pid'].astype(str)
	df.set_index('stop_pid_id')	
	
dfs = pandas.concat(
	base_dfs,
	axis=1,
	join="inner"
).reset_index()

ed = eland.pandas_to_eland(
	dfs,
	es_client=client,
	es_dest_index="sd-ripa-basic-stop",
	es_if_exists="replace",
	es_refresh=True,
	)