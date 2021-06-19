import typer
import os

import eland
import pandas
from functools import reduce
from itertools import combinations
from elasticsearch import Elasticsearch

client = Elasticsearch(
    cloud_id=os.environ.get("CLOUD_ID"),
    http_auth=(
        os.environ.get("ES_CLIENT_USERNAME"),
        os.environ.get("ES_CLIENT_PASSWORD"),
    ),
)

app = typer.Typer()
base_data = pandas.read_csv(
            'ripa_stops_datasd.csv',
            keep_default_na=False,
            parse_dates=["date_stop"],
        )

base_data["stop_pid_id"] = base_data["stop_id"].astype(str) + base_data["pid"].astype(str)
base_data.set_index("stop_pid_id", inplace=True)

@app.command()
def update(filenames: list[str], include_base: bool=True, es_name: str=""):
    entries = []

    for filename in filenames:
        base_name = filename.split('.')[0]

        df = pandas.read_csv(
            filename,
            keep_default_na=False,
        )
        df["stop_pid_id"] = df["stop_id"].astype(str) + df["pid"].astype(str)
        df.set_index("stop_pid_id", inplace=True)
        entries.append(df)

    if include_base:
        entries.append(base_data)

    new_df = reduce(lambda left,right: pandas.merge(left,right), entries)

    if not es_name:
        es_name='-'.join(list(map(lambda x:x.split('.')[0], filenames)))


    eland.pandas_to_eland(
            new_df,
            es_client=client,
            es_dest_index=es_name,
            es_if_exists="replace",
            es_dropna=False,
        )
        
@app.command()
def combo(filenames: list[str]):
    combos = combinations(filenames, 2)

    for left, right in combos:
        left_df = pandas.read_csv(
            left,
            keep_default_na=False,
        )
        right_df= pandas.read_csv(
            right,
            keep_default_na=False,
        )

        for df in (left_df, right_df):
            df["stop_pid_id"] = df["stop_id"].astype(str) + df["pid"].astype(str)
            df.set_index("stop_pid_id", inplace=True)

        base_df = pandas.merge(left_df, base_data)
        names = '_'.join([x.split('.')[0] for x in [left, right]])

        combo_df = pandas.merge(base_df, right_df)

        eland.pandas_to_eland(
                combo_df,
                es_client=client,
                es_dest_index=names,
                es_if_exists="replace",
                es_dropna=False,
            )

if __name__ == "__main__":
    app()
