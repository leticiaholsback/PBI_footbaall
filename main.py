import requests
import json
import pandas as pd
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from google.colab import auth
auth.authenticate_user()

API_KEY = '66fa5192ea29b6e667661468f424eb01'
API_URL = 'https://v3.football.api-sports.io'
PROJECT_ID = 'betfullx-soccer'
DATASET_NAME = 'soccer'
FULL_LOAD_DATE = '2023-01-01'
LEAGUE = 2
SEASON = 2023
HEADERS = {
    'x-rapidapi-key': API_KEY
}

endpoints = [
    {
        "table": "past_fixtures",
        "write_disposition": "WRITE_APPEND",
        "path": "fixtures",
        "quality_control": False,
        "params": {
            "league": LEAGUE,
            "season": SEASON
        },
        "incremental_load_params": {
            "from": "YYYY-MM-DD",
            "to": "YYYY-MM-DD"
        },
        "fields": [],
        "nested_fields": [
            "fixture.id",
            "fixture.referee",
            "fixture.timezone",
            "fixture.date",
            "fixture.timestamp",
            "fixture.periods.first",
            "fixture.periods.second",
            "fixture.venue.id",
            "fixture.venue.name",
            "fixture.venue.city",
            "fixture.status.long",
            "fixture.status.short",
            "fixture.status.elapsed",
            "league.id",
            "league.name",
            "league.country",
            "league.logo",
            "league.flag",
            "league.season",
            "league.round",
            "teams.home.id",
            "teams.home.name",
            "teams.home.logo",
            "teams.home.winner",
            "teams.away.id",
            "teams.away.name",
            "teams.away.logo",
            "teams.away.winner",
            "goals.home",
            "goals.away",
            "score.halftime.home",
            "score.halftime.away",
            "score.fulltime.home",
            "score.fulltime.away",
            "score.extratime.home",
            "score.extratime.away",
            "score.penalty.home",
            "score.penalty.away"
        ],
        "repeatable_fields": []
    },
    {
        "table": "future_fixtures",
        "write_disposition": "WRITE_TRUNCATE",
        "path": "fixtures",
        "quality_control": False,
        "params": {
            "league": LEAGUE,
            "season": SEASON,
            "to": "2099-12-31"
        },
        "incremental_load_params": {
            "from": "YYYY-MM-DD",
        },
        "fields": [],
        "nested_fields": [
            "fixture.id",
            "fixture.timezone",
            "fixture.date",
            "fixture.timestamp",
            "fixture.venue.id",
            "fixture.venue.name",
            "fixture.venue.city",
            "fixture.status.long",
            "fixture.status.short",
            "league.id",
            "league.name",
            "league.country",
            "league.logo",
            "league.flag",
            "league.season",
            "league.round",
            "teams.home.id",
            "teams.home.name",
            "teams.home.logo",
            "teams.away.id",
            "teams.away.name",
            "teams.away.logo"
        ],
        "repeatable_fields": []
    },
    {
        "table": "players",
        "write_disposition": "WRITE_TRUNCATE",
        "path": "players",
        "quality_control": True,
        "params": {
            "league": LEAGUE,
            "season": SEASON,
            "page": 1
        },
        "fields": [],
        "nested_fields": [
            "player.id",
            "player.name",
            "player.firstname",
            "player.lastname",
            "player.age",
            "player.birth.date",
            "player.birth.place",
            "player.nationality",
            "player.height",
            "player.weight",
            "player.injured",
            "player.photo"
        ],
        "repeatable_fields": []
    },
]

iterable_endpoints = {
    "past_fixtures": [
        {
            "table": "fixturesStatistics",
            "write_disposition": "WRITE_APPEND",
            "path": "fixtures/statistics",
            "query_param": {
                "fixture": "fixture.id"
            },
            "fixed_params": {},
            "fields": ["fixture"],
            "nested_fields": [
                "team.id",
                "team.name",
                "team.logo"
            ],
            "repeatable_fields": [
                "statistics"
            ]
        },
        {
            "table": "fixturesLineups",
            "write_disposition": "WRITE_APPEND",
            "path": "fixtures/lineups",
            "query_param": {
                "fixture": "fixture.id"
            },
            "fixed_params": {},
            "fields": [
                "fixture",
                "formation"
            ],
            "nested_fields": [
                "team.id"
            ],
            "repeatable_fields": [
                "startXI"
            ]
        }
    ]
}



def fetch_data(path, params, headers):
    all_data = []
    while True:
        response = requests.get(f"{API_URL}/{path}", headers=headers, params=params)
        data = response.json()
        print(response.text)
        if 'response' in data and data['response']:
            all_data.extend(data['response'])
            if 'page' in params:
                params['page'] += 1
            else:
                break
        else:
            break
    return all_data

def fetch_iterable_data(main_data, iterable_endpoint):
    all_data = []
    for _, item in main_data.iterrows():
        query_params = {key: item[value.replace('.', '__')] for key, value in iterable_endpoint["query_param"].items()}
        params = query_params.copy()
        params.update(iterable_endpoint['fixed_params'])
        data = fetch_data(iterable_endpoint['path'], params, HEADERS)

        iterated_data = [{**query_params, **item} for item in data]
        all_data.extend(iterated_data)
    return all_data

def prepare_dataframe(data, fields, nested_fields, repeatable_fields):
    for item in data:
        for field in repeatable_fields:
            if field in item:
                for sub_item in item[field]:
                    for key in sub_item.keys():
                        sub_item[key] = str(sub_item[key])

    meta_fields = fields + repeatable_fields

    df = pd.json_normalize(data, sep='__', meta=meta_fields)

    all_fields = fields + nested_fields + repeatable_fields
    column_names = [col.replace('.', '__') for col in all_fields]

    existing_columns = [col for col in column_names if col in df.columns]

    df = df[existing_columns]
    return df

def create_dataset_if_not_exists(client, dataset_name):
    try:
        client.get_dataset(dataset_name)
    except NotFound:
        print(f"Dataset {dataset_name} not found. Creating...")
        client.create_dataset(dataset_name)

def load_data_to_bigquery(client, dataset_name, table_name, data, write_disposition):
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=write_disposition
    )
    json_str = data.to_json(orient='records', date_format='iso')
    job = client.load_table_from_json(json.loads(json_str), table_ref, job_config=job_config)
    job.result()
    print(f"Foram carregadas {len(data)} linhas em {dataset_name}.{table_name}")

def get_last_update(client, now):
    table_name = f'{PROJECT_ID}.{DATASET_NAME}.updates'
    query = f'SELECT MAX(updated_at) AS last_update FROM `{table_name}`'

    try:
        client.get_table(table_name)
    except NotFound:
        print(f"Table {table_name} not found. Initializing with FULL_LOAD_DATE: {FULL_LOAD_DATE}")
        full_load_date = datetime.strptime(FULL_LOAD_DATE, '%Y-%m-%d')
        initial_data = [{'updated_at': full_load_date}]
        load_data_to_bigquery(client, DATASET_NAME, 'updates', pd.DataFrame(initial_data), 'WRITE_TRUNCATE')
        return full_load_date

    try:
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            print(f"Ultima data de atualização: {row.last_update}")
            return row.last_update if row.last_update else now - timedelta(days=1)
    except Exception as e:
        print(f"An error occurred while executing the query: {e}")
        return None

def log_update(client,now):
    table_name = f'{PROJECT_ID}.{DATASET_NAME}.updates'
    updated_at = [{'updated_at': now}]
    load_data_to_bigquery(client, DATASET_NAME, 'updates', pd.DataFrame(updated_at), 'WRITE_APPEND')

def incremental_params_update(table, incremental_load_params, params, last_update, now):
    if table == "past_fixtures":
        params.update({
            'from': last_update.strftime('%Y-%m-%d'),
            'to': (now - timedelta(days=1)).strftime('%Y-%m-%d')
        })
    elif table == "future_fixtures":
        params.update({
            'from': now.strftime('%Y-%m-%d')
        })
    else:
        params
    return params

def main(data=None, context=None):
    client = bigquery.Client(project=PROJECT_ID)
    create_dataset_if_not_exists(client, DATASET_NAME)
    now = datetime.now()
    # now = datetime(2024, 6, 1, 0, 0)
    last_update = get_last_update(client, now)
    main_endpoints = endpoints.copy()
    main_iterable_endpoints = iterable_endpoints.copy()

    for endpoint in main_endpoints:
        params = endpoint.get('params', {})
        table = endpoint.get('table')
        incremental_load_params = endpoint.get('incremental_load_params')
        path = endpoint.get('path')
        fields = endpoint.get('fields')
        nested_fields = endpoint.get('nested_fields')
        repeatable_fields = endpoint.get('repeatable_fields')
        write_disposition = endpoint.get('write_disposition')
        quality_control = endpoint.get('quality_control', False)

        if incremental_load_params:
            params = incremental_params_update(table, incremental_load_params, params, last_update, now)

        raw_data = fetch_data(path, params, HEADERS)

        if raw_data:
            prepared_data = prepare_dataframe(raw_data, fields, nested_fields, repeatable_fields)
            load_data_to_bigquery(client, DATASET_NAME, table, prepared_data, write_disposition)

            if table in main_iterable_endpoints:
                for iterable_endpoint in main_iterable_endpoints[table]:
                    iterable_table = iterable_endpoint.get('table')
                    iterable_fields = iterable_endpoint.get('fields')
                    iterable_nested_fields = iterable_endpoint.get('nested_fields')
                    iterable_repeatable_fields = iterable_endpoint.get('repeatable_fields')
                    iterable_write_disposition = iterable_endpoint.get('write_disposition')

                    print(f"Buscando os dados iteráveis para o endpoint: {iterable_table}")
                    detailed_data = fetch_iterable_data(prepared_data, iterable_endpoint)

                    if detailed_data:
                        prepared_iterable_data = prepare_dataframe(detailed_data, iterable_fields, iterable_nested_fields, iterable_repeatable_fields)
                        load_data_to_bigquery(client, DATASET_NAME, iterable_table, prepared_iterable_data, iterable_write_disposition)
                    else:
                        print(f"Não foram encontrados dados para o endpoint: {iterable_table}")
        else:
            print(f"Não foram encontrados dados para o endpoint: {table}")

    log_update(client, now)
