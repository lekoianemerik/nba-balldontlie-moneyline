import pandas as pd
import requests
import time
import multiprocessing
from tqdm import tqdm

def get_stats_for_year_cursor(apikey, year, cursor=0, verbose=False):

    start_date = f'{year}-01-01'
    end_date   = f'{year}-12-31'
    
    resp = requests.get(
        url='https://api.balldontlie.io/v1/stats',
        params={
            'per_page': 100,
            'cursor': cursor,
            'start_date': start_date,
            'end_date': end_date
        },
        headers={
            'Authorization': apikey
        }
    )

    if resp.ok:
        if verbose:
            print(f'got cursor {cursor}')
            print(f'meta: {resp.json()['meta']}')
        
        df = pd.Series(resp.json()['data']).apply(pd.Series)
        df['player_id'] = df.player.apply(lambda x: x['id'])
        df['team_id'] = df.team.apply(lambda x: x['id'])
        df['game_id'] = df.game.apply(lambda x: x['id'])
        df = df.drop(columns=['player', 'team', 'game'])

        if 'next_cursor' in resp.json()['meta'].keys():
            next_cursor = resp.json()['meta']['next_cursor']
        else:
            next_cursor = None

        return df, next_cursor
    
    else:
        if verbose:
            print(f'FAIL cursor {cursor}')

        return None, None



def get_stats_for_year(apikey, year, sleep=0.0, verbose=False):

    dfs = []

    cursor = 0
    df, next_cursor = get_stats_for_year_cursor(apikey, year, cursor, verbose=False)
    dfs.append(df)

    cursor = next_cursor
    i = 0
    while cursor:
        i = i + 1
        df, next_cursor = get_stats_for_year(apikey, year, cursor, verbose=False)
        dfs.append(df)

        cursor = next_cursor

        time.sleep(sleep)
        if verbose:
            print(f'year={year}, iteration {i}')

    return pd.concat(dfs)


def get_stats_for_years(apikey, years, sleep=0.0, cores=None, verbose=False):

    if not cores:
        
        dfs = []
        for y in years:
            dfs.append(
                get_stats_for_year(apikey, y, sleep=sleep, verbose=verbose)
            )

        return pd.concat(dfs)

    else:
        
        apikeys = [apikey] * len(years)
        sleeps = [sleep] * len(years)
        verboses = [verbose] * len(years)
        args = zip(apikeys, years, sleeps, verboses)
        
        # TODO Verbose (tqdm?)
        with multiprocessing.Pool(cores) as p:
            dfs = list(tqdm(p.starmap(get_stats_for_year, args)))

        return pd.concat(dfs)



