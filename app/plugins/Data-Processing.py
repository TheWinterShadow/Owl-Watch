# Created by The White Wolf

import pandas as pd
import numpy as np


def main() -> None:
    us_cases, us_deaths, global_cases, global_deaths, global_recovered = load_data()

    us_cases = clean_us_data(us_cases)
    export_data(us_cases, 'us_cases.pkl')

    us_deaths = clean_us_data(us_deaths)
    export_data(us_deaths, 'us_deaths.pkl')

    global_cases = clean_global_data(global_cases)
    export_data(global_cases, 'global_cases.pkl')

    global_deaths = clean_global_data(global_deaths)
    export_data(global_deaths, 'global_deaths.pkl')

    global_recovered = clean_global_data(global_recovered)
    export_data(global_recovered, 'global_recovered.pkl')


def export_data(df, pickle_name) -> None:
    df.to_pickle('../static/data/{}'.format(pickle_name))


def load_data() -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    us_cases = pd.read_csv('https://web.netsecure.dev/files/covid/US_cases.csv')
    us_deaths = pd.read_csv('https://web.netsecure.dev/files/covid/US_deaths.csv')

    global_cases = pd.read_csv('https://web.netsecure.dev/files/covid/global_cases.csv')
    global_deaths = pd.read_csv('https://web.netsecure.dev/files/covid/global_deaths.csv')
    global_recovered = pd.read_csv('https://web.netsecure.dev/files/covid/global_recovered.csv')

    return us_cases, us_deaths, global_cases, global_deaths, global_recovered


def clean_global_data(df) -> pd.DataFrame:
    # Rename columns.
    df.rename(columns={'Province/State': 'Province', 'Country/Region': 'Country'}, inplace=True)

    # Change NaN to ''
    df['Province'].fillna('', inplace=True)

    # Create new column with Province-Country
    df['Index'] = df['Province'] + '-' + df['Country']

    # Replace any index that doesn't have a Province with Country
    df['Index'] = df['Index'].apply(lambda x: x.split('-')[1] if x.split('-')[0] == '' else x)

    # Set index
    df.set_index('Index', inplace=True)
    df.index.name = None

    # Make a Total column
    dates = list(df.columns.drop(['Country', 'Province', 'Lat', 'Long']))
    df['Total'] = df[dates].sum(axis=1)

    # Sort by Total (High to Low)
    df.sort_values(by='Total', ascending=True, inplace=True)

    return df


def clean_us_data(df) -> pd.DataFrame:
    # Drop useless columns and rename useful ones.
    df.drop(['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Country_Region', 'Combined_Key'], inplace=True, axis=1)
    df.rename(columns={'Admin2': 'County', 'Province_State': 'State', 'Long_': 'Long'}, inplace=True)

    # Change NaN to ''
    df['County'].fillna('', inplace=True)

    # Create new column with State-County
    df['Index'] = df['State'] + '-' + df['County']

    # Replace any index that doesn't have State with County
    df['Index'] = df['Index'].apply(lambda x: x.split('-')[1] if x.split('-')[0] == '' else x)

    # Replace values that have 'Out of' or 'Unassigned' with NaN
    df['Index'] = df['Index'].apply(
        lambda x: np.nan if x.split('-')[1][:6] == 'Out of' or x.split('-')[1] == 'Unassigned' else x)

    # Drop rows that have NaN in the Index column
    df = df[df['Index'].notna()]

    # Set index
    df.set_index('Index', inplace=True)
    df.index.name = None

    # Make a Total column
    dates = list(df.columns.drop(['County', 'State', 'Lat', 'Long']))
    df['Total'] = df[dates].sum(axis=1)

    # Sort by Total (High to Low)
    df.sort_values(by='Total', ascending=True, inplace=True)

    return df


if __name__ == '__main__':
    main()

