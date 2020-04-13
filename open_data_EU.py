from datetime import date
import os.path
import requests
import pandas as pd 

def download_data(new_file_name):
    if os.path.isfile(new_file_name):
        print("file aready present, no need to downloand again!")
    else:
        print("downloanding data!")
        response = requests.get(url)
        with open(new_file_name, 'wb') as f:
            f.write(response.content)

def aggregation_check(raw_df, aggregated_file):
    if os.path.isfile(aggregated_file):
        agg_df    = pd.read_csv(aggregated_file, dtype=str)
        left_join = pd.merge(raw_df,
                            agg_df[['dateRep', 'countriesAndTerritories', 'total_cases']],
                            on=['dateRep', 'countriesAndTerritories'], 
                            how='left')
        new_count = left_join['total_cases'].isnull().sum()
        if new_count == 0:
            return False
        return True
    else:
        return True

url = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'
today = date.today().strftime("%Y-%m-%d")
new_file = 'eu_data_'+today+'.csv'
aggregated_file = 'eu_data_aggregated.csv'

download_data(new_file)

df = pd.read_csv(new_file, dtype=str)
aggregation_needed = aggregation_check(df, aggregated_file)
if aggregation_needed:
    print("aggregating data!")
    df['cases'] = pd.to_numeric(df.cases, downcast='integer')
    df['deaths'] = pd.to_numeric(df.deaths, downcast='integer')
    df['Date']=df.apply(lambda x: pd.to_datetime(x.day.zfill(2) + "-" + x.month.zfill(2) + "-" + x.year, format='%d-%m-%Y'), axis = 1)

    df.sort_values(['countriesAndTerritories','Date'], ascending=[True,True],inplace=True)
    df['cummulative_cases']  = df.groupby('countriesAndTerritories')['cases'].cumsum()
    df['cummulative_deaths'] = df.groupby('countriesAndTerritories')['deaths'].cumsum()

    df['prev_date_cases']  = df.groupby('countriesAndTerritories')['cases'].shift(1).fillna(0)
    df['prev_date_deaths'] = df.groupby('countriesAndTerritories')['deaths'].shift(1).fillna(0)

    df['new_cases_less_than_prev']  = df.apply(lambda x: 1 if x.cases <= x.prev_date_cases else 0, axis = 1)
    df['new_deaths_less_than_prev'] = df.apply(lambda x: 1 if x.deaths <= x.prev_date_deaths else 0, axis = 1)

    df['total_cases']  = pd.to_numeric(df.groupby('countriesAndTerritories')['cases'].transform('sum'), downcast='integer')
    df['total_deaths'] = pd.to_numeric(df.groupby('countriesAndTerritories')['deaths'].transform('sum'), downcast='integer')

    df['times_reduced_last_7_days'] = df.apply(lambda x: df[df['countriesAndTerritories']==x.countriesAndTerritories]['new_cases_less_than_prev'][-7:].sum(), axis = 1)
    print("aggregation done!")


    df.to_csv(aggregated_file)
    print("aggregated data saved!")
else:
    print("aggregated already, no need to aggregate again!")