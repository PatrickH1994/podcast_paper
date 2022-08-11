"""
Creates the sample of episodes based on the file sample.csv

I choose all the episodes that the podcasts in the sample published within the time frame.

Steps:
1) Download the sample.csv
2) Download the episodes from apple api
3) Filter podcasters that are not in the sample
4) Filter episodes that are not in the sample
5) Save dataset

"""

import pandas as pd
from datetime import datetime

PODCASTER_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//sample.csv'
EPISODE_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//episodeInfo//popular_podcasts.csv'
SAVE_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//episodeInfo//episode_sample.csv'

START_DATE = "2020-05-01"
END_DATE = "2021-07-12"

def download_podcaster_sample(path):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    print(f"Sample includes {df.id.nunique()} podcasters and has shape {df.shape}")
    return df
def download_all_episodes(path):
    df = pd.read_csv(path)
    df.drop(columns=['Unnamed: 0'], inplace=True)
    print(f'Dataframe size: {df.shape}\nColumns: {list(df.columns)}')
    return df

def filter_podcasters(episodes, podcasters):
    print("Start function: filter_podcasters")
    id_lst = list(podcasters.id.unique())
    # The Victor Davis Hanson Show has the wrong id, so I add it to the list
    id_lst.append(1570380458)
    episodes = episodes[episodes.id.isin(id_lst)]
    print(f"New shape is: {episodes.shape}")

    #Check that all podcasters' episodes are collected
    if episodes.id.nunique() == len(id_lst)-1:
        print("All podcasts have episodes!")
    else:
        for id_ in id_lst:
            if id_ not in episodes.id.unique():
                print(f"Missing id: {id_}")
    return episodes

def is_ranked(row, podcasters):
    nr_of_episodes = len(podcasters[
        (podcasters.date==row.date)&
        (podcasters.id==row.id)
    ]) 
    if nr_of_episodes > 0:
        return 1
    return 0

def select_episodes(episodes, podcasters, start_date = START_DATE, end_date=END_DATE):
    episodes['date'] = pd.to_datetime(episodes.pubDate)
    episodes['date'] = episodes['date'].apply(lambda date: datetime(date.year, date.month, date.day))
    episodes = episodes[
        (episodes.date>=start_date)&
        (episodes.date<=end_date)&
        (episodes.episodeType=='full')
    ]
    print(f"After removing episodes based on timeframe and episode type: {episodes.shape}")

    #Check if the podcast was ranked when the episode was published
    episodes['isRanked'] = episodes.apply(lambda row: is_ranked(row, podcasters), axis=1)
    print(f'Number of ranked episodes: {episodes.isRanked.sum()}')
    print(f'Number of podcasters: {episodes.id.nunique()}')

    return episodes



def main():

    # Get datasets
    podcasters = download_podcaster_sample(PODCASTER_PATH)
    episodes = download_all_episodes(EPISODE_PATH)
    
    # Filter podcasters
    episodes = filter_podcasters(episodes, podcasters)

    #Filter episodes by date
    episodes = select_episodes(episodes, podcasters)

    #Save episodes
    episodes.to_csv(SAVE_PATH)


if __name__=="__main__":
    main()
