"""
The programme creates the sample of podcasts that have been indcluded on the top news podcasts ranking

I remove the following values:

TIME:
- Podcasts published before 2021-07-12 and after 2020-05-01

PRODUCER
- Podcasts that belong to a producer with only one podcast in the ranking
- Podcasts that belong to producers that do not create news or english content
    * e.g. Joe biden has a podcast and universities have podcasts

PODCAST
- Podcasts that have been fewer than 30 days in a on the ranking
- Non-news podcasts (e.g. series about a historic event)
- TV shows (e.g. Anderson Cooper --> they won't care about the ranking)


Created by: Patrick Hallila 10/08/2022

"""
#Data manipulation
from tkinter import N
from unicodedata import name
import pandas as pd
from datetime import timedelta

#Text processing
import re

DATA_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//apple_news_ranking.csv'
SAVE_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//sample.csv'
SAVE_PATH_PODCASTS_TO_INCLUDE = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//podcsts_to_include.csv'
MAX_TIME = '2021-07-12'
MIN_TIME = '2020-05-01'

#The lists below are created manually
NEWS_PRODUCERS = ['abc news', 'axios', 'bbc', 'betches media',
       'blaze podcast network', 'bloomberg','cafe', 'cbs news radio',
       'cnbc', 'cnn','coindesk', 'crooked media', 'fox', 'fox news radio','gingrich 360',
       'iheartpodcasts', 'kfi am 640 (kfi-am)', 'lemonada media', 'marketplace',
          'msnbc', 'national review', 'npr',
       'pbs newshour','podcastone', 'politico', 'politicon','premiere networks','radio america',
           'ride home media', 'salem podcast network','siriusxm', 'slate podcasts', 'soundfront',
           'the appeal', 'the australian', 'the black effect and iheartpodcasts', 'the bulwark',
           'the daily beast', 'the daily wire','the dispatch', 'the dsr network', 'the economist',
           'the intercept', 'the new york times', 'the north star', 'the wall street journal', 'the washington post',
           'vice', 'vox', 'wnyc studios','wnyc studios and the new yorker', 'wonder media network']


NON_NEWS_PODCASTS = ['1619',
 "the teacher's pet",
 '20/20',
 'trump, inc.',
 'foundering',
 'missing america',
 'the deciding decade with pete buttigieg',
 ' no compromise',
 'q clearance: the hunt for qanon',
 ' verdict with ted cruz',
 'how it happened',
 'the lazarus heist',
 'end of days',
 'bag man',
 'rush limbaugh: the man behind the golden eib microphone',
 'death in ice valley', 'npr news now','the new yorker radio hour'
]

TV_SHOWS_TO_DROP = ['pbs newshour - full show',
 'the 11th hour with stephanie ruhle',
 'fox news radio hourly newscast',
 'anderson cooper 360',
 'cnn tonight',
 'the five',
 '60 minutes',
 'the one w/ greg gutfeld',
 'squawk on the street',
 'world news tonight with david muir',
 'the lead with jake tapper',
 'vice news reports'
]

def print_current_dataset_dimension(df, prev_func_name):
    print(f'After performing {prev_func_name} the dataset has {df.shape[0]:,} rows and {df.shape[1]} columns')
    print(f'Producers: {df.producer.nunique()} and Podcasts: {df["id"].nunique()}\n')


def download_data(path = DATA_PATH):
    
    df = pd.read_csv(path)
    print("Column names: {}".format(df.columns))
    print_current_dataset_dimension(df, 'download_data')

    return df

def rename_producer(producer):
    if producer == 'iheartradio':
        return 'iheartpodcasts'
    elif re.match("msnbc", producer):
        return 'msnbc'
    elif re.match("bbc", producer):
        return "bbc"
    elif re.match("fox", producer):
        return "fox"
    return producer


def clean_columns(df):
    
    # Clean strings
    lowercase_and_remove_extra_spaces = lambda x: x.lower().strip()
    df['podcastName'] = df['podcastName'].apply(lowercase_and_remove_extra_spaces)
    df['producer'] = df['producer'].apply(lowercase_and_remove_extra_spaces)

    # Fix data types
    df['rank'] = df['rank'].astype('int')
    df['date'] = pd.to_datetime(df['date'])

    # Rename producers
    df['producer'] = df['producer'].apply(rename_producer)

    print("\nData is cleaned")
    print_current_dataset_dimension(df, 'clean_columns')
    return df

def cut_timeframe(df, max_time = MAX_TIME, min_time = MIN_TIME):
    df = df[
        (df.date<=max_time)&
        (df.date >= min_time)
    ]
    print_current_dataset_dimension(df, 'cut_timeframe')
    return df

def cut_small_producers(df):
    #Find producers with more than one podcast in the ranking
    producers = df[['producer', 'id']].drop_duplicates().groupby('producer').count().reset_index()
    producers = producers[producers.id > 1]
    producers_to_include = producers.producer.unique()
    del producers

    #Include only the producers with more than 1 podcast on the ranking
    df = df[df.producer.isin(producers_to_include)]
    print_current_dataset_dimension(df, 'cut_small_producers')
    return df

def remove_non_news_producers(df, producers_to_keep = NEWS_PRODUCERS):
    df = df[df.producer.isin(producers_to_keep)]
    print_current_dataset_dimension(df, 'remove_non_news_producers')
    return df

def cut_podcasts_with_insufficient_data(df, min_streak_length=30):
    temp = pd.DataFrame()

    for podcast_id in df.id.unique():
        
        
        podcaster = df[df.id == podcast_id].copy()
        podcaster = podcaster['date'].to_frame()
        podcaster['prev_date'] = podcaster['date'].shift()
        podcaster['streak'] = podcaster['date'] - podcaster['prev_date']
        
        streak = [1]
        
        for i, row in podcaster[1:].iterrows():
            if row.streak == timedelta(days=1):
                streak.append(streak[-1]+1)
            else:
                streak.append(1)
        podcaster['streak_length'] = streak
        podcaster['id'] = podcast_id
        
        temp = pd.concat([temp, podcaster])
        del podcaster
    
    df = df.merge(temp[['date', 'id', 'streak_length']], on = ['date', 'id'], how='left')

    df = df[df.streak_length >= min_streak_length]
    print_current_dataset_dimension(df, 'cut_podcasts_with_insufficient_data')
    return df

def cut_non_news_podcasts(df, podcasts_to_drop, print_message):
    # I need ids because some how misspelled names
    non_news_podcasts_to_drop_ids = df[df.podcastName.isin(podcasts_to_drop)].id.unique()

    # Check that I found all the ids
    if len(non_news_podcasts_to_drop_ids) != len(non_news_podcasts_to_drop_ids):
        print("COULDN'T FIND ALL THE IDS\nFollowing podcasts are missing:\n")
        for podcast in podcasts_to_drop:
            if len(df[df.podcastName==podcast])==0:
                print(podcast)
    
    df = df[~df.id.isin(non_news_podcasts_to_drop_ids)]
    print_current_dataset_dimension(df, print_message)
    return df

    

def main():

    # Download data
    data = download_data(path = DATA_PATH)

    # Cut timeframe
    data = cut_timeframe(data)

    # Clean columns
    data = clean_columns(data)

    # Cut podcasts that belong to a producer with only 1 podcast in the ranking
    data = cut_small_producers(data)

    # Cut producers that do not create news
    data = remove_non_news_producers(data)

    # Cut podcasts that have been fewer than 30 days in a row included in the ranking
    data = cut_podcasts_with_insufficient_data(data, min_streak_length=30)

    # Cut non-news podcasts
    data = cut_non_news_podcasts(data, NON_NEWS_PODCASTS, 'cut_non_news_podcasts (series)')

    # Cut TV shows
    data = cut_non_news_podcasts(data, TV_SHOWS_TO_DROP, 'cut_non_news_podcasts (TV)')

    # Save dataset
    data.to_csv(SAVE_PATH, index=False)

    #Save df with all podcasts indcluded
    data[['podcastName', 'producer']].drop_duplicates().to_csv(SAVE_PATH_PODCASTS_TO_INCLUDE, index=False)

if __name__ == "__main__":
    main()
