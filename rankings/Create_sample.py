"""
The programme creates the sample of podcasters and 
podcasts that have been indcluded on the top news podcasts ranking

I remove the following values:

TIME:
- Podcasts published before 2021-07-12 and after 2020-05-01

PRODUCER
- Podcasts that belong to a producer with only one podcast in the ranking
- Podcasts that belong to producers that do not create news or english content
    * e.g. Joe biden has a podcast and universities have podcasts

PODCAST
- Podcasts that have been fewer than 8 days in a row on the ranking
- Non-news podcasts (e.g. series about a historic event)
- TV shows (e.g. Anderson Cooper --> they won't care about the ranking)


Created by: Patrick Hallila 25/08/2022

"""
#Data manipulation
from errno import EPIPE
from struct import pack
from tkinter import N
from unicodedata import name
import pandas as pd
from datetime import datetime
from datetime import timedelta

#Text processing
import re

#Path variables
DATA_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//apple_news_ranking.csv'
SAVE_PODCASTER_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//clean_files//sample.csv'
SAVE_PATH_PODCASTS_TO_INCLUDE = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//clean_files//220823podcasts_to_include.csv'
PODCAST_TRANSCRIPT_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//clean_files//220823podcasts_to_include_with_Transcript_links.csv'
EPISODE_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//episodeInfo//popular_podcasts.csv'
SAVE_EPISODES_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//rankings//clean_files//220823episodes_to_include.csv'

# Time variables
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
 'death in ice valley', 'npr news now','the new yorker radio hour',
 'caliphate', 'intrigue', 'election 101', 'winning wisconsin', 'chapo',
 'turnout with katie couric', 'the missing cryptoqueen'
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
 'vice news reports',
 'fareed zakaria gps',
 'the news with shepard smith',
 'election 2020: updates from the washington post',
 'the jboy show', 'pop alarm', 'fareed zakaria gps'
]

class podcasters:
    def print_current_dataset_dimension(df, prev_func_name):
        
        #Catches cases in which I don't use podcaster data
        try:
            print(f'After performing {prev_func_name} the dataset has {df.shape[0]:,} rows and {df.shape[1]} columns')
            print(f'Producers: {df.producer.nunique()} and Podcasts: {df["id"].nunique()}\n')
        except:
            pass


    def download_data(path = DATA_PATH):
        
        df = pd.read_csv(path)
        print("Column names: {}".format(df.columns))
        podcasters.print_current_dataset_dimension(df, 'download_data')

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
        df['producer'] = df['producer'].apply(podcasters.rename_producer)

        print("\nData is cleaned")
        podcasters.print_current_dataset_dimension(df, 'clean_columns')
        return df

    def cut_timeframe(df, max_time = MAX_TIME, min_time = MIN_TIME):
        df = df[
            (df.date<=max_time)&
            (df.date >= min_time)
        ]
        podcasters.print_current_dataset_dimension(df, 'cut_timeframe')
        return df

    def cut_small_producers(df):
        #Find producers with more than one podcast in the ranking
        producers = df[['producer', 'id']].drop_duplicates().groupby('producer').count().reset_index()
        producers = producers[producers.id > 1]
        producers_to_include = producers.producer.unique()
        del producers

        #Include only the producers with more than 1 podcast on the ranking
        df = df[df.producer.isin(producers_to_include)]
        podcasters.print_current_dataset_dimension(df, 'cut_small_producers')
        return df

    def remove_non_news_producers(df, producers_to_keep = NEWS_PRODUCERS):
        df = df[df.producer.isin(producers_to_keep)]
        podcasters.print_current_dataset_dimension(df, 'remove_non_news_producers')
        return df

    def cut_podcasts_with_insufficient_data(df, min_streak_length=8):
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
        podcasters.print_current_dataset_dimension(df, 'cut_podcasts_with_insufficient_data')
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
        podcasters.print_current_dataset_dimension(df, print_message)
        return df

class episodes:
    def download_podcaster_sample(path):
        df = pd.read_csv(path)
        df['date'] = pd.to_datetime(df['date'])
        print("== PODCASTER DATASET ==\n")
        print(f"Sample includes {df.id.nunique()} podcasters and has shape {df.shape}")
        return df

    def download_all_episodes(path):
        df = pd.read_csv(path)
        df.drop(columns=['Unnamed: 0'], inplace=True)
        print("\n== EPISODES DATASET ==\n")
        print(f'Dataframe size: {df.shape}\nColumns: {list(df.columns)}')
        return df

    def filter_podcasters(episode_data, podcasters):
        print("Start function: filter_podcasters")
        id_lst = list(podcasters.id.unique())
        # The Victor Davis Hanson Show has the wrong id, so I add it to the list
        id_lst.append(1570380458)
        episode_data = episode_data[episode_data.id.isin(id_lst)]
        print(f"New shape is: {episode_data.shape}")

        #Check that all podcasters' episodes are collected
        if episode_data.id.nunique() == len(id_lst)-1:
            print("All podcasts have episodes!")
        else:
            for id_ in id_lst:
                if id_ not in episode_data.id.unique():
                    print(f"Missing id: {id_}")
        return episode_data

    def is_ranked(row, podcasters):
        nr_of_episodes = len(podcasters[
            (podcasters.date==row.date)&
            (podcasters.id==row.id)
        ]) 
        if nr_of_episodes > 0:
            return 1
        return 0

    def select_episodes(episode_data, podcasters, start_date = MIN_TIME, end_date=MAX_TIME):
        episode_data['date'] = pd.to_datetime(episode_data.pubDate)
        episode_data['date'] = episode_data['date'].apply(lambda date: datetime(date.year, date.month, date.day))
        episode_data = episode_data[
            (episode_data.date>=start_date)&
            (episode_data.date<=end_date)&
            (episode_data.episodeType=='full')
        ]
        print(f"After removing episodes based on timeframe and episode type: {episode_data.shape}")

        #Check if the podcast was ranked when the episode was published
        episode_data['isRanked'] = episode_data.apply(lambda row: episodes.is_ranked(row, podcasters), axis=1)
        print(f'Number of ranked episodes: {episode_data.isRanked.sum()}')
        print(f'Number of podcasters: {episode_data[episode_data.isRanked==1].id.nunique()}')

        return episode_data

    def check_episodes_with_transcript(episode_data, podcaster_data, path = PODCAST_TRANSCRIPT_PATH):
        transcripts = pd.read_csv(path)
        transcripts = transcripts.merge(podcaster_data[['podcastName', 'id']], 
                                        on=['podcastName'], how='left')

        transcripts = transcripts[['id', 'hasTranscript']].drop_duplicates()
        episode_data = episode_data.merge(transcripts, on=['id'], how='left')

        return episode_data

    def print_summary_stats(episode_data):

        print("\n==FINAL DATASET==\n")
        episode_data = episode_data[episode_data.isRanked==1]
        print("Dataset shape: {}".format(episode_data.shape))
        print("Number of podcasters: {}".format(episode_data.id.nunique()))

        print("\nWITH TRANSCRIPTS\n")
        episode_data = episode_data[episode_data.hasTranscript==1]
        print("Dataset shape: {}".format(episode_data.shape))
        print("Number of podcasters: {}".format(episode_data.id.nunique()))


def main():

    # Download data
    data = podcasters.download_data(path = DATA_PATH)

    # Cut timeframe
    data = podcasters.cut_timeframe(data)

    # Clean columns
    data = podcasters.clean_columns(data)

    # Cut podcasts that belong to a producer with only 1 podcast in the ranking
    data = podcasters.cut_small_producers(data)

    # Cut producers that do not create news
    data = podcasters.remove_non_news_producers(data)

    # Cut podcasts that have been fewer than 8 days in a row included in the ranking
    data = podcasters.cut_podcasts_with_insufficient_data(data, min_streak_length=8)

    # Cut non-news podcasts
    data = podcasters.cut_non_news_podcasts(data, NON_NEWS_PODCASTS, 'cut_non_news_podcasts (series)')

    # Cut TV shows
    data = podcasters.cut_non_news_podcasts(data, TV_SHOWS_TO_DROP, 'cut_non_news_podcasts (TV)')

    # Save dataset
    data.to_csv(SAVE_PODCASTER_PATH, index=False)

    #Save df with all podcasts indcluded
    data[['podcastName', 'producer']].drop_duplicates().to_csv(SAVE_PATH_PODCASTS_TO_INCLUDE, index=False)

    #Start working on episode sample
    print("\n Starting to process episode data...\n")

    # Get datasets
    podcaster_data = episodes.download_podcaster_sample(SAVE_PODCASTER_PATH)
    episode_data = episodes.download_all_episodes(EPISODE_PATH)
    
    # Filter podcasters
    episode_data = episodes.filter_podcasters(episode_data, podcaster_data)

    #Filter episodes by date
    episode_data = episodes.select_episodes(episode_data, podcaster_data)

    #Check number of episodes with transcript
    episode_data = episodes.check_episodes_with_transcript(episode_data, podcaster_data,path = PODCAST_TRANSCRIPT_PATH)

    #Print summary stats
    episodes.print_summary_stats(episode_data)

    #Save episodes
    episode_data.to_csv(SAVE_EPISODES_PATH)

    

if __name__ == "__main__":
    main()
