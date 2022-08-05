# Packages for using the browser
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
# Packages for data manipulation
import re
import pandas as pd
# Package for downloading transcripts from YouTube
from youtube_transcript_api import YouTubeTranscriptApi

VIDEO_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//Transcripts//youtube_videos.csv'
TRANSCRIPT_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//Transcripts//youtube_transcripts.csv'

def get_channel_data(url):
    """
    Collect Youtube episodes using selenium
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")  # Use full screen
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
    driver.get(url)
    time.sleep(3)
    driver.find_element(by=By.XPATH, value='//*[@id="yDmH0d"]/c-wiz/div/div/div/div[2]/div[1]/div[3]/div[1]/form[1]/div/div/button/span').click()
    time.sleep(3)
    
    print("Start scrolling down!")
    for i in range(150):
        driver.execute_script("window.scrollTo(0, 140000);")
        time.sleep(1)
        
    print("Start collecting data from the webpage")
    result = []
    for element in driver.find_elements(by=By.ID, value="video-title"):
        val = element.get_attribute("aria-label")
        viewers=val.split(' ')[-2]
        duration = val.replace(viewers, 'ago').split('ago')[1].strip()
        
        result.append([
            element.get_attribute('href'),
            element.get_attribute("text"),
            viewers,
            duration,
            url
        ])
    driver.close()
    return result


def get_transcripts(data):
    """
    Uses the YouTube transcript api to get transcripts for all the podcast episodes.

    """
    #Get only podcast episodes
    data = filter_episodes(data)

    df = pd.DataFrame()
    old_len = 0
    for row in data:
        #Sometimes there is no transcript because the video doesn't have subtitles
        try:
            temp = YouTubeTranscriptApi.get_transcript(row[1]) #Input: episode id --> Returns a dictionary
            temp = pd.DataFrame(temp)
            temp['url'] = row[0]
            df = pd.concat([df, temp])
            if old_len < len(df):
                print(f"Added: {row[1]}")
                old_len=len(df)
            del temp
        except:
            temp = pd.DataFrame([["not found", 'not found', 'not found', row[1]]], columns=['text','start','duration','url'])
            df = pd.concat([df, temp])
            old_len=len(df)
            print("\n#######\nFailed to collect: {}\n#######\n".format(row[0]))
            del temp
    return df



def filter_episodes(data):
    """
    Returns a list of lists with only the episodes, which transcripts I need

    NB!!!!
    Current filter works only for Ben Shapiro's channel

    """
    return_lst = []
    for i, row in data.iterrows():

        #Remove shorts
        if re.search('shorts', row.url): continue

        #I need to add conditions to each channel
        if row.channel_url== "https://www.youtube.com/c/BenShapiro/videos":
            txt = row.episodeName.lower()
            if re.search('ep.', txt):
                id_ = row.url.split('v=')[1]
                return_lst.append([row.url, id_])
    return return_lst

if __name__ == "__main__":
    print("Starting programme!")

    #Create a list of channel urls
    channel_urls = ["https://www.youtube.com/c/BenShapiro/videos"]
    data = []
    for url in channel_urls:
        data += get_channel_data(url)
    print("Channel data collected")

    data = pd.DataFrame(data, columns=['url', 'episodeName', 'viewers', 'duration', 'channel_url'])
    data.to_csv(VIDEO_PATH, index=False)
    data = pd.read_csv(VIDEO_PATH)
    print("Get transcripts for episodes")
    transcripts = get_transcripts(data)
    print("Transcripts collected")
    transcripts.to_csv(TRANSCRIPT_PATH, index=False)