# Packages for using the browser
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
#webscraping
from bs4 import BeautifulSoup

# Packages for data manipulation
import re
import pandas as pd

URL_LIST = ['https://www.npr.org/podcasts/510318/up-first']
TRANSCRIPT_URL = "https://www.npr.org/transcripts/"
CHANNEL_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//Transcripts//NPR_podcasts.csv'
TRANSCRIPT_PATH = 'C://Users//Patrick//OneDrive - City, University of London//PhD//Research//Podcast paper//Scrapes//Transcripts//NPR_transcripts.csv'

def launch_driver(url):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")  # Use full screen
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
    driver.get(url)
    time.sleep(3)
    #Press agree continue button
    driver.find_element(by=By.XPATH, value="/html/body/main/section/div/button").click()
    return driver

def get_channel_data(url, scroll=False):
    
    """
    Collect Youtube episodes using selenium and beautiful soup
    Input: the channel's url
    Returns list of list with the values: 
        - url = channel url 
        - episode_name = Name of the episode
        - date = publishing date 
        - episode_url = url for the episode 
        - episode_id = episode id 
        - hasTransrcript = 1 if the episode has a transcript else 0
    """
    
    #Launches the driver and accesses the npr webpage
    driver = launch_driver(url)
    
    #Scroll down and click next episodes
    if scroll:
        time.sleep(5)
        print("Start scrolling down the page")
        continue_scrolling = True
        while continue_scrolling: #When the next button dissapears I end the scrolling
            try:
                driver.execute_script("window.scrollTo(0, 1400000);")
                driver.find_element(by=By.XPATH, value="/html/body/main/div[2]/section/section[4]/div[2]/div[2]/button").click()
                time.sleep(1)
            except:
                continue_scrolling = False
    
    #Collect episode data
    print("Start collecting data from the webpage")
    result = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    for tag in soup.find_all('div', class_='item-info'):
        episode_name = tag.find('h2', class_='title').text
        episode_url = tag.find('h2', class_='title').find('a')['href']
        date = episode_url.split('/')[3:6]
        date = '-'.join(date)
        episode_id = episode_url.split('/')[6]
        
        try:
            tag.find('li', class_="audio-tool audio-tool-transcript").find('a')['href']
            hasTransrcript=1
        except:
            hasTransrcript=0
        print(f"Title: {episode_name}\nEpisode url: {episode_url}\nDate: {date}\nID: {episode_id}\nTanscript: {hasTransrcript}\n")
        result.append([url, episode_name, date, episode_url, episode_id, hasTransrcript])
  
    driver.close()

    return result


def get_transcript(episode):
    # Create transcript for url
    url = TRANSCRIPT_URL + episode[-2]
    driver = launch_driver(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    result = [tag.text for tag in soup.find('div', class_="transcript storytext").find_all('p')]
    result = " || ".join(result)
    print(result)

    return 0




def main():

    # Get channel data
    channel_episodes = []
    for url in URL_LIST:
        channel_episodes += get_channel_data(url)

    # Get transcripts
    transcripts = []
    for episode in channel_episodes:
        if episode[-1] == 1:
            print(episode)

            transcript = get_transcript(episode)
            transcripts.append([episode[-2], transcript]) #Episdoe id and transcript

    #Store channel data and transcripts
    df = pd.DataFrame(channel_episodes, columns=['podcast_url', 'episodeName', 'date', 'episode_url', 'episode_id', 'hasTranscript'])
    df.to_csv(CHANNEL_PATH)
    del df
    df = pd.DataFrame(transcripts, columns=['episode_id', 'transcript'])
    df.to_csv(TRANSCRIPT_PATH)


if __name__ == "__main__":
    main()