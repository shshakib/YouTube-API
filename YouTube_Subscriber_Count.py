import csv
import time
import os
import random
import pickle
import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from langdetect import detect
import re
import langid
from polyglot.detect import Detector

#Getting API KEY from environment variables
API_KEY = os.environ.get('YOUTUBE_API_KEY')
if not API_KEY:
    raise ValueError("Please set the API_KEY environment variable.")

YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
MAX_RETRIES = 3 #Max number if waite
SLEEP_DURATION = 5  #Waite duration for retries
MAX_DELAY = 320 #Max wait time for retries
COUNTRIES = ['US', 'CA']
CHUNK_SIZE = 40000 #Rows to process at once

youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY) #Initialize YouTube API
fetched_channel_data = {} #Keep subscriber counts


# def save_channel_data():
#     with open('channel_data.pkl', 'wb') as f:
#         pickle.dump(fetched_channel_data, f)


# def load_channel_data():
#     global fetched_channel_data
#     try:
#         with open('channel_data.pkl', 'rb') as f:
#             fetched_channel_data = pickle.load(f)
#     except (FileNotFoundError, pickle.UnpicklingError):
#         fetched_channel_data = {}

def save_channel_data():
    with open('channel_data.json', 'w') as f:
        json.dump(fetched_channel_data, f)

def load_channel_data():
    global fetched_channel_data
    try:
        with open('channel_data.json', 'r') as f:
            fetched_channel_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        fetched_channel_data = {}


def get_subscriber_count(channel_id):
    if channel_id in fetched_channel_data:
        return fetched_channel_data[channel_id]

    delay = SLEEP_DURATION

    for _ in range(MAX_RETRIES):
        try:
            response = youtube.channels().list(part='statistics', id=channel_id).execute()

            if not response.get('items'):
                # print(f"Channel ID {channel_id} not found.")
                fetched_channel_data[channel_id] = "Channel Not Found"
                return "Channel Not Found"
                
            subscriber_count = response['items'][0]['statistics']['subscriberCount']
            fetched_channel_data[channel_id] = subscriber_count
            return subscriber_count

        except HttpError as e:
            if e.resp.status in [403, 429]:  #Quota error and Too Many Requests
                sleep_time = delay + (random.randint(0, 1000) / 1000.0)
                print(f"Encountered {e.resp.status} error for channel ID: {channel_id}. Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                delay *= 2
                
                if delay > MAX_DELAY:
                    delay = MAX_DELAY
            else:
                print(f"An HTTP error {e.resp.status} occurred for channel ID: {channel_id}")
                fetched_channel_data[channel_id] = f"Error: {e.resp.status}" 
                return "Error"

    print(f"Max retries reached for channel ID: {channel_id}")
    fetched_channel_data[channel_id] = "Error"
    return "Error"



# def detect_language(title):
#     try:
#         return detect(title)
#     except:
#         return "unknown"

def detect_language(title):
    try:
        return Detector(title, quiet=True).languages[0].code
    except:
        return "unknown"

# def detect_language(title):
#     try:
#         lang, _ = langid.classify(title)
#         return lang
#     except:
#         return "unknown"

    
def get_country_from_filename(filename):
    base_name = os.path.basename(filename)
    for country in COUNTRIES:
        if base_name.startswith(country):
            return country
    return "unknown"

    
def write_to_csv(filename, rows, fieldnames):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def process_csvs_in_chunks(input_files):
    combined_rows_file1 = []
    combined_rows_file2 = []

    seen_video_ids = set()
    seen_videoid_trendingdate = set()

    total_rows_processed = 0

    for input_csv in input_files:
        country = get_country_from_filename(input_csv)

        try:
            with open(input_csv, 'rb') as f_in_binary:
                cleaned_lines = (line.replace(b'\x00', b'').decode('utf-8') for line in f_in_binary) #Remove NUL
                reader = csv.DictReader(cleaned_lines)
                
                chunk = []
                for row in reader:
                    row['country'] = country
                    chunk.append(row)
                    
                    if len(chunk) == CHUNK_SIZE:
                        start_time = time.time()
                        for row_chunk in chunk:
                            processed_row1, processed_row2 = process_row(row_chunk, seen_video_ids, seen_videoid_trendingdate, country)
                            
                            if processed_row1:
                                combined_rows_file1.append(processed_row1)
                            if processed_row2:
                                combined_rows_file2.append(processed_row2)
                        chunk = []
                        
                        end_time = time.time()
                        total_rows_processed += CHUNK_SIZE
                        print(f"Processed {CHUNK_SIZE} rows ({total_rows_processed}) in {end_time - start_time:.2f} seconds.")
                        save_channel_data()
                
                for row_chunk in chunk:
                    processed_row1, processed_row2 = process_row(row_chunk, seen_video_ids, seen_videoid_trendingdate, country)

                    if processed_row1:
                        combined_rows_file1.append(processed_row1)
                    if processed_row2:
                        combined_rows_file2.append(processed_row2)

        except (FileNotFoundError, UnicodeDecodeError) as e:
            print(f"Error processing file {input_csv}. Error: {e}")

    output_csv1 = 'combined_no_duplicate_video_id.csv'
    output_csv2 = 'combined_no_duplicate_video_id_and_date.csv'
    fieldnames = reader.fieldnames + ['subscribers', 'country', 'language', 'clean_title'] 

    write_to_csv(output_csv1, combined_rows_file1, fieldnames)
    write_to_csv(output_csv2, combined_rows_file2, fieldnames)

def clean_title(title):
    """Clean the title for processing."""
    title = re.sub(r"http\S+|www\S+|https\S+", '', title, flags=re.MULTILINE) #Remove URL
    title = title.encode('ascii', 'ignore').decode('ascii') #Remove emojis and special characters
    return title

def process_row(row, seen_video_ids, seen_videoid_trendingdate, country):
    video_id = row['video_id']
    trending_date = row['trending_date']
    channel_id = row['channelId']
    subscriber_count = get_subscriber_count(channel_id)
    #subscriber_count = 1
    #row['subscribers'] = subscriber_count if subscriber_count != "Error" else "Error"
    row['subscribers'] = subscriber_count

    cleaned_title = clean_title(row['title'])
    language = detect_language(cleaned_title)
    row['clean_title'] = cleaned_title
    row['language'] = language

    processed_row1 = None
    processed_row2 = None

    #Unique video_id
    if video_id not in seen_video_ids:
        seen_video_ids.add(video_id)
        processed_row1 = row.copy() 

    #Unique combination of video_id and trending_date
    combo_key = f"{video_id}_{trending_date}"
    if combo_key not in seen_videoid_trendingdate:
        seen_videoid_trendingdate.add(combo_key)
        processed_row2 = row.copy()

    return processed_row1, processed_row2

if __name__ == "__main__":
    input_files = [
            './Data/US_youtube_trending_data.csv',
            './Data/CA_youtube_trending_data.csv']
    
    load_channel_data()
    process_csvs_in_chunks(input_files)
    save_channel_data()
