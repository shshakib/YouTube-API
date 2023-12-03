# YouTube API Subscriber Count Fetcher
## Description
This Python script is designed to enrich YouTube trending dataset information by fetching the subscriber counts for channels listed in the dataset. It connects to the YouTube API, retrieves subscriber counts for each channel ID, and integrates this data into the YouTube trending dataset. The script is equipped with functions to handle large datasets, manage API requests, and ensure efficient data processing.

Dataset link: https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset

## Features
    YouTube API Integration: Connects to YouTube's Data API to fetch subscriber counts.
    Data Enrichment: Adds subscriber count data to the YouTube trending dataset.
    Efficient Data Handling: Processes large datasets in chunks, ensuring efficient memory usage.
    Error Handling and Retries: Implements retry logic for handling API rate limits and other HTTP errors.
    Data Persistence: Saves and loads processed data using JSON for continuity between script runs.
    Language Detection: Determines the language of video titles using the Polyglot library.

## Requirements
    Python 3.x
    googleapiclient
    langdetect
    polyglot

## Installation
    Clone the repository or download the script.
    Install required Python libraries: pip install google-api-python-client langdetect polyglot.
    Set the YOUTUBE_API_KEY environment variable with your YouTube Data API key.

## Usage
    Place the YouTube trending dataset CSV files in the Data directory.
    Run the script: python youtube_subscriber_fetcher.py.
    The enriched dataset will be saved as new CSV files.

## Configuration
    API Key: Set your YouTube Data API key in the YOUTUBE_API_KEY environment variable.
    Chunk Size: Modify CHUNK_SIZE to control the number of rows processed at once.
    Retry Settings: Adjust MAX_RETRIES, SLEEP_DURATION, and MAX_DELAY to handle API rate limits.

## Functions
    save_channel_data(): Saves the fetched subscriber data to a JSON file.
    load_channel_data(): Loads subscriber data from a JSON file.
    get_subscriber_count(): Fetches the subscriber count for a given channel ID.
    detect_language(): Detects the language of a given text.
    get_country_from_filename(): Extracts country code from the filename.
    write_to_csv(): Writes processed data to a CSV file.
    process_csvs_in_chunks(): Processes input CSV files in chunks.
    clean_title(): Cleans and prepares video titles for language detection.
    process_row(): Processes each row of the dataset, enriches with subscriber count and other data.
