# main.py
import os
import asyncio
import pandas as pd
from process import check_users_for_tarkov_streams
from utils import read_csv
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Ensure environment variables are loaded
CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')

if not CLIENT_ID or not CLIENT_SECRET:
    print("Error: TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must be set in the .env file.")
    exit(1)
else:
    print(f"CLIENT_ID: {CLIENT_ID}")
    print(f"CLIENT_SECRET: {CLIENT_SECRET}")

async def main():
    matched_df = read_csv('resources/FoundNamesOnTwitch.csv')
    if matched_df is None:
        return

    all_clip_urls, all_video_urls = await check_users_for_tarkov_streams(matched_df, CLIENT_ID, CLIENT_SECRET)

    # Write all clip and video URLs to a CSV file
    if all_clip_urls or all_video_urls:
        clip_urls_df = pd.DataFrame(all_clip_urls + all_video_urls, columns=['Streamer', 'URL'])
        output_clip_urls_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results/ClipAndVideoURLs.csv')
        clip_urls_df.to_csv(output_clip_urls_csv_path, index=False)
        print(f"Total clips and videos URLs: {len(clip_urls_df)}")
    else:
        print("No clips or videos found.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Process interrupted")
    except asyncio.CancelledError:
        print("Task cancelled")
