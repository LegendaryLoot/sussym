# process.py
import os
import asyncio
import pandas as pd
import aiohttp
from tqdm.asyncio import tqdm
from fetch import check_user
from auth import get_twitch_oauth_token

BATCH_SIZE = 1
BATCH_DELAY = 0  # Seconds to wait between batches
TIMEOUT = aiohttp.ClientTimeout(total=60)
CONCURRENT_REQUESTS = 100  # Limit the number of concurrent requests

async def check_users_for_tarkov_streams(matched_df, client_id, client_secret):
   
    oauth_token = await get_twitch_oauth_token(client_id, client_secret)
    if oauth_token is None:
        return pd.DataFrame(), []

    all_clip_urls = []
    all_video_urls = []
    processed_usernames = set()  # Set to track processed usernames
    first_batch = True
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)  # Limit concurrent requests

    async def check_user_with_semaphore(oauth_token, username, session, client_id):
        async with semaphore:
            return await check_user(oauth_token, username, session, client_id)

    for i in range(0, len(matched_df), BATCH_SIZE):
        batch = matched_df.iloc[i:i + BATCH_SIZE]
        async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
            tasks = []
            for index, row in batch.iterrows():
                username = row['FoundName']
                if username not in processed_usernames:
                    tasks.append(check_user_with_semaphore(oauth_token, username, session, client_id))
                    processed_usernames.add(username)  # Track the processed username

            batch_results = []
            for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Checking users batch {i // BATCH_SIZE + 1}"):
                result, clip_urls, video_urls = await task
                if result:
                    batch_results.append(row)  # Append the row to batch_results
                all_clip_urls.extend(clip_urls)
                all_video_urls.extend(video_urls)

        # Convert batch results to DataFrame and write to CSV
        if batch_results:
            batch_df = pd.DataFrame(batch_results).drop_duplicates()
            output_tarkov_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results/HasPlayedTarkov.csv')
            if first_batch:
                batch_df.to_csv(output_tarkov_csv_path, index=False, mode='w')
                first_batch = False
            else:
                batch_df.to_csv(output_tarkov_csv_path, index=False, mode='a', header=False)
            print(f"Batch {i // BATCH_SIZE + 1} written to CSV with {len(batch_df)} records")

        # Wait for BATCH_DELAY seconds before processing the next batch
        if i + BATCH_SIZE < len(matched_df):
            print(f"Waiting for {BATCH_DELAY} seconds before processing the next batch...")
            await asyncio.sleep(BATCH_DELAY)

    return all_clip_urls, all_video_urls
