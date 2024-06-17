# fetch.py
import os
import asyncio
import aiohttp
import random
from aiohttp import ClientResponseError, ClientConnectorError
from utils import contains_keywords

TIMEOUT = aiohttp.ClientTimeout(total=60)
RATE_LIMIT_DELAY = 1.0  # Initial delay time in seconds for rate limiting
RETRY_LIMIT = 5
GAME_ID = '491931'  # Escape from Tarkov game ID
KEYWORDS = [
    'tarkov', 'escape from tarkov', 'eft', 'battle state', 'bsg', 'raid', 'PMC', 'scav',
    'labs', 'customs', 'factory', 'shoreline', 'interchange', 'reserve', 'woods',
    'flea market', 'tarkov wipe', 'tarkov event', 'tarkov gameplay', 'tarkov highlights'
]

async def exponential_backoff_with_jitter(base_delay=RATE_LIMIT_DELAY, factor=2, jitter=0.1):
    delay = base_delay * factor + random.uniform(0, jitter)
    await asyncio.sleep(delay)

async def get_user_id(oauth_token, username, session, client_id, retries=RETRY_LIMIT):
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {oauth_token}'
    }

    url = 'https://api.twitch.tv/helix/users'
    params = {'login': username}
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 429:
                    delay = int(response.headers.get('Retry-After', RATE_LIMIT_DELAY))
                    print(f"\rRate limited. Waiting for {delay} seconds before retrying...", end='', flush=True)
                    await exponential_backoff_with_jitter(base_delay=delay)
                    continue
                response.raise_for_status()
                data = await response.json()
                if 'data' in data and len(data['data']) > 0:
                    return data['data'][0]['id']
                return None
        except (ClientResponseError, ClientConnectorError) as e:
            print(f"\nAttempt {attempt + 1} - Error fetching user ID: {e}")
            if attempt < retries - 1:
                await exponential_backoff_with_jitter(base_delay=RATE_LIMIT_DELAY * (2 ** attempt))
            else:
                return None
        except asyncio.CancelledError:
            raise

async def get_videos(oauth_token, user_id, session, client_id, retries=RETRY_LIMIT):
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {oauth_token}'
    }
    url = 'https://api.twitch.tv/helix/videos'
    params = {'user_id': user_id, 'first': 100}  # Query parameters for the request
    videos = []
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 429:
                    delay = int(response.headers.get('Retry-After', RATE_LIMIT_DELAY))
                    print(f"\rRate limited. Waiting for {delay} seconds before retrying...", end='', flush=True)
                    await exponential_backoff_with_jitter(base_delay=delay)
                    continue
                response.raise_for_status()
                data = await response.json()
                videos.extend(data.get('data', []))
                break
        except (ClientResponseError, ClientConnectorError) as e:
            print(f"\nAttempt {attempt + 1} - Error fetching videos: {e}")
            if attempt < retries - 1:
                await exponential_backoff_with_jitter(base_delay=RATE_LIMIT_DELAY * (2 ** attempt))
            else:
                break
        except asyncio.CancelledError:
            raise
    return videos

async def get_clips(oauth_token, user_id, session, client_id, retries=RETRY_LIMIT):
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {oauth_token}'
    }
    url = 'https://api.twitch.tv/helix/clips'
    params = {'broadcaster_id': user_id, 'first': 100}  # Query parameters for the request
    clips = []
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 429:
                    delay = int(response.headers.get('Retry-After', RATE_LIMIT_DELAY))
                    print(f"\rRate limited. Waiting for {delay} seconds before retrying...", end='', flush=True)
                    await exponential_backoff_with_jitter(base_delay=delay)
                    continue
                response.raise_for_status()
                data = await response.json()
                clips.extend(data.get('data', []))
                break
        except (ClientResponseError, ClientConnectorError) as e:
            print(f"\nAttempt {attempt + 1} - Error fetching clips: {e}")
            if attempt < retries - 1:
                await exponential_backoff_with_jitter(base_delay=RATE_LIMIT_DELAY * (2 ** attempt))
            else:
                break
        except asyncio.CancelledError:
            raise
    return clips

async def check_user(oauth_token, username, session, client_id):
    user_id = await get_user_id(oauth_token, username, session, client_id)
    if user_id:
        print(f"\rChecking user: {username:<30}", end='', flush=True)
        videos = await get_videos(oauth_token, user_id, session, client_id)
        clips = await get_clips(oauth_token, user_id, session, client_id)

        clip_urls = []
        video_urls = []

        has_escaped_tarkov = False

        if clips:
            for clip in clips:
                game_id = clip.get('game_id')
                if game_id == GAME_ID:
                    clip_urls.append((clip['broadcaster_name'], clip['url']))  # Use broadcaster_name for the channel
                    has_escaped_tarkov = True

        if videos:
            for video in videos:
                title = video['title']
                if contains_keywords(title, KEYWORDS):
                    video_urls.append((username, video['url']))
                    has_escaped_tarkov = True

        return has_escaped_tarkov, clip_urls, video_urls

    return False, [], []
