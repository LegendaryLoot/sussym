# auth.py
import aiohttp
from aiohttp import ClientResponseError, ClientConnectorError

TIMEOUT = aiohttp.ClientTimeout(total=60)
RATE_LIMIT_DELAY = 1.0  # Initial delay time in seconds for rate limiting

async def get_twitch_oauth_token(client_id, client_secret):
    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        try:
            async with session.post(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                return data['access_token']
        except ClientResponseError as e:
            print(f"Error getting OAuth token: {e.status} - {e.message}")
        except ClientConnectorError as e:
            print(f"Connection error: {e}")
