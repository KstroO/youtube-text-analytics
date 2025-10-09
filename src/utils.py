from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import config

API_SERVICE_NAME = config.API_SERVICE_NAME
API_VERSION = config.API_VERSION
API_KEY = config.API_KEY

def get_channel_id(user_handle: str) -> str | None:
    """
        Retrieves the channel ID for the given YouTube handle.

        Args:
            user_handle (str): YouTube handle (without @).
        
        Returns:
            str | None: The channel ID or None if not found.
    """
    # trim start if @
    if user_handle.startswith("@"):
        user_handle = user_handle[1:]

    params = dict(
        part = 'id',
        fields = 'items/id',
        forHandle = user_handle)

    try:
        with build(API_SERVICE_NAME, API_VERSION, developerKey = API_KEY) as youtube:
            request = youtube.channels().list(**params) 
            response = request.execute()
            return response['items'][0]['id']

    except (KeyError, IndexError):
        print(f'There is not a channel id associated with the handle {user_handle}')
    except HttpError as e:
        print(f'An error has occurred: {e}')

    return None