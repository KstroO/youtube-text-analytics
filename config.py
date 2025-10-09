from dotenv import load_dotenv
import os

############################
# YouTube handle of the desired channel, it appears with a @<username> in the profile page.
channel_handle = "kurzgesagt" # without @

############################
# API
load_dotenv()
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
API_KEY = os.getenv('api_key')

if not API_KEY:
    raise ValueError("API_KEY not found.Please create a .env file in the project root.")

# Project root directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))