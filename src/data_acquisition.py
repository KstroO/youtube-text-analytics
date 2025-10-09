import config
import os
import json
import time
import logging
from typing import TextIO
from googleapiclient.discovery import build
from tqdm import tqdm
from googleapiclient.errors import HttpError

# make logging info visible
logging.basicConfig(level=logging.INFO)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

API_SERVICE_NAME = config.API_SERVICE_NAME
API_VERSION = config.API_VERSION
API_KEY = config.API_KEY

def save_channel_playlists(channel_id: str, save_location: str, overwrite: bool = False) -> None:
    """
    Saves all playlists for a given channel ID into a JSON file, writing incrementally.

    Args:
        channel_id (str): ID of the YouTube channel.
        save_location (str): Relative or absolute path to output JSON file.
        overwrite (bool): Whether to overwrite the file if it exists. Defaults to False.
    """

    # Check if file exists and overwrite is not allowed
    if os.path.exists(save_location) and not overwrite:
        logging.error(f"File '{save_location}' already exists. Set overwrite=True to allow overwriting.")
        return
    
    playlists_saved = 0

    # Ensure parent directories exist
    os.makedirs(os.path.dirname(save_location), exist_ok=True)

    try:
        with build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY) as youtube, \
             open(save_location, mode='w', encoding='utf-8') as jsonfile:
            jsonfile.write('[\n')  # Start JSON array
            first_item = True
            next_page = None
            while True:
                # logging.info(f'Fetching page: {next_page}')

                params = dict(
                    part='snippet',
                    fields='nextPageToken, items/id, items/snippet/title, items/snippet/description',
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page
                )

                request = youtube.playlists().list(**params)
                response = request.execute()

                page_items = response.get('items', [])

                for item in page_items:
                    playlist_id = item['id']
                    playlist_title = item['snippet'].get('title', '')
                    playlist_description = item['snippet'].get('description', '')
                    playlist_obj = {
                        'id': playlist_id,
                        'title': playlist_title,
                        'description': playlist_description
                    }
                    if not first_item:
                        jsonfile.write(',\n')
                    json.dump(playlist_obj, jsonfile, ensure_ascii=False)
                    first_item = False
                    playlists_saved += 1

                next_page = response.get('nextPageToken')

                if not next_page:
                    break
            jsonfile.write('\n]\n')  # End JSON array
        # logging.info(f'Saved {playlists_saved} playlists to {save_location}')

    except HttpError as e:
        logging.error(f'An error has occurred {e}')

def get_channel_uploads_playlist(channel_id: str) -> str | None:
    """
        Gets the uploads playlist ID for the given YouTube channel ID.
        Note: that an user resource is identified with the letters "UC", and
        a uploads playlist resource is identified with the letters "UU". If you
        have an user ID = "UC123", the uploads playlist associated to this user
        will be "UU123".

        Args:
            channel_id (str): ID of the YouTube channel.
        Returns:
            str | None: ID of the uploads playlist
    """

    #logging.info(f"Fetching the user ID {channel_id} uploads playlist...")
    try:

        with build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY) as youtube:
            params = dict(
                part="contentDetails",
                id = channel_id,
                fields = "items/contentDetails/relatedPlaylists"
            )

            request = youtube.channels().list(**params)
            response = request.execute()
            items = response.get('items', [])
            if not items:
                logging.info(f"The information for the channel ID {channel_id} returned empty.")
                return None
            
            uploads = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
            
            #logging.info("Uploads playlist fetch finalized successfully.")
            return uploads
    except (KeyError, IndexError, TypeError):
        logging.error(f"There was an error retrieving the resource.")
    except HttpError as e:
        logging.error(f"An error has occurred {e}.")
    return None

def save_playlist_videos(playlist_id: str, save_location: str, overwrite: bool = False) -> None:
    """
        Saves all videos for a given playlist ID into a JSON file.

        Args:
            playlist_id (str): ID of the YouTube playlist.
            save_location (str): Relative or absolute path to output JSON file.
            overwrite (bool): Whether to overwrite the file if it exists. Defaults to False.
    """
    
    # Check if file exists and overwrite is not allowed
    if os.path.exists(save_location) and not overwrite:
        logging.error(f"File '{save_location}' already exists. Set overwrite=True to allow overwriting.")
        return

    next_page = None
    page_count = 0
    videos_count = 0

    videos = []
    logging.info(f"Fetching all videos for playlist ID {playlist_id}...")
    try:
        with build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY) as youtube:
            while True:
                params = dict(
                    part = "contentDetails",
                    fields = "nextPageToken, items/contentDetails/videoId",
                    playlistId = playlist_id,
                    maxResults = 50,
                    pageToken = next_page
                )

                request = youtube.playlistItems().list(**params)
                response = request.execute()

                for item in response.get("items",[]):
                    video_id = item["contentDetails"]["videoId"]
                    videos.append({"videoId": video_id, "done": False, "nextPageToken": None})
                    videos_count += 1

                # logging.info(f"Page {next_page} processed successfully.")

                next_page = response.get("nextPageToken")

                # artificial time delay, avoid triggering rate limit quotas 20ms
                time.sleep(0.02)
                page_count += 1
                if not next_page:
                    break
        logging.info(f"Process finalized successfully: {page_count} pages processed, {videos_count} videos saved.")

        # dump all videos into json file
        try:
            with open(save_location, "w") as f:
                json.dump(videos, f, indent=4, ensure_ascii=False)
            logging.info("Videos updated and saved to file successfully")
        except IOError as e:
            logging.error(f"An error has occurred {e}")
            
    except (KeyError, IndexError, TypeError):
        logging.error(f"There was an error parsing the resource.")
    except HttpError as e:
        logging.error(f"An error has occurred {e}.")
    

def save_video_comments(video_id: str, next_page_token: str, save_location: str, quota_remaining: int) -> tuple[int, str | None, int, int, bool]:
    """
    Saves all comments and replies for a given YouTube video ID into a NDJSON file.

    Args:
        video_id (str): ID of the YouTube video.
        next_page_token (str): Starts the search from this page.
        save_location (str): Path of the ndjson file.
        quota_remaining (int): Quota left for usage.

    Returns:
        (tuple(int, str, int, int, bool)): quota remaining, next page token (if not finished), comments count and replies count, finished bool
    """

    # quota costs from Google Data API V3
    COMMENT_THREADS_QUOTA_COST = 1
    comments_count = 0
    replies_count = 0

    current_quota_usage = 0
    next_page_token = next_page_token

    # logging.info(f"Trying to fetch comments for video {video_id}...")
    try:
        with build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY) as youtube, \
            open(save_location, 'a') as file:

            while current_quota_usage < quota_remaining:
                # artificial delay, avoid rate limits
                time.sleep(0.001) # 1 millisecond
                
                params = dict(
                    part = "snippet, replies",
                    fields = ",".join([
                            "nextPageToken",
                            "items/snippet/totalReplyCount",
                            "items/snippet/topLevelComment/id",
                            "items/snippet/topLevelComment/snippet/channelId",
                            "items/snippet/topLevelComment/snippet/textDisplay",
                            "items/snippet/topLevelComment/snippet/authorDisplayName",
                            "items/snippet/topLevelComment/snippet/authorChannelId/value",
                            "items/snippet/topLevelComment/snippet/likeCount",
                            "items/snippet/topLevelComment/snippet/publishedAt",
                            "items/snippet/topLevelComment/snippet/parentId",
                            "items/replies/comments/id",
                            "items/replies/comments/snippet/channelId",
                            "items/replies/comments/snippet/textDisplay",
                            "items/replies/comments/snippet/authorDisplayName",
                            "items/replies/comments/snippet/authorChannelId/value",
                            "items/replies/comments/snippet/likeCount",
                            "items/replies/comments/snippet/publishedAt",
                            "items/replies/comments/snippet/parentId",
                        ]),
                    textFormat = "plainText",
                    videoId = video_id,
                    maxResults = 100,
                    pageToken = next_page_token
                )

                request = youtube.commentThreads().list(**params)
                response = request.execute()

                # one call, quota usage increase
                current_quota_usage += COMMENT_THREADS_QUOTA_COST

                # for every main comment
                for item in response.get('items', []):
                    reply_count = item['snippet']['totalReplyCount']
                    comment_id = item['snippet']['topLevelComment']['id']
                    
                    # save the top comment
                    file.write(json.dumps({**item['snippet']['topLevelComment'], "totalReplyCount": reply_count, "videoId": video_id}, ensure_ascii=False) + '\n')
                    comments_count += 1

                    # if there are more than 5 replies
                    if reply_count > 5:
                        # make separate call
                        comments_quota_used, current_replies_count = save_comment_replies(comment_id, file, quota_remaining - current_quota_usage)
                        replies_count += current_replies_count
                        current_quota_usage += comments_quota_used

                        # if comments filled quota, we repeat the same page next time
                        if current_quota_usage >= quota_remaining:
                            logging.info(f"Comments for video {video_id} fetched partially: {comments_count} comments saved, {replies_count} replies saved.")
                            return (current_quota_usage, next_page_token, comments_count, replies_count, False)

                    elif 'replies' in item:
                        # for every reply with the comment
                        replies_count += reply_count
                        
                        for reply in item['replies']['comments']:
                            file.write(json.dumps(reply, ensure_ascii=False) + '\n')

                next_page_token = response.get('nextPageToken')               

                if not next_page_token:
                    break

        # if pages remaining
        if current_quota_usage >= quota_remaining and next_page_token != None:
            logging.info(f"Comments for video {video_id} fetched partially: {comments_count} comments saved, {replies_count} replies saved.")
            return (0, next_page_token, comments_count, replies_count, False)
        
        # return quota used
        #logging.info(f"Comments for video {video_id} fetched successfully: {comments_count} comments saved, {replies_count} replies saved.")
        return (current_quota_usage, None, comments_count, replies_count, True)
        
    except (KeyError, IndexError, TypeError):
        logging.error(f"There was an error parsing the resource for video {video_id}")
    except HttpError as e:
        if e.resp.status == 403 and "quotaExceeded" in str(e):
            logging.error(f'Quota limit exceded at video {video_id}')
            if next_page_token != None:
                logging.info(f"Comments for video {video_id} fetched partially: {comments_count} comments saved, {replies_count} replies saved.")
            return (float('inf'), next_page_token, comments_count, replies_count, False)

        logging.error(f"An error has occurred for video {video_id} {e}.")
    except (IOError, OSError) as e:
        logging.error(f"A system-level error has occurred for video {video_id}: {e}")
    return (current_quota_usage, next_page_token, comments_count, replies_count, False)

def save_comment_replies(top_comment_id: str, file: TextIO, quota_remaining: int) -> tuple[int, int]:
    """
        Saves the textDisplay of a YouTube reply to the specified parent ID.

        Args:
            top_comment_id (str): Comment ID of the topLevelComment.
            file (TextIO): file to append the comments.
            quota_remaining (int): Quota left for usage.
        Returns:
            tuple(int, int): Total quota used and replies count processed.
    """

    COMMENTS_QUOTA_COST = 1
    current_quota_usage = 0
    replies_count = 0
    next_page_token = None

    try:
        with build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY) as youtube:

            while current_quota_usage < quota_remaining:
                # artificial delay, avoid rate limits
                time.sleep(0.001) # 1 millisecond

                params = dict(
                    part = "snippet",
                    fields = ",".join(["nextPageToken",
                                       "items/id",
                                       "items/snippet/channelId",
                                       "items/snippet/textDisplay",
                                       "items/snippet/authorDisplayName",
                                       "items/snippet/authorChannelId/value",
                                       "items/snippet/likeCount",
                                       "items/snippet/publishedAt",
                                       "items/snippet/parentId",
                                       ]),
                    parentId = top_comment_id,
                    textFormat = "plainText",
                    maxResults = 100,
                    pageToken = next_page_token
                )

                request = youtube.comments().list(**params)
                response = request.execute()
                current_quota_usage += COMMENTS_QUOTA_COST

                next_page_token = response.get('nextPageToken')

                for item in response.get('items', []):
                    file.write(json.dumps(item, ensure_ascii=False) + '\n')
                    replies_count += 1

                if not next_page_token:
                    break
        return (current_quota_usage, replies_count)

    except (KeyError, IndexError, TypeError):
        logging.error(f"There was an error parsing the resource for comment {top_comment_id}")
    except HttpError as e:
        if e.resp.status == 403 and "quotaExceeded" in str(e):
            logging.error(f'Quota limit exceeded in replies for top comment {top_comment_id}')
            return (float('inf'), replies_count)
        logging.error(f"An error has occurred for top comment {top_comment_id}, {e}.")
    except (OSError, IOError) as e:
        logging.error(f"A system-level error has occurred for top  comment {top_comment_id}: {e}")
            
    return (current_quota_usage, replies_count)

def save_all_videos_comments(videos_location: str, comments_location: str, debugging: bool, log_every_count: int = 1) -> None:
    """
    Reads the videos from a file and fetches the comments for them.
    If a video is finished it is marked as done.
    If a video is left unfinished the nextPageToken is saved, for later processing.

    Args:
        videos_location (str): Location to the JSON file containing the videos.
        comments_location (str): Location to the NDJSON file containing the comments.
        debugging (bool): Makes a test run, with only 50 units.
        log_every_count (int): The program will report every count of videos.
    """

    DAILY_QUOTA = 10 if debugging else 9900
    current_quota_usage = 0
    skiped_videos = 0
    current_videos_count = 0
    current_comments_count = 0
    current_replies_count = 0
    start_time = time.time()
    if log_every_count <= 0:
        log_every_count = 1 # fallback

    #load all videos
    logging.info("Comments fetch initialized...")
    try:
        with open(videos_location, 'r') as file:
            videos = json.load(file)
        logging.info(f"Videos list from {videos_location} loaded successfully.")

        for video in videos:
            if video['done'] == False:
                video_id = video['videoId']
                next_page_token = video['nextPageToken']

                if next_page_token != None:
                    logging.info(f"Resuming comments fetch for video {video_id} from page {next_page_token}")

                quota_video_used, next_page_token, video_comments_count, video_replies_count, done = save_video_comments(video_id, next_page_token, comments_location, DAILY_QUOTA - current_quota_usage)

                current_comments_count += video_comments_count
                current_replies_count += video_replies_count
                current_quota_usage += quota_video_used
                current_videos_count += 1

                # save page
                video['nextPageToken'] = next_page_token

                # quota_met
                if current_quota_usage >= DAILY_QUOTA:
                    logging.info(f"Daily quota limit reached, {current_videos_count} videos saved.")
                    break

                # Report every single video
                if log_every_count == 1:
                    elapsed = time.time() - start_time
                    logging.info(f"Video {video_id} processed. Finished: {done}. Comments: {video_comments_count}, Replies: {video_replies_count}")

                # Report every count of videos
                elif current_videos_count % log_every_count == 0:
                    elapsed = time.time() - start_time
                    logging.info(f"{current_videos_count} videos processed ({elapsed:.2f}s), current comments: {current_comments_count}, current replies: {current_replies_count}")

                # video done
                if done:
                    video['done'] = True
                
                # save progress after each video is processed
                try:
                    with open(videos_location, 'w') as file:
                        json.dump(videos, file, indent=4)
                except IOError as e:
                    logging.error(f"Failed to save progress for video {video_id}: {e}")
            else:
                skiped_videos += 1

        end_time = time.time() - start_time
        logging.info(f"Success. Skipped: {skiped_videos}, Processed: {current_videos_count}, Comments: {current_comments_count}, Replies: {current_replies_count}. ({end_time:.2f}s)")
    except json.JSONDecodeError:
        logging.error("Error: File is not valid JSON")
    except KeyError:
        logging.error("Error parsing JSON")
    except (OSError, IOError) as e:
        logging.error(f"A system-level error has occurred {e}")
    finally:
        #save videos list
        try:
            with open(videos_location, 'w') as file:
                json.dump(videos, file, indent=4)
            logging.info(f"Videos saved successfully, videos count: {current_videos_count}, comments & replies: {current_comments_count + current_replies_count}")
        except IOError as e:
            logging.error(f"Failed to save progress to file: {e}")

def get_videos_progress(videos_location: str) -> dict[str: int] | None:
    """
    Returns the current progress as dict for all videos in the current path:
    {done: int, half_way: int, undone: int}

    Args:
        videos_location (str): Path of the JSON videos.
    
    Returns:
        dict(str, int): Progress for videos.
    """
    try:
        with open(videos_location, 'r') as file:
            videos = json.load(file)

        result = {'done': 0, 'half_way': 0, 'undone': 0}
        for video in videos:
            if video['done']:
                result['done'] += 1
            elif video['nextPageToken'] != None:
                result['half_way'] += 1
            else:
                result['undone'] += 1
        return result
    except FileNotFoundError:
        logging.error('Error: file not found.')
    except json.JSONDecodeError:
        logging.error("Error: file is not valid JSON.")
    except KeyError:
        logging.error("Error: the given file has the incorrect format.")
    return None

def add_video(videos_location: str, video_id: str) -> None:
    """
    Adds a video to the current video list. The current function doesn't check
    if the inserted video belongs to the channel.

    Args:
        videos_location (str): path of the JSON video list.
        video_id (str): YouTube video ID.
    """
    try:
        video_id = str.strip(video_id)
        new_video = {'videoId': video_id, 'done': False, 'nextPageToken': None}
        with open(videos_location, 'r') as file:
            videos = json.load(file)

        # check existence
        for video in videos:
            if video['videoId'] == video_id:
                logging.info(f'Video already in list. Skipping videoId {video_id}')
                return

        videos.append(new_video)

        with open(videos_location, 'w') as file:
            json.dump(videos, file, indent= 4)
        logging.info(f'Success: Video {video_id} added.')
    except FileNotFoundError:
        logging.error('Error: file not found.')
    except json.JSONDecodeError:
        logging.error("Error: file is not valid JSON.")
    except KeyError:
        logging.error("Error: the given file has the incorrect format.")
