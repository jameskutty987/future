import os
import requests
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta
import sqlite3
from dotenv import load_dotenv
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, filename='script.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv('/home/DropeMusics/myproject/.env')

# Spotify API credentials
SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
SPOTIPY_REDIRECT_URI = os.getenv('SPOTIPY_REDIRECT_URI', 'https://dropemusics.pythonanywhere.com/callback')
SPOTIPY_REFRESH_TOKEN = os.getenv('SPOTIPY_REFRESH_TOKEN')

# Endpoint to get new access token
TOKEN_URL = 'https://accounts.spotify.com/api/token'

# Keep track of the expiration time
expiry_time = datetime.utcnow()

def refresh_access_token():
    """Refresh the access token for Spotify API."""
    global expiry_time
    if datetime.utcnow() >= expiry_time:
        response = requests.post(TOKEN_URL, data={
            'grant_type': 'refresh_token',
            'refresh_token': SPOTIPY_REFRESH_TOKEN,
            'client_id': SPOTIPY_CLIENT_ID,
            'client_secret': SPOTIPY_CLIENT_SECRET
        })
        response_data = response.json()
        if response.status_code == 200:
            access_token = response_data['access_token']
            expires_in = response_data['expires_in']
            expiry_time = datetime.utcnow() + timedelta(seconds=expires_in)
            logging.info("Access token refreshed successfully.")
            return access_token
        else:
            logging.error(f"Error refreshing token: {response_data}")
            raise Exception(f"Error refreshing token: {response_data}")
    return None

def get_spotify_client():
    """Get a Spotify client instance."""
    access_token = refresh_access_token()
    if access_token:
        return spotipy.Spotify(auth=access_token)
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=SPOTIPY_CLIENT_ID,
        client_secret=SPOTIPY_CLIENT_SECRET,
        redirect_uri=SPOTIPY_REDIRECT_URI,
        scope='playlist-read-private playlist-modify-public playlist-modify-private playlist-read-collaborative user-library-read',
        cache_path='.spotify_cache'
    ))

sp = get_spotify_client()

DATABASE = '/home/DropeMusics/myproject/music_data.db'

def get_db_connection():
    """Establish a connection to the database."""
    try:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logging.error(f"Exception in get_db_connection: {e}")
        raise Exception(f"Failed to connect to the database: {e}")

def fetch_tracks_from_artist_last_week(artist_id, limit=15):
    """Fetch tracks released by a given artist in the last week."""
    tracks = []
    one_week_ago = datetime.now() - timedelta(days=7)

    try:
        results = sp.artist_albums(artist_id, album_type='album,single', limit=50)
        albums = results['items']

        while results['next']:
            results = sp.next(results)
            albums.extend(results['items'])

        for album in albums:
            album_release_date = album.get('release_date')
            if album_release_date:
                release_date = datetime.strptime(album_release_date, '%Y-%m-%d')
                if release_date < one_week_ago:
                    continue

                results = sp.album_tracks(album['id'], limit=50)
                album_tracks = results['items']

                while results['next']:
                    results = sp.next(results)
                    album_tracks.extend(results['items'])

                for track in album_tracks:
                    track['release_date'] = album_release_date
                tracks.extend(album_tracks)

        tracks = [track for track in tracks if datetime.strptime(track['release_date'], '%Y-%m-%d') >= one_week_ago]
        tracks = tracks[:limit]

    except spotipy.exceptions.SpotifyException as e:
        logging.error(f"Error fetching tracks for artist ID {artist_id}: {e}")
        if e.http_status == 403:
            logging.error("Check settings on developer.spotify.com/dashboard, the user may not be registered.")
    except Exception as e:
        logging.error(f"Unexpected error fetching tracks for artist ID {artist_id}: {e}")

    return tracks

def get_track_details(track_item):
    """Extract and classify track details."""
    track = track_item
    release_date = track.get('release_date')
    if release_date:
        try:
            release_date = datetime.strptime(release_date, '%Y-%m-%d')
        except ValueError:
            release_date = None

    one_week_ago = datetime.now() - timedelta(days=7)
    if release_date and release_date >= one_week_ago:
        artist_id = track.get('artists', [{}])[0].get('id')
        genre = classify_track_by_artist(artist_id) if artist_id else 'Unknown'
        return {
            'id': track.get('id'),
            'genre': genre,
            'name': track.get('name'),
            'artist': track.get('artists', [{}])[0].get('name'),
            'release_date': release_date.strftime('%Y-%m-%d') if release_date else 'Unknown'
        }
    return None

def classify_track_by_artist(artist_id):
    """Classify track by artist's genre."""
    if not artist_id:
        return 'Unknown'
    try:
        artist = sp.artist(artist_id)
        genres = artist.get('genres', [])
        return genres[0] if genres else 'Unknown'
    except Exception as e:
        logging.error(f"Error fetching artist information: {e}")
        return 'Unknown'

def add_tracks_to_playlist(playlist_id, tracks, limit=70):
    """Add tracks to the specified playlist."""
    if not tracks:
        return 0

    track_ids = [track['id'] for track in tracks if track['id']]
    added_count = 0

    try:
        current_playlist_track_count = 0
        playlist_info = sp.playlist(playlist_id)
        current_playlist_track_count = playlist_info['tracks']['total']

        while track_ids:
            batch = track_ids[:limit]
            track_ids = track_ids[limit:]

            if current_playlist_track_count + len(batch) > limit:
                batch = batch[:limit - current_playlist_track_count]

            if batch:
                sp.playlist_add_items(playlist_id, batch)
                logging.info(f"Added {len(batch)} tracks to playlist ID {playlist_id}.")
                added_count += len(batch)
                current_playlist_track_count += len(batch)

    except Exception as e:
        logging.error(f"Error adding tracks to playlist ID {playlist_id}: {e}")

    return added_count

def run_only_on_sunday(func):
    """Decorator to run the function only on Sundays."""
    def wrapper(*args, **kwargs):
        if datetime.today().weekday() == 6:  # 6 means Sunday
            return func(*args, **kwargs)
        else:
            logging.info(f"Not Sunday. Skipping the job. Today is {datetime.today().strftime('%A')}.")
            return None
    return wrapper

@run_only_on_sunday
def job():
    """Main job function to process music data."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM genre")
        genres = cursor.fetchall()

        cursor.execute("SELECT * FROM unknown_playlist")
        unknown_playlists = cursor.fetchall()

        if not genres:
            logging.info("No genres found in database.")
            return

        total_tracks_fetched = 0
        total_tracks_processed = 0
        total_tracks_added = 0
        tracks_added_to_unknown_playlist = 0
        genre_add_count = {}

        genre_to_playlist = {row['genre'].strip().lower(): row['playlist_id'].strip() for row in genres if row['genre'] and row['playlist_id']}
        unknown_playlists_ids = [row['playlist_id'] for row in unknown_playlists if row['playlist_id']]

        cursor.execute("SELECT * FROM artist")
        artists = cursor.fetchall()

        # Process artists in batches
        batch_size = 50
        for i in range(0, len(artists), batch_size):
            batch_artists = artists[i:i + batch_size]
            for artist in batch_artists:
                artist_id = artist['artist_id'].strip()
                if not artist_id:
                    continue

                tracks = fetch_tracks_from_artist_last_week(artist_id)
                total_tracks_fetched += len(tracks)
                processed_tracks = []

                for track in tracks:
                    track_details = get_track_details(track)
                    if track_details:
                        processed_tracks.append(track_details)
                        total_tracks_processed += 1

                if processed_tracks:
                    for track in processed_tracks:
                        genre = track['genre'].lower()
                        playlist_id = genre_to_playlist.get(genre)
                        if playlist_id:
                            count_added = add_tracks_to_playlist(playlist_id, [track])
                            total_tracks_added += count_added
                            if genre not in genre_add_count:
                                genre_add_count[genre] = 0
                            genre_add_count[genre] += count_added
                        else:
                            for playlist_id in unknown_playlists_ids:
                                count_added = add_tracks_to_playlist(playlist_id, [track])
                                tracks_added_to_unknown_playlist += count_added

        summary_message = (
            f"Tracks fetched: {total_tracks_fetched}, "
            f"Tracks processed: {total_tracks_processed}, "
            f"Tracks added: {total_tracks_added}, "
            f"Tracks added to unknown playlists: {tracks_added_to_unknown_playlist}"
        )

        logging.info(summary_message)
        print(summary_message)

    except Exception as e:
        logging.error(f"Error running the job: {e}")

# Schedule the job to run weekly (on Sunday)
if __name__ == '__main__':
    job()

