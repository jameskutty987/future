from flask import Flask
import logging
import time
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from collections import Counter
from spotipy.exceptions import SpotifyException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Spotify setup
def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id='YOUR_CLIENT_ID',
        client_secret='YOUR_CLIENT_SECRET',
        redirect_uri='YOUR_REDIRECT_URI',
        scope='playlist-read-private playlist-read-collaborative'
    ))

sp = get_spotify_client()

source_playlist_id = 'YOUR_PLAYLIST_ID'

def handle_rate_limits(e):
    if e.http_status == 429:
        retry_after = int(e.headers.get('Retry-After', 1))
        logger.info(f"Rate limit hit. Retrying after {retry_after} seconds.")
        time.sleep(retry_after)
        return True
    return False

def get_track_details(track):
    return {
        'name': track['name'],
        'id': track['id'],
        'album': track['album']['name'],
        'artists': track['artists']
    }

def fetch_playlist_tracks(playlist_id, offset, limit=100):
    try:
        results = sp.playlist_tracks(playlist_id, offset=offset, limit=limit)
        return results
    except SpotifyException as e:
        if handle_rate_limits(e):
            return fetch_playlist_tracks(playlist_id, offset, limit)
        else:
            raise

def get_tracks_from_playlist(playlist_id, min_tracks=2000):
    tracks = []
    offset = 0
    limit = 100

    while len(tracks) < min_tracks:
        try:
            results = fetch_playlist_tracks(playlist_id, offset, limit)
            if not results['items']:
                break

            for item in results['items']:
                track = item['track']
                tracks.append(get_track_details(track))
                if len(tracks) % 100 == 0:
                    logger.info(f"Fetched {len(tracks)} tracks so far...")

            offset += limit
        except Exception as e:
            logger.error(f"Error fetching tracks at offset {offset}: {e}")
            break

    return tracks

def get_genres_for_artist(artist_id):
    try:
        artist = sp.artist(artist_id)
        return artist['genres']
    except SpotifyException as e:
        if handle_rate_limits(e):
            return get_genres_for_artist(artist_id)
        else:
            raise

def classify_genres(tracks):
    genre_counter = Counter()
    for track in tracks:
        for artist in track['artists']:
            genres = get_genres_for_artist(artist['id'])
            genre_counter.update(genres)
    return genre_counter

def summarize_genres(genre_counter):
    logger.info("\n=== Genre Summary in the Source Playlist ===\n")
    for genre, count in genre_counter.items():
        logger.info(f"Genre: {genre}, Tracks: {count}")

    logger.info("\n=== Overall Summary ===")
    logger.info(f"Total Genres Found: {len(genre_counter)}")
    logger.info(f"Total Tracks Processed: {sum(genre_counter.values())}")

@app.route('/')
def run_job():
    try:
        logger.info("Fetching tracks from source playlist...")
        start_time = time.time()
        tracks = get_tracks_from_playlist(source_playlist_id, min_tracks=2000)
        end_time = time.time()
        logger.info(f"Total tracks fetched: {len(tracks)}")
        logger.info(f"Time taken: {end_time - start_time:.2f} seconds")

        logger.info("Classifying genres...")
        genre_counter = classify_genres(tracks)
        summarize_genres(genre_counter)

        return "Job completed successfully."
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return "An error occurred during the job."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
