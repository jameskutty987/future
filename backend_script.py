import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta
import sqlite3

# Spotify API credentials
SPOTIPY_CLIENT_ID = '0e02cd071be04751a0f0602a93b6fdde'
SPOTIPY_CLIENT_SECRET = '3e8b95b835eb47239298ee7310cfcb62'
SPOTIPY_REDIRECT_URI = 'http://localhost:8888/callback'

# Spotify setup
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIPY_CLIENT_ID,
    client_secret=SPOTIPY_CLIENT_SECRET,
    redirect_uri=SPOTIPY_REDIRECT_URI,
    scope='playlist-read-private playlist-modify-public playlist-modify-private playlist-read-collaborative'
))

# Database path for PythonAnywhere
DATABASE = '/home/Dropemusic/myproject/music_data.db'

def get_db_connection():
    """Establish a connection to the database."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def fetch_tracks_from_artist_last_month(artist_id, limit=15):
    """Fetch tracks released by a given artist in the last month."""
    tracks = []
    one_month_ago = datetime.now() - timedelta(days=30)

    try:
        print(f"Fetching albums for artist ID: {artist_id}")
        results = sp.artist_albums(artist_id, album_type='album,single', limit=50)
        albums = results['items']
        print(f"Initial albums fetched: {albums}")

        # Handle pagination
        while results['next']:
            results = sp.next(results)
            albums.extend(results['items'])
            print(f"Additional albums fetched: {results['items']}")

        for album in albums:
            album_release_date = album.get('release_date')
            if album_release_date:
                release_date = datetime.strptime(album_release_date, '%Y-%m-%d')
                if release_date < one_month_ago:
                    continue

                print(f"Fetching tracks for album ID {album['id']} released on {album_release_date}")
                results = sp.album_tracks(album['id'], limit=50)
                album_tracks = results['items']
                print(f"Initial tracks fetched for album ID {album['id']}: {album_tracks}")

                # Handle pagination for tracks
                while results['next']:
                    results = sp.next(results)
                    album_tracks.extend(results['items'])
                    print(f"Additional tracks fetched for album ID {album['id']}: {results['items']}")

                for track in album_tracks:
                    track['release_date'] = album_release_date
                tracks.extend(album_tracks)

        # Filter tracks by release date
        tracks = [track for track in tracks if datetime.strptime(track['release_date'], '%Y-%m-%d') >= one_month_ago]
        tracks = tracks[:limit]

    except Exception as e:
        print(f"Error fetching tracks for artist ID {artist_id}: {e}")

    print(f"Tracks fetched for artist ID {artist_id}: {tracks}")
    return tracks

def get_track_details(track_item):
    """Extract and classify track details."""
    track = track_item
    release_date = track.get('release_date', '')
    track_name = track.get('name', '')
    track_id = track.get('id', '')
    track_uri = track.get('uri', '')

    artist_names = [artist['name'] for artist in track.get('artists', [])]
    primary_artist_name = artist_names[0] if artist_names else ''
    primary_artist_id = track['artists'][0]['id'] if track['artists'] else ''

    # Fetch audio features (e.g., tempo, key, mode, etc.)
    audio_features = sp.audio_features([track_id])
    track_features = audio_features[0] if audio_features else {}

    return {
        'track_id': track_id,
        'track_name': track_name,
        'track_uri': track_uri,
        'release_date': release_date,
        'primary_artist_name': primary_artist_name,
        'primary_artist_id': primary_artist_id,
        'audio_features': track_features
    }

def classify_track_genre(track_details):
    """Classify the genre of a track based on its primary artist's genres."""
    artist_id = track_details.get('primary_artist_id', '')
    try:
        artist_info = sp.artist(artist_id)
        genres = artist_info.get('genres', [])
        if genres:
            primary_genre = genres[0]  # Choose the first genre as the primary genre
        else:
            primary_genre = 'Unknown'
    except Exception as e:
        print(f"Error fetching genres for artist ID {artist_id}: {e}")
        primary_genre = 'Unknown'
    
    return primary_genre

def add_tracks_to_playlist(playlist_id, track_uris):
    """Add a list of track URIs to a playlist."""
    try:
        sp.playlist_add_items(playlist_id, track_uris)
        print(f"Added {len(track_uris)} tracks to playlist {playlist_id}")
    except Exception as e:
        print(f"Error adding tracks to playlist {playlist_id}: {e}")

def store_track_details_in_db(track_details, genre, playlist_id):
    """Store track details in the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insert track details into the tracks table
        cursor.execute('''
            INSERT INTO tracks (track_id, track_name, artist_id, artist_name, release_date, genre, playlist_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (track_details['track_id'], track_details['track_name'], track_details['primary_artist_id'],
              track_details['primary_artist_name'], track_details['release_date'], genre, playlist_id))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Stored track {track_details['track_id']} in the database.")
    except Exception as e:
        print(f"Error storing track details in the database: {e}")

def main():
    """Main script to fetch, classify, and add tracks to playlists."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('SELECT * FROM artist')
        artists = cursor.fetchall()
        cursor.execute('SELECT * FROM genre')
        genres = cursor.fetchall()
        cursor.execute('SELECT * FROM unknown_playlist')
        unknown_playlists = cursor.fetchall()

        summary = []

        for artist in artists:
            artist_id = artist['artist_id']
            tracks = fetch_tracks_from_artist_last_month(artist_id)

            for track in tracks:
                track_details = get_track_details(track)
                genre = classify_track_genre(track_details)

                # Check if the genre exists in the genre table and get the associated playlist_id
                genre_info = next((g for g in genres if g['genre'] == genre), None)
                if genre_info:
                    playlist_id = genre_info['playlist_id']
                else:
                    # If genre is unknown, add to the unknown playlist
                    if unknown_playlists:
                        playlist_id = unknown_playlists[0]['playlist_id']
                    else:
                        playlist_id = None

                if playlist_id:
                    add_tracks_to_playlist(playlist_id, [track_details['track_uri']])
                    store_track_details_in_db(track_details, genre, playlist_id)

                # Summarize results
                summary.append(f"Added {track_details['track_name']} by {track_details['primary_artist_name']} to {genre} playlist")

        # Print the summary
        print("Summary:")
        for item in summary:
            print(item)

    except Exception as e:
        print(f"Error during the main script execution: {e}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
