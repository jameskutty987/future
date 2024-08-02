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

# Database setup
DATABASE = 'C:/Users/AKHIL/Desktop/drope/music_data.db'

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
    release_date = track.get('release_date')
    if release_date:
        try:
            release_date = datetime.strptime(release_date, '%Y-%m-%d')
        except ValueError:
            release_date = None

    one_month_ago = datetime.now() - timedelta(days=30)
    if release_date and release_date >= one_month_ago:
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
        print(f"Error fetching artist information: {e}")
        return 'Unknown'

def add_tracks_to_playlist(playlist_id, tracks):
    """Add tracks to the specified playlist."""
    if tracks:
        track_ids = [track['id'] for track in tracks if track['id']]
        if not track_ids:
            return 0
        try:
            sp.playlist_add_items(playlist_id, track_ids)
            return len(track_ids)
        except Exception as e:
            print(f"Error adding tracks to playlist ID {playlist_id}: {e}")
            return 0
    return 0

def job():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("Fetching genres from database")
    cursor.execute("SELECT * FROM genre")
    genres = cursor.fetchall()
    print(f"Genres fetched: {genres}")
    
    print("Fetching unknown playlists from database")
    cursor.execute("SELECT * FROM unknown_playlist")
    unknown_playlists = cursor.fetchall()
    print(f"Unknown playlists fetched: {unknown_playlists}")
    
    if not genres:
        print("No genres found in database.")
        return

    total_tracks_fetched = 0
    total_tracks_processed = 0
    total_tracks_added = 0
    genre_add_count = {}

    genre_to_playlist = {row['genre'].strip().lower(): row['playlist_id'].strip() for row in genres if row['genre'] and row['playlist_id']}
    print(f"Genre to Playlist Mapping: {genre_to_playlist}")

    unknown_playlist_id = [row['playlist_id'] for row in unknown_playlists if row['playlist_id']][0] if unknown_playlists else None
    print(f"Unknown Playlist ID: {unknown_playlist_id}")

    print("Fetching artists from database")
    cursor.execute("SELECT * FROM artist")
    artists = cursor.fetchall()
    print(f"Artists fetched: {artists}")

    for artist in artists:
        artist_id = artist['artist_id'].strip()
        if not artist_id:
            continue

        print(f"\nProcessing artist ID: {artist_id}")

        tracks = fetch_tracks_from_artist_last_month(artist_id)  # Updated function call
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
                playlist_id = genre_to_playlist.get(genre, unknown_playlist_id)
                if playlist_id:
                    count_added = add_tracks_to_playlist(playlist_id, [track])
                    total_tracks_added += count_added
                    if genre not in genre_add_count:
                        genre_add_count[genre] = 0
                    genre_add_count[genre] += count_added

    # Simplified results summary
    print("\nSummary:")
    print(f"Total tracks fetched: {total_tracks_fetched}")
    print(f"Total tracks processed: {total_tracks_processed}")
    print(f"Total tracks added to playlist: {total_tracks_added}")

    conn.close()

if __name__ == "__main__":
    job()
