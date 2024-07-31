from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
import subprocess

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///home/Dropemusic/myproject/music_data.db'
app.config['SECRET_KEY'] = '0123456789'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Database models
class Artist(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    artist_id = db.Column(db.String(100), nullable=False, unique=True)

class Genre(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    genre = db.Column(db.String(100), nullable=False, unique=True)
    playlist_id = db.Column(db.String(100), nullable=False)  # Associated Playlist ID

class UnknownPlaylist(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    playlist_id = db.Column(db.String(100), nullable=False, unique=True)

# Home route
@app.route('/')
def index():
    try:
        artists = Artist.query.all()
        genres = Genre.query.all()
        unknown_playlists = UnknownPlaylist.query.all()
        return render_template('index.html', artists=artists, genres=genres, unknown_playlists=unknown_playlists)
    except Exception as e:
        flash(f'Error retrieving data: {e}', 'danger')
        return render_template('index.html', artists=[], genres=[], unknown_playlists=[])

# Route to add an artist
@app.route('/add_artist', methods=['GET', 'POST'])
def add_artist():
    if request.method == 'POST':
        artist_id = request.form['artist_id']
        if Artist.query.filter_by(artist_id=artist_id).first():
            flash('Artist ID already exists!', 'warning')
            return redirect(url_for('add_artist'))
        try:
            new_artist = Artist(artist_id=artist_id)
            db.session.add(new_artist)
            db.session.commit()
            flash('Artist added successfully!', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'Error adding artist: {e}', 'danger')
        return redirect(url_for('index'))
    return render_template('add_artist.html')

# Route to add a genre and its associated playlist
@app.route('/add_genre', methods=['GET', 'POST'])
def add_genre():
    if request.method == 'POST':
        genre_name = request.form['genre']
        playlist_id = request.form['playlist_id']

        # Validate that the playlist_id is valid by checking the Genre table itself
        if Genre.query.filter_by(playlist_id=playlist_id).first():
            flash('Playlist ID already exists in the genre table!', 'warning')
            return redirect(url_for('add_genre'))

        try:
            new_genre = Genre(genre=genre_name, playlist_id=playlist_id)
            db.session.add(new_genre)
            db.session.commit()
            flash('Genre added successfully!', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'Error adding genre: {e}', 'danger')
        return redirect(url_for('index'))
    return render_template('add_genre.html')

# Route to add an unknown playlist
@app.route('/add_unknown_playlist', methods=['GET', 'POST'])
def add_unknown_playlist():
    if request.method == 'POST':
        playlist_id = request.form['playlist_id']
        if UnknownPlaylist.query.filter_by(playlist_id=playlist_id).first():
            flash('Unknown playlist ID already exists!', 'warning')
            return redirect(url_for('add_unknown_playlist'))
        try:
            new_unknown_playlist = UnknownPlaylist(playlist_id=playlist_id)
            db.session.add(new_unknown_playlist)
            db.session.commit()
            flash('Unknown playlist added successfully!', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'Error adding unknown playlist: {e}', 'danger')
        return redirect(url_for('index'))
    return render_template('add_unknown_playlist.html')

# Route to delete an artist
@app.route('/delete_artist/<int:id>')
def delete_artist(id):
    try:
        artist = Artist.query.get(id)
        if artist:
            db.session.delete(artist)
            db.session.commit()
            flash('Artist deleted successfully!', 'success')
        else:
            flash('Artist not found!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting artist: {e}', 'danger')
    return redirect(url_for('index'))

# Route to delete a genre
@app.route('/delete_genre/<int:id>')
def delete_genre(id):
    try:
        genre = Genre.query.get(id)
        if genre:
            db.session.delete(genre)
            db.session.commit()
            flash('Genre deleted successfully!', 'success')
        else:
            flash('Genre not found!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting genre: {e}', 'danger')
    return redirect(url_for('index'))

# Route to delete an unknown playlist
@app.route('/delete_unknown_playlist/<int:id>')
def delete_unknown_playlist(id):
    try:
        unknown_playlist = UnknownPlaylist.query.get(id)
        if unknown_playlist:
            db.session.delete(unknown_playlist)
            db.session.commit()
            flash('Unknown playlist deleted successfully!', 'success')
        else:
            flash('Unknown playlist not found!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting unknown playlist: {e}', 'danger')
    return redirect(url_for('index'))

# Route to run a backend job
@app.route('/run_job')
def run_job():
    try:
        result = subprocess.run(['python', 'backend_script.py'], check=True, capture_output=True, text=True)

        # Extract summary from the script output
        summary = extract_summary_from_output(result.stdout)

        # Store summary in a flash message
        flash(f"Summary:\n{summary}", 'info')

    except subprocess.CalledProcessError as e:
        flash(f'Error executing job: {e}', 'danger')
        print(e.stderr)  # Optionally print error
    return redirect(url_for('index'))

def extract_summary_from_output(output):
    # Example of extracting summary from the output
    lines = output.split('\n')
    summary_lines = [line for line in lines if line.startswith('Total')]
    return '\n'.join(summary_lines)

# Remove or comment out this line for production

