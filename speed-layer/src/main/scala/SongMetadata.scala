// Case class for song metadata
case class SongMetadata (
    song_id: String,
    title: String,
    artist_name: String,
    album_name: String,
    year: Int,
    danceability: Double,
    duration: Double,
    energy: Double,
    loudness: Double,
    mode: Int,
    tempo: Double
)