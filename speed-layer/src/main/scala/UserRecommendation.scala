// Case class for user-specific recommendations
case class UserRecommendation (
    user_id: String,
    song_id: String,
    title: String,
    artist_name: String,
    album_name: String,
    year: Int
)