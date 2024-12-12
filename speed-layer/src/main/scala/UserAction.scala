// Case class for user actions
case class UserAction (
    userId: String,
    songId: String,
    rating: Double,
    action: String,
    timestamp: Long
)