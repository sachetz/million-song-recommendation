// Case class to represent the user action
case class UserAction(
    userId: String,
    songId: String,
    rating: Double,
    action: String,
    timestamp: Long
)
