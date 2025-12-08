from sqlalchemy import create_engine
from datetime import datetime, timedelta
from db.connection import database, DATABASE_URL
from db.schema import users, guilds, user_private_channels, user_xp, message_logs, metadata


async def get_user_channel(guild_id: int, user_id: int):
    """Check if a user already has a personal channel in a guild.
    Returns the channel_id if found, None otherwise."""
    query = user_private_channels.select().where(
        (user_private_channels.c.guild_id == guild_id) &
        (user_private_channels.c.user_id == user_id)
    )
    result = await database.fetch_one(query)
    return result["channel_id"] if result else None


async def create_user_channel(guild_id: int, user_id: int, channel_id: int, username: str = None, guild_name: str = None):
    """Create or update a user_private_channel record.
    Also ensures the user and guild records exist."""
    # Ensure user exists
    user_query = users.select().where(users.c.user_id == user_id)
    user_exists = await database.fetch_one(user_query)
    if not user_exists:
        await database.execute(
            users.insert().values(user_id=user_id, username=username)
        )
    
    # Ensure guild exists
    guild_query = guilds.select().where(guilds.c.guild_id == guild_id)
    guild_exists = await database.fetch_one(guild_query)
    if not guild_exists:
        await database.execute(
            guilds.insert().values(guild_id=guild_id, name=guild_name)
        )
    
    # Check if user_private_channel record already exists
    channel_query = user_private_channels.select().where(
        (user_private_channels.c.guild_id == guild_id) &
        (user_private_channels.c.user_id == user_id)
    )
    existing_record = await database.fetch_one(channel_query)
    
    if existing_record:
        # Update existing record
        await database.execute(
            user_private_channels.update().where(
                (user_private_channels.c.guild_id == guild_id) &
                (user_private_channels.c.user_id == user_id)
            ).values(channel_id=channel_id)
        )
    else:
        # Create new record
        await database.execute(
            user_private_channels.insert().values(
                guild_id=guild_id,
                user_id=user_id,
                channel_id=channel_id
            )
        )


async def delete_user_channel(guild_id: int, user_id: int):
    """Delete a user_private_channel record from the database."""
    await database.execute(
        user_private_channels.delete().where(
            (user_private_channels.c.guild_id == guild_id) &
            (user_private_channels.c.user_id == user_id)
        )
    )


async def can_award_xp(guild_id: int, user_id: int) -> bool:
    """Check if a user can receive XP (rate limiting: at most one message per minute).
    Returns True if they can receive XP, False otherwise."""
    # Ensure user and guild exist
    user_query = users.select().where(users.c.user_id == user_id)
    user_exists = await database.fetch_one(user_query)
    if not user_exists:
        await database.execute(
            users.insert().values(user_id=user_id)
        )
    
    guild_query = guilds.select().where(guilds.c.guild_id == guild_id)
    guild_exists = await database.fetch_one(guild_query)
    if not guild_exists:
        await database.execute(
            guilds.insert().values(guild_id=guild_id)
        )
    
    # Check if user sent a message in the last minute
    one_minute_ago = (datetime.utcnow() - timedelta(minutes=1)).isoformat()
    query = message_logs.select().where(
        (message_logs.c.guild_id == guild_id) &
        (message_logs.c.user_id == user_id) &
        (message_logs.c.timestamp >= one_minute_ago)
    ).order_by(message_logs.c.timestamp.desc()).limit(1)
    
    recent_message = await database.fetch_one(query)
    return recent_message is None


async def award_xp(guild_id: int, user_id: int, xp_amount: float, username: str = None, guild_name: str = None):
    """Award XP to a user. XP is rounded to 3 decimal places."""
    # Ensure user exists
    user_query = users.select().where(users.c.user_id == user_id)
    user_exists = await database.fetch_one(user_query)
    if not user_exists:
        await database.execute(
            users.insert().values(user_id=user_id, username=username)
        )
    
    # Ensure guild exists
    guild_query = guilds.select().where(guilds.c.guild_id == guild_id)
    guild_exists = await database.fetch_one(guild_query)
    if not guild_exists:
        await database.execute(
            guilds.insert().values(guild_id=guild_id, name=guild_name)
        )
    
    # Round XP to 3 decimal places
    xp_amount = round(xp_amount, 3)
    
    # Get current timestamp
    timestamp = datetime.utcnow().isoformat()
    
    # Log the message and XP awarded
    await database.execute(
        message_logs.insert().values(
            guild_id=guild_id,
            user_id=user_id,
            timestamp=timestamp,
            xp_awarded=xp_amount
        )
    )
    
    # Update or create user_xp record
    xp_query = user_xp.select().where(
        (user_xp.c.guild_id == guild_id) &
        (user_xp.c.user_id == user_id)
    )
    existing_xp = await database.fetch_one(xp_query)
    
    if existing_xp:
        # Calculate new total XP from message_logs within 3 days
        three_days_ago = (datetime.utcnow() - timedelta(days=3)).isoformat()
        total_xp_query = message_logs.select().where(
            (message_logs.c.guild_id == guild_id) &
            (message_logs.c.user_id == user_id) &
            (message_logs.c.timestamp >= three_days_ago)
        )
        all_messages = await database.fetch_all(total_xp_query)
        total_xp = round(sum(float(msg["xp_awarded"]) for msg in all_messages), 3)
        
        await database.execute(
            user_xp.update().where(
                (user_xp.c.guild_id == guild_id) &
                (user_xp.c.user_id == user_id)
            ).values(xp=total_xp, updated_at=timestamp)
        )
    else:
        # Create new user_xp record
        await database.execute(
            user_xp.insert().values(
                guild_id=guild_id,
                user_id=user_id,
                xp=round(xp_amount, 3),
                updated_at=timestamp
            )
        )


async def get_user_xp(guild_id: int, user_id: int, days: int = 3) -> float:
    """Get a user's total XP within a rolling time period.
    
    Args:
        guild_id: The guild ID
        user_id: The user ID
        days: Number of days for the rolling period (default: 3)
    
    Returns:
        XP rounded to 3 decimal places.
    """
    # Calculate XP from message_logs within the specified period
    period_start = (datetime.utcnow() - timedelta(days=days)).isoformat()
    query = message_logs.select().where(
        (message_logs.c.guild_id == guild_id) &
        (message_logs.c.user_id == user_id) &
        (message_logs.c.timestamp >= period_start)
    )
    all_messages = await database.fetch_all(query)
    total_xp = round(sum(float(msg["xp_awarded"]) for msg in all_messages), 3)
    return total_xp


async def init_database():
    """Initialize the database by creating all tables if they don't exist."""
    # Create a synchronous engine for table creation
    # Remove the async driver part for synchronous table creation
    sync_url = DATABASE_URL.replace("+aiosqlite", "")
    engine = create_engine(sync_url)
    metadata.create_all(engine)
    engine.dispose()

