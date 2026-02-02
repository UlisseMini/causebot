from sqlalchemy import create_engine
from datetime import datetime, timedelta
from db.connection import database, DATABASE_URL
from db.schema import users, guilds, user_private_channels, user_xp, message_logs, guild_settings, reminders, one_on_one_pool, one_on_one_matches, metadata


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


async def get_welcome_message(guild_id: int) -> str | None:
    """Get the welcome message template for a guild.
    Returns the message template if set, None otherwise."""
    query = guild_settings.select().where(guild_settings.c.guild_id == guild_id)
    result = await database.fetch_one(query)
    return result["welcome_message"] if result else None


async def set_welcome_message(guild_id: int, message: str, guild_name: str = None):
    """Set the welcome message template for a guild."""
    # Ensure guild exists
    guild_query = guilds.select().where(guilds.c.guild_id == guild_id)
    guild_exists = await database.fetch_one(guild_query)
    if not guild_exists:
        await database.execute(
            guilds.insert().values(guild_id=guild_id, name=guild_name)
        )

    # Check if guild_settings record already exists
    settings_query = guild_settings.select().where(guild_settings.c.guild_id == guild_id)
    existing_record = await database.fetch_one(settings_query)

    if existing_record:
        # Update existing record
        await database.execute(
            guild_settings.update().where(
                guild_settings.c.guild_id == guild_id
            ).values(welcome_message=message)
        )
    else:
        # Create new record
        await database.execute(
            guild_settings.insert().values(
                guild_id=guild_id,
                welcome_message=message
            )
        )


async def update_last_journal_message(guild_id: int, user_id: int):
    """Update the last journal message timestamp for a user's personal channel."""
    timestamp = datetime.utcnow().isoformat()
    await database.execute(
        user_private_channels.update().where(
            (user_private_channels.c.guild_id == guild_id) &
            (user_private_channels.c.user_id == user_id)
        ).values(last_journal_message=timestamp)
    )


async def get_active_users(guild_id: int, days: int = 3) -> list[int]:
    """Get list of user IDs who have journaled in their personal channel within the last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    query = user_private_channels.select().where(
        (user_private_channels.c.guild_id == guild_id) &
        (user_private_channels.c.last_journal_message >= cutoff)
    )
    results = await database.fetch_all(query)
    return [row["user_id"] for row in results]


async def get_active_role_id(guild_id: int) -> int | None:
    """Get the active role ID for a guild."""
    query = guild_settings.select().where(guild_settings.c.guild_id == guild_id)
    result = await database.fetch_one(query)
    return result["active_role_id"] if result else None


async def set_active_role_id(guild_id: int, role_id: int, guild_name: str = None):
    """Set the active role ID for a guild."""
    # Ensure guild exists
    guild_query = guilds.select().where(guilds.c.guild_id == guild_id)
    guild_exists = await database.fetch_one(guild_query)
    if not guild_exists:
        await database.execute(
            guilds.insert().values(guild_id=guild_id, name=guild_name)
        )

    # Check if guild_settings record already exists
    settings_query = guild_settings.select().where(guild_settings.c.guild_id == guild_id)
    existing_record = await database.fetch_one(settings_query)

    if existing_record:
        # Update existing record
        await database.execute(
            guild_settings.update().where(
                guild_settings.c.guild_id == guild_id
            ).values(active_role_id=role_id)
        )
    else:
        # Create new record
        await database.execute(
            guild_settings.insert().values(
                guild_id=guild_id,
                active_role_id=role_id
            )
        )


async def create_reminder(guild_id: int, user_id: int, channel_id: int, message_link: str, message_preview: str | None, remind_at: datetime):
    """Create a new reminder."""
    await database.execute(
        reminders.insert().values(
            guild_id=guild_id,
            user_id=user_id,
            channel_id=channel_id,
            message_link=message_link,
            message_preview=message_preview,
            remind_at=remind_at.isoformat(),
        )
    )


async def get_due_reminders():
    """Get all reminders that are due and not yet completed."""
    now = datetime.utcnow().isoformat()
    query = reminders.select().where(
        (reminders.c.remind_at <= now) &
        (reminders.c.completed == 0)
    )
    return await database.fetch_all(query)


async def mark_reminder_completed(reminder_id: int):
    """Mark a reminder as completed."""
    await database.execute(
        reminders.update().where(reminders.c.id == reminder_id).values(completed=1)
    )


async def init_database():
    """Initialize the database by creating all tables if they don't exist."""
    # Create a synchronous engine for table creation
    # Remove the async driver part for synchronous table creation
    sync_url = DATABASE_URL.replace("+aiosqlite", "")
    engine = create_engine(sync_url)
    metadata.create_all(engine)
    engine.dispose()


# --- 1-1 Pool Management ---

async def join_one_on_one_pool(guild_id: int, user_id: int) -> bool:
    """Add a user to the 1-1 matching pool. Returns True if newly joined, False if already in pool."""
    query = one_on_one_pool.select().where(
        (one_on_one_pool.c.guild_id == guild_id) &
        (one_on_one_pool.c.user_id == user_id)
    )
    existing = await database.fetch_one(query)
    if existing:
        return False
    await database.execute(
        one_on_one_pool.insert().values(guild_id=guild_id, user_id=user_id)
    )
    return True


async def leave_one_on_one_pool(guild_id: int, user_id: int) -> bool:
    """Remove a user from the 1-1 matching pool. Returns True if removed, False if not in pool."""
    query = one_on_one_pool.select().where(
        (one_on_one_pool.c.guild_id == guild_id) &
        (one_on_one_pool.c.user_id == user_id)
    )
    existing = await database.fetch_one(query)
    if not existing:
        return False
    await database.execute(
        one_on_one_pool.delete().where(
            (one_on_one_pool.c.guild_id == guild_id) &
            (one_on_one_pool.c.user_id == user_id)
        )
    )
    return True


async def get_one_on_one_pool_status(guild_id: int, user_id: int) -> dict | None:
    """Get a user's pool status. Returns dict with joined_at, skip_until, sat_out_at, or None if not in pool."""
    query = one_on_one_pool.select().where(
        (one_on_one_pool.c.guild_id == guild_id) &
        (one_on_one_pool.c.user_id == user_id)
    )
    result = await database.fetch_one(query)
    if not result:
        return None
    return {
        "joined_at": result["joined_at"],
        "skip_until": result["skip_until"],
        "sat_out_at": result["sat_out_at"]
    }


async def set_one_on_one_skip(guild_id: int, user_id: int, skip_until: str | None):
    """Set or clear the skip_until date for a user."""
    await database.execute(
        one_on_one_pool.update().where(
            (one_on_one_pool.c.guild_id == guild_id) &
            (one_on_one_pool.c.user_id == user_id)
        ).values(skip_until=skip_until)
    )


async def get_available_pool_members(guild_id: int, week_start: str) -> list[int]:
    """Get users in pool who are not skipping this week."""
    query = one_on_one_pool.select().where(
        (one_on_one_pool.c.guild_id == guild_id) &
        ((one_on_one_pool.c.skip_until == None) | (one_on_one_pool.c.skip_until <= week_start))
    )
    results = await database.fetch_all(query)
    return [row["user_id"] for row in results]


async def mark_user_sat_out(guild_id: int, user_id: int):
    """Mark that a user sat out this week (for fair rotation)."""
    timestamp = datetime.utcnow().isoformat()
    await database.execute(
        one_on_one_pool.update().where(
            (one_on_one_pool.c.guild_id == guild_id) &
            (one_on_one_pool.c.user_id == user_id)
        ).values(sat_out_at=timestamp)
    )


async def get_users_who_sat_out_recently(guild_id: int, since: str) -> list[int]:
    """Get users who sat out since a given date."""
    query = one_on_one_pool.select().where(
        (one_on_one_pool.c.guild_id == guild_id) &
        (one_on_one_pool.c.sat_out_at != None) &
        (one_on_one_pool.c.sat_out_at >= since)
    )
    results = await database.fetch_all(query)
    return [row["user_id"] for row in results]


# --- 1-1 Match Management ---

async def create_one_on_one_match(guild_id: int, week_start: str, user1_id: int, user2_id: int, thread_id: int | None) -> int:
    """Create a new match record. Returns the match ID."""
    result = await database.execute(
        one_on_one_matches.insert().values(
            guild_id=guild_id,
            week_start=week_start,
            user1_id=user1_id,
            user2_id=user2_id,
            thread_id=thread_id
        )
    )
    return result


async def get_match_by_thread(thread_id: int) -> dict | None:
    """Get a match by its thread ID."""
    query = one_on_one_matches.select().where(one_on_one_matches.c.thread_id == thread_id)
    result = await database.fetch_one(query)
    if not result:
        return None
    return dict(result._mapping)


async def get_match_by_id(match_id: int) -> dict | None:
    """Get a match by its ID."""
    query = one_on_one_matches.select().where(one_on_one_matches.c.id == match_id)
    result = await database.fetch_one(query)
    if not result:
        return None
    return dict(result._mapping)


async def update_match_status(match_id: int, user_id: int, status: str):
    """Update a user's status in a match (pending/confirmed/declined)."""
    match = await get_match_by_id(match_id)
    if not match:
        return
    if match["user1_id"] == user_id:
        await database.execute(
            one_on_one_matches.update().where(one_on_one_matches.c.id == match_id).values(user1_status=status)
        )
    elif match["user2_id"] == user_id:
        await database.execute(
            one_on_one_matches.update().where(one_on_one_matches.c.id == match_id).values(user2_status=status)
        )


async def complete_match(match_id: int):
    """Mark a match as completed."""
    timestamp = datetime.utcnow().isoformat()
    await database.execute(
        one_on_one_matches.update().where(one_on_one_matches.c.id == match_id).values(completed_at=timestamp)
    )


async def increment_match_reminder(match_id: int):
    """Increment the reminder count for a match."""
    match = await get_match_by_id(match_id)
    if match:
        await database.execute(
            one_on_one_matches.update().where(one_on_one_matches.c.id == match_id).values(
                reminder_count=match["reminder_count"] + 1
            )
        )


async def get_matches_needing_reminder(guild_id: int, week_start: str, max_reminders: int) -> list[dict]:
    """Get matches that have pending status and haven't exceeded reminder limit."""
    query = one_on_one_matches.select().where(
        (one_on_one_matches.c.guild_id == guild_id) &
        (one_on_one_matches.c.week_start == week_start) &
        (one_on_one_matches.c.completed_at == None) &
        (one_on_one_matches.c.reminder_count < max_reminders) &
        ((one_on_one_matches.c.user1_status == "pending") | (one_on_one_matches.c.user2_status == "pending"))
    )
    results = await database.fetch_all(query)
    return [dict(row._mapping) for row in results]


async def get_user_match_history(guild_id: int, user_id: int, limit: int = 10) -> list[dict]:
    """Get a user's past matches."""
    query = one_on_one_matches.select().where(
        (one_on_one_matches.c.guild_id == guild_id) &
        ((one_on_one_matches.c.user1_id == user_id) | (one_on_one_matches.c.user2_id == user_id))
    ).order_by(one_on_one_matches.c.created_at.desc()).limit(limit)
    results = await database.fetch_all(query)
    return [dict(row._mapping) for row in results]


async def get_users_matched_this_week(guild_id: int, week_start: str) -> set[int]:
    """Get all user IDs who have been matched this week."""
    query = one_on_one_matches.select().where(
        (one_on_one_matches.c.guild_id == guild_id) &
        (one_on_one_matches.c.week_start == week_start)
    )
    results = await database.fetch_all(query)
    users_set = set()
    for row in results:
        users_set.add(row["user1_id"])
        users_set.add(row["user2_id"])
    return users_set


async def get_user_partners_this_week(guild_id: int, user_id: int, week_start: str) -> set[int]:
    """Get the user IDs that a specific user has been paired with this week."""
    query = one_on_one_matches.select().where(
        (one_on_one_matches.c.guild_id == guild_id) &
        (one_on_one_matches.c.week_start == week_start) &
        ((one_on_one_matches.c.user1_id == user_id) | (one_on_one_matches.c.user2_id == user_id))
    )
    results = await database.fetch_all(query)
    partners = set()
    for row in results:
        if row["user1_id"] == user_id:
            partners.add(row["user2_id"])
        else:
            partners.add(row["user1_id"])
    return partners


async def get_all_user_channels(guild_id: int) -> list[dict]:
    """Get all user channel mappings for a guild.
    Returns list of dicts with user_id and channel_id."""
    query = user_private_channels.select().where(
        user_private_channels.c.guild_id == guild_id
    )
    results = await database.fetch_all(query)
    return [{"user_id": row["user_id"], "channel_id": row["channel_id"]} for row in results]

