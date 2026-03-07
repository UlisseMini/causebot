from sqlalchemy import create_engine
from datetime import datetime, timedelta
from db.connection import database, DATABASE_URL
from db.schema import users, guilds, user_private_channels, user_xp, message_logs, guild_settings, reminders, one_on_one_pool, one_on_one_matches, user_ai_config, user_ai_wakeups, channel_messages, metadata


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


async def get_active_days(guild_id: int) -> int:
    """Get the number of days of inactivity before losing the active role. Default 3."""
    query = guild_settings.select().where(guild_settings.c.guild_id == guild_id)
    result = await database.fetch_one(query)
    if result and result["active_days"] is not None:
        return result["active_days"]
    return 3


async def set_active_days(guild_id: int, days: int, guild_name: str = None):
    """Set the number of days of inactivity before losing the active role."""
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
        await database.execute(
            guild_settings.update().where(
                guild_settings.c.guild_id == guild_id
            ).values(active_days=days)
        )
    else:
        await database.execute(
            guild_settings.insert().values(
                guild_id=guild_id,
                active_days=days
            )
        )


# --- AI Companion ---

async def get_ai_config(guild_id: int, user_id: int) -> dict | None:
    """Get a user's AI companion config."""
    query = user_ai_config.select().where(
        (user_ai_config.c.guild_id == guild_id) &
        (user_ai_config.c.user_id == user_id)
    )
    result = await database.fetch_one(query)
    if not result:
        return None
    return dict(result._mapping)


async def upsert_ai_config(guild_id: int, user_id: int, system_prompt: str, enabled: bool = True):
    """Create or update a user's AI companion config."""
    existing = await get_ai_config(guild_id, user_id)
    if existing:
        await database.execute(
            user_ai_config.update().where(
                (user_ai_config.c.guild_id == guild_id) &
                (user_ai_config.c.user_id == user_id)
            ).values(system_prompt=system_prompt, enabled=1 if enabled else 0)
        )
    else:
        await database.execute(
            user_ai_config.insert().values(
                guild_id=guild_id,
                user_id=user_id,
                system_prompt=system_prompt,
                enabled=1 if enabled else 0,
            )
        )


async def update_ai_system_prompt(guild_id: int, user_id: int, new_prompt: str):
    """Update just the system prompt for a user's AI companion."""
    await database.execute(
        user_ai_config.update().where(
            (user_ai_config.c.guild_id == guild_id) &
            (user_ai_config.c.user_id == user_id)
        ).values(system_prompt=new_prompt)
    )


async def get_ai_wakeups(guild_id: int, user_id: int) -> list[dict]:
    """Get all wakeups for a user's AI companion."""
    query = user_ai_wakeups.select().where(
        (user_ai_wakeups.c.guild_id == guild_id) &
        (user_ai_wakeups.c.user_id == user_id)
    )
    results = await database.fetch_all(query)
    return [dict(row._mapping) for row in results]


async def set_ai_wakeups(guild_id: int, user_id: int, wakeups: list[dict]):
    """Replace all wakeups for a user. Each wakeup: {label, schedule, message, next_run_at}."""
    # Delete existing
    await database.execute(
        user_ai_wakeups.delete().where(
            (user_ai_wakeups.c.guild_id == guild_id) &
            (user_ai_wakeups.c.user_id == user_id)
        )
    )
    # Insert new
    for w in wakeups:
        await database.execute(
            user_ai_wakeups.insert().values(
                guild_id=guild_id,
                user_id=user_id,
                label=w["label"],
                schedule=w["schedule"],
                message=w.get("message", ""),
                next_run_at=w.get("next_run_at"),
                enabled=1,
            )
        )


async def get_due_wakeups() -> list[dict]:
    """Get all wakeups that are due (next_run_at <= now) and enabled."""
    now = datetime.utcnow().isoformat()
    query = user_ai_wakeups.select().where(
        (user_ai_wakeups.c.next_run_at <= now) &
        (user_ai_wakeups.c.enabled == 1) &
        (user_ai_wakeups.c.next_run_at != None)
    )
    results = await database.fetch_all(query)
    return [dict(row._mapping) for row in results]


async def update_wakeup_next_run(wakeup_id: int, next_run_at: str):
    """Update the next_run_at for a wakeup."""
    await database.execute(
        user_ai_wakeups.update().where(
            user_ai_wakeups.c.id == wakeup_id
        ).values(next_run_at=next_run_at)
    )


# --- Channel Messages ---

async def store_message(guild_id: int, channel_id: int, author_id: int,
                        content: str | None, attachment_text: str | None,
                        discord_message_id: int, created_at: str):
    """Store a message. Silently skips if discord_message_id already exists."""
    try:
        await database.execute(
            channel_messages.insert().values(
                guild_id=guild_id,
                channel_id=channel_id,
                author_id=author_id,
                content=content,
                attachment_text=attachment_text,
                discord_message_id=discord_message_id,
                created_at=created_at,
            )
        )
    except Exception:
        # UNIQUE constraint on discord_message_id — already stored
        pass


async def get_messages_page(channel_id: int, page: int = 1, page_size: int = 50,
                            after_date: str | None = None, before_date: str | None = None) -> tuple[list[dict], int]:
    """Get a page of messages from a channel. Returns (messages, total_count)."""
    from sqlalchemy import func as sa_func, select

    # Build where clause
    conditions = [channel_messages.c.channel_id == channel_id]
    if after_date:
        conditions.append(channel_messages.c.created_at >= after_date)
    if before_date:
        conditions.append(channel_messages.c.created_at <= before_date)

    # Count total
    count_query = select(sa_func.count()).select_from(channel_messages)
    for cond in conditions:
        count_query = count_query.where(cond)
    total = await database.fetch_val(count_query)

    # Fetch page
    query = channel_messages.select()
    for cond in conditions:
        query = query.where(cond)
    query = query.order_by(channel_messages.c.created_at.asc())
    query = query.offset((page - 1) * page_size).limit(page_size)
    results = await database.fetch_all(query)

    return [dict(row._mapping) for row in results], total


async def search_messages_db(channel_id: int, query: str, before_context: int = 3,
                             after_context: int = 3, max_results: int = 10) -> list[dict]:
    """Search messages in DB by text. Returns matches with surrounding context.
    Each result is a dict with 'match' and 'context' (list of surrounding messages)."""
    from sqlalchemy import or_

    # Find matching message IDs
    match_query = channel_messages.select().where(
        (channel_messages.c.channel_id == channel_id) &
        (or_(
            channel_messages.c.content.ilike(f"%{query}%"),
            channel_messages.c.attachment_text.ilike(f"%{query}%"),
        ))
    ).order_by(channel_messages.c.created_at.asc()).limit(max_results)
    matches = await database.fetch_all(match_query)

    if not matches:
        return []

    # For each match, fetch surrounding context
    results = []
    for match in matches:
        match_time = match["created_at"]
        match_id = match["id"]

        # Get before context
        before_query = channel_messages.select().where(
            (channel_messages.c.channel_id == channel_id) &
            (channel_messages.c.id < match_id)
        ).order_by(channel_messages.c.id.desc()).limit(before_context)
        before_msgs = await database.fetch_all(before_query)
        before_msgs = list(reversed(before_msgs))

        # Get after context
        after_query = channel_messages.select().where(
            (channel_messages.c.channel_id == channel_id) &
            (channel_messages.c.id > match_id)
        ).order_by(channel_messages.c.id.asc()).limit(after_context)
        after_msgs = await database.fetch_all(after_query)

        context_msgs = (
            [dict(m._mapping) for m in before_msgs]
            + [dict(match._mapping)]
            + [dict(m._mapping) for m in after_msgs]
        )
        results.append({
            "match": dict(match._mapping),
            "context": context_msgs,
        })

    return results


async def count_channel_messages(channel_id: int) -> int:
    """Count total messages stored for a channel."""
    from sqlalchemy import func as sa_func, select
    query = select(sa_func.count()).select_from(channel_messages).where(
        channel_messages.c.channel_id == channel_id
    )
    return await database.fetch_val(query)


async def get_latest_stored_message_id(channel_id: int) -> int | None:
    """Get the discord_message_id of the most recent stored message for a channel."""
    query = channel_messages.select().where(
        channel_messages.c.channel_id == channel_id
    ).order_by(channel_messages.c.created_at.desc()).limit(1)
    result = await database.fetch_one(query)
    return result["discord_message_id"] if result else None


async def get_earliest_stored_message_id(channel_id: int) -> int | None:
    """Get the discord_message_id of the oldest stored message for a channel."""
    query = channel_messages.select().where(
        channel_messages.c.channel_id == channel_id
    ).order_by(channel_messages.c.created_at.asc()).limit(1)
    result = await database.fetch_one(query)
    return result["discord_message_id"] if result else None


async def get_recent_messages_db(channel_id: int, limit: int = 200,
                                 after_date: str | None = None) -> list[dict]:
    """Get recent messages from DB for context assembly."""
    conditions = [channel_messages.c.channel_id == channel_id]
    if after_date:
        conditions.append(channel_messages.c.created_at >= after_date)

    query = channel_messages.select()
    for cond in conditions:
        query = query.where(cond)
    query = query.order_by(channel_messages.c.created_at.desc()).limit(limit)
    results = await database.fetch_all(query)
    return [dict(row._mapping) for row in reversed(results)]


# --- Memory Notes ---

async def get_memory_notes(guild_id: int, user_id: int) -> str | None:
    """Get a user's AI companion memory notes."""
    config = await get_ai_config(guild_id, user_id)
    return config["memory_notes"] if config else None


async def set_memory_notes(guild_id: int, user_id: int, notes: str):
    """Set a user's AI companion memory notes."""
    await database.execute(
        user_ai_config.update().where(
            (user_ai_config.c.guild_id == guild_id) &
            (user_ai_config.c.user_id == user_id)
        ).values(memory_notes=notes)
    )

