from sqlalchemy import create_engine, text, inspect
from db.connection import DATABASE_URL


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    result = conn.execute(text(f"PRAGMA table_info({table_name})"))
    columns = {row[1] for row in result}
    return column_name in columns


# List of migrations to run in order
# Each migration is a tuple of (name, migration_function)
def migration_001_create_guild_settings(conn):
    """Create guild_settings table."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id BIGINT PRIMARY KEY REFERENCES guilds(guild_id),
            welcome_message TEXT
        )
    """))


def migration_002_add_last_journal_message(conn):
    """Add last_journal_message column to user_private_channels."""
    if not _column_exists(conn, "user_private_channels", "last_journal_message"):
        conn.execute(text("ALTER TABLE user_private_channels ADD COLUMN last_journal_message TEXT"))


def migration_003_add_active_role_id(conn):
    """Add active_role_id column to guild_settings."""
    if not _column_exists(conn, "guild_settings", "active_role_id"):
        conn.execute(text("ALTER TABLE guild_settings ADD COLUMN active_role_id BIGINT"))


def migration_004_create_reminders(conn):
    """Create reminders table."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            message_link TEXT NOT NULL,
            message_preview TEXT,
            remind_at TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            completed INTEGER DEFAULT 0
        )
    """))


def migration_005_create_one_on_one_pool(conn):
    """Create one_on_one_pool table for users who opt into weekly 1-1 matching."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS one_on_one_pool (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            joined_at TEXT DEFAULT CURRENT_TIMESTAMP,
            skip_until TEXT,
            sat_out_at TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
    """))


def migration_006_create_one_on_one_matches(conn):
    """Create one_on_one_matches table for tracking weekly pairings."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS one_on_one_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id BIGINT NOT NULL,
            week_start TEXT NOT NULL,
            user1_id BIGINT NOT NULL,
            user2_id BIGINT NOT NULL,
            thread_id BIGINT,
            user1_status TEXT DEFAULT 'pending',
            user2_status TEXT DEFAULT 'pending',
            reminder_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT
        )
    """))


def migration_007_add_active_days(conn):
    """Add active_days column to guild_settings (default 3 days)."""
    if not _column_exists(conn, "guild_settings", "active_days"):
        conn.execute(text("ALTER TABLE guild_settings ADD COLUMN active_days INTEGER DEFAULT 3"))


def migration_008_create_user_ai_config(conn):
    """Create user_ai_config table for AI companion system prompts."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS user_ai_config (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            system_prompt TEXT,
            enabled INTEGER DEFAULT 1,
            PRIMARY KEY (guild_id, user_id)
        )
    """))


def migration_009_create_user_ai_wakeups(conn):
    """Create user_ai_wakeups table for scheduled AI check-ins."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS user_ai_wakeups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            label TEXT NOT NULL,
            schedule TEXT NOT NULL,
            message TEXT,
            next_run_at TEXT,
            enabled INTEGER DEFAULT 1
        )
    """))


def migration_010_create_channel_messages(conn):
    """Create channel_messages table for full-text message storage."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS channel_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            author_id BIGINT NOT NULL,
            content TEXT,
            attachment_text TEXT,
            discord_message_id BIGINT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_channel_messages_channel_time
        ON channel_messages (channel_id, created_at)
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_messages_discord_id
        ON channel_messages (discord_message_id)
    """))


def migration_011_add_memory_notes(conn):
    """Add memory_notes column to user_ai_config."""
    if not _column_exists(conn, "user_ai_config", "memory_notes"):
        conn.execute(text("ALTER TABLE user_ai_config ADD COLUMN memory_notes TEXT"))


def migration_012_add_wakeup_channel_id(conn):
    """Add channel_id column to user_ai_wakeups so wakeups can fire in specific channels/threads."""
    if not _column_exists(conn, "user_ai_wakeups", "channel_id"):
        conn.execute(text("ALTER TABLE user_ai_wakeups ADD COLUMN channel_id BIGINT"))


MIGRATIONS = [
    ("001_create_guild_settings", migration_001_create_guild_settings),
    ("002_add_last_journal_message", migration_002_add_last_journal_message),
    ("003_add_active_role_id", migration_003_add_active_role_id),
    ("004_create_reminders", migration_004_create_reminders),
    ("005_create_one_on_one_pool", migration_005_create_one_on_one_pool),
    ("006_create_one_on_one_matches", migration_006_create_one_on_one_matches),
    ("007_add_active_days", migration_007_add_active_days),
    ("008_create_user_ai_config", migration_008_create_user_ai_config),
    ("009_create_user_ai_wakeups", migration_009_create_user_ai_wakeups),
    ("010_create_channel_messages", migration_010_create_channel_messages),
    ("011_add_memory_notes", migration_011_add_memory_notes),
    ("012_add_wakeup_channel_id", migration_012_add_wakeup_channel_id),
]


def run_migrations():
    """Run all pending migrations."""
    sync_url = DATABASE_URL.replace("+aiosqlite", "")
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        # Create migrations tracking table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS migrations (
                name TEXT PRIMARY KEY,
                applied_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.commit()

        # Get list of already applied migrations
        result = conn.execute(text("SELECT name FROM migrations"))
        applied = {row[0] for row in result}

        # Run pending migrations
        for name, migration_func in MIGRATIONS:
            if name not in applied:
                print(f"Running migration: {name}")
                migration_func(conn)
                conn.execute(text("INSERT INTO migrations (name) VALUES (:name)"), {"name": name})
                conn.commit()
                print(f"Migration {name} complete")

    engine.dispose()
