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


MIGRATIONS = [
    ("001_create_guild_settings", migration_001_create_guild_settings),
    ("002_add_last_journal_message", migration_002_add_last_journal_message),
    ("003_add_active_role_id", migration_003_add_active_role_id),
    ("004_create_reminders", migration_004_create_reminders),
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
