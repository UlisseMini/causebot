from sqlalchemy import create_engine, text, inspect
from db.connection import DATABASE_URL

# List of migrations to run in order
# Each migration is a tuple of (name, sql_statement)
MIGRATIONS = [
    (
        "001_create_guild_settings",
        """
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id BIGINT PRIMARY KEY REFERENCES guilds(guild_id),
            welcome_message TEXT
        )
        """
    ),
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
        for name, sql in MIGRATIONS:
            if name not in applied:
                print(f"Running migration: {name}")
                conn.execute(text(sql))
                conn.execute(text("INSERT INTO migrations (name) VALUES (:name)"), {"name": name})
                conn.commit()
                print(f"Migration {name} complete")

    engine.dispose()
