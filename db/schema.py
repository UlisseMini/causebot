from sqlalchemy import Table, Column, Integer, BigInteger, MetaData, String, ForeignKey, Numeric, DateTime
from sqlalchemy.sql import func

metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("user_id", BigInteger, primary_key=True),
    Column("username", String, nullable=True),
    Column("created_at", String, server_default=func.now())
)

guilds = Table(
    "guilds",
    metadata,
    Column("guild_id", BigInteger, primary_key=True),
    Column("name", String, nullable=True),
)

user_private_channels = Table(
    "user_private_channels",
    metadata,
    Column("guild_id", BigInteger, ForeignKey("guilds.guild_id"), primary_key=True),
    Column("user_id", BigInteger, ForeignKey("users.user_id"), primary_key=True),
    Column("channel_id", BigInteger, nullable=False),
    Column("created_at", String, server_default=func.now()),
    Column("last_journal_message", String, nullable=True)
)

user_xp = Table(
    "user_xp",
    metadata,
    Column("guild_id", BigInteger, ForeignKey("guilds.guild_id"), primary_key=True),
    Column("user_id", BigInteger, ForeignKey("users.user_id"), primary_key=True),
    Column("xp", Numeric(10, 3), nullable=False, default=0),
    Column("updated_at", String, server_default=func.now())
)

message_logs = Table(
    "message_logs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("guild_id", BigInteger, ForeignKey("guilds.guild_id"), nullable=False),
    Column("user_id", BigInteger, ForeignKey("users.user_id"), nullable=False),
    Column("timestamp", String, nullable=False),
    Column("xp_awarded", Numeric(10, 3), nullable=False)
)

guild_settings = Table(
    "guild_settings",
    metadata,
    Column("guild_id", BigInteger, ForeignKey("guilds.guild_id"), primary_key=True),
    Column("welcome_message", String, nullable=True),
    Column("active_role_id", BigInteger, nullable=True),
    Column("active_days", Integer, nullable=True, default=3)
)

reminders = Table(
    "reminders",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("guild_id", BigInteger, nullable=False),
    Column("user_id", BigInteger, nullable=False),
    Column("channel_id", BigInteger, nullable=False),
    Column("message_link", String, nullable=False),
    Column("message_preview", String, nullable=True),
    Column("remind_at", String, nullable=False),
    Column("created_at", String, server_default=func.now()),
    Column("completed", Integer, server_default="0")
)

one_on_one_pool = Table(
    "one_on_one_pool",
    metadata,
    Column("guild_id", BigInteger, primary_key=True),
    Column("user_id", BigInteger, primary_key=True),
    Column("joined_at", String, server_default=func.now()),
    Column("skip_until", String, nullable=True),
    Column("sat_out_at", String, nullable=True)
)

one_on_one_matches = Table(
    "one_on_one_matches",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("guild_id", BigInteger, nullable=False),
    Column("week_start", String, nullable=False),
    Column("user1_id", BigInteger, nullable=False),
    Column("user2_id", BigInteger, nullable=False),
    Column("thread_id", BigInteger, nullable=True),
    Column("user1_status", String, server_default="pending"),
    Column("user2_status", String, server_default="pending"),
    Column("reminder_count", Integer, server_default="0"),
    Column("created_at", String, server_default=func.now()),
    Column("completed_at", String, nullable=True)
)