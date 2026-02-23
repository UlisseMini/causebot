#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["py-cord>=2.6.1", "python-dotenv>=1.0.0", "aiosqlite>=0.21.0", "databases>=0.9.0", "sqlalchemy>=2.0.44"]
# ///
"""One-off script: archive duplicate personal channels (ones ending in -1).

Creates a hidden "Archived Channels" category and moves duplicates there.
Also cleans up DB entries so the original channel remains as the user's channel.
"""
import asyncio
import os
import re
import sys
import discord
from dotenv import load_dotenv

# Add project root to path so we can import db module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

TOKEN = os.environ["DISCORD_BOT_TOKEN"]
GUILD_ID = int(os.environ["MAIN_GUILD_ID"])

intents = discord.Intents.default()
intents.members = True
bot = discord.Bot(intents=intents)


@bot.event
async def on_ready():
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        print(f"Guild {GUILD_ID} not found")
        await bot.close()
        return

    print(f"Connected to {guild.name}")

    # Find "Personal Channels" category
    category = discord.utils.get(guild.categories, name="Personal Channels")
    if not category:
        print("No 'Personal Channels' category found")
        await bot.close()
        return

    # Find duplicate channels: names ending in -N (numeric suffix)
    duplicates = []
    for ch in category.channels:
        if not isinstance(ch, discord.TextChannel):
            continue
        # Match names like "rose-1", "jacob-c-1", "ethan-1"
        m = re.match(r'^(.+)-(\d+)$', ch.name)
        if m:
            base_name = m.group(1)
            # Check if the base channel also exists in this category
            base_channel = discord.utils.get(category.channels, name=base_name)
            if base_channel and base_channel.id != ch.id:
                duplicates.append(ch)

    if not duplicates:
        print("No duplicate channels found")
        await bot.close()
        return

    print(f"\nFound {len(duplicates)} duplicate channels:")
    for ch in duplicates:
        print(f"  #{ch.name}")

    # Create or find "Archived Channels" category, hidden from @everyone
    archive_cat = discord.utils.get(guild.categories, name="Archived Channels")
    if not archive_cat:
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False)
        }
        archive_cat = await guild.create_category("Archived Channels", overwrites=overwrites)
        print(f"\nCreated hidden 'Archived Channels' category")
    else:
        print(f"\nUsing existing 'Archived Channels' category")

    # Set up DB connection for cleanup
    from db.connection import database
    await database.connect()

    # Move duplicates to archive and clean up DB
    for ch in duplicates:
        try:
            # Check if this channel is in the DB
            query = "SELECT user_id FROM user_private_channels WHERE guild_id = :guild_id AND channel_id = :channel_id"
            result = await database.fetch_one(query=query, values={"guild_id": guild.id, "channel_id": ch.id})
            if result:
                user_id = result["user_id"]
                # Check if the user also has the original (non-duplicate) channel in DB
                # If so, just delete this duplicate mapping
                base_name = re.match(r'^(.+)-\d+$', ch.name).group(1)
                base_channel = discord.utils.get(category.channels, name=base_name)
                if base_channel:
                    query2 = "SELECT channel_id FROM user_private_channels WHERE guild_id = :guild_id AND user_id = :user_id AND channel_id = :channel_id"
                    base_result = await database.fetch_one(query=query2, values={
                        "guild_id": guild.id, "user_id": user_id, "channel_id": base_channel.id
                    })
                    if not base_result:
                        # The DB points to the duplicate, update it to point to the original
                        await database.execute(
                            "UPDATE user_private_channels SET channel_id = :new_channel_id WHERE guild_id = :guild_id AND channel_id = :old_channel_id",
                            values={"new_channel_id": base_channel.id, "guild_id": guild.id, "old_channel_id": ch.id}
                        )
                        print(f"  DB: Updated user {user_id} channel from #{ch.name} -> #{base_channel.name}")
                    else:
                        # User already has the base channel in DB, just delete the duplicate mapping
                        await database.execute(
                            "DELETE FROM user_private_channels WHERE guild_id = :guild_id AND channel_id = :channel_id",
                            values={"guild_id": guild.id, "channel_id": ch.id}
                        )
                        print(f"  DB: Removed duplicate mapping for #{ch.name}")

            await ch.edit(category=archive_cat, reason="Archiving duplicate personal channel")
            print(f"  Moved #{ch.name} to Archived Channels")
        except Exception as e:
            print(f"  ERROR moving #{ch.name}: {e}")

    await database.disconnect()
    print(f"\nDone! Archived {len(duplicates)} duplicate channels.")
    await bot.close()


bot.run(TOKEN)
