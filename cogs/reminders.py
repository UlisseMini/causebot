# ABOUTME: Handles reminder scheduling and delivery.
# ABOUTME: Users can set reminders for messages that will be delivered after a delay.

import discord
from discord.ext import commands, tasks
from discord import option
import logging
import re
from datetime import datetime, timedelta
from db.actions import create_reminder, get_due_reminders, mark_reminder_completed


def parse_time_interval(time_str: str) -> timedelta | None:
    """Parse a time interval string like '1h30m' or '2d' into a timedelta.

    Supports: s (seconds), m (minutes), h (hours), d (days), w (weeks)
    Returns None if the format is invalid.
    """
    pattern = r'(\d+)([smhdw])'
    matches = re.findall(pattern, time_str.lower())

    if not matches:
        return None

    total = timedelta()
    for value, unit in matches:
        value = int(value)
        if unit == 's':
            total += timedelta(seconds=value)
        elif unit == 'm':
            total += timedelta(minutes=value)
        elif unit == 'h':
            total += timedelta(hours=value)
        elif unit == 'd':
            total += timedelta(days=value)
        elif unit == 'w':
            total += timedelta(weeks=value)

    if total == timedelta():
        return None

    return total


def parse_message_link(link: str) -> tuple[int, int, int] | None:
    """Parse a Discord message link into (guild_id, channel_id, message_id).

    Returns None if the format is invalid.
    """
    parts = link.strip().split("/")
    if len(parts) < 3:
        return None

    try:
        guild_id = int(parts[-3])
        channel_id = int(parts[-2])
        message_id = int(parts[-1])
        return (guild_id, channel_id, message_id)
    except ValueError:
        return None


class Reminders(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_reminders.start()

    def cog_unload(self):
        self.check_reminders.cancel()

    @tasks.loop(minutes=1)
    async def check_reminders(self):
        """Check for due reminders and send them."""
        try:
            due_reminders = await get_due_reminders()
            for reminder in due_reminders:
                await self._send_reminder(reminder)
        except Exception as e:
            logging.error(f"Error checking reminders: {str(e)}")

    @check_reminders.before_loop
    async def before_check_reminders(self):
        """Wait until the bot is ready before starting the loop."""
        await self.bot.wait_until_ready()

    async def _send_reminder(self, reminder):
        """Send a single reminder and mark it as completed."""
        reminder_id = reminder["id"]
        guild_id = reminder["guild_id"]
        user_id = reminder["user_id"]
        channel_id = reminder["channel_id"]
        message_link = reminder["message_link"]
        message_preview = reminder["message_preview"]

        try:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logging.warning(f"Guild {guild_id} not found for reminder {reminder_id}")
                await mark_reminder_completed(reminder_id)
                return

            channel = guild.get_channel(channel_id)
            if not channel:
                logging.warning(f"Channel {channel_id} not found for reminder {reminder_id}")
                await mark_reminder_completed(reminder_id)
                return

            # Build reminder message
            content = f"🔔 **Reminder** <@{user_id}>\n\n"
            if message_preview:
                # Truncate preview if too long
                preview = message_preview[:200] + "..." if len(message_preview) > 200 else message_preview
                content += f"> {preview}\n\n"
            content += f"[Original message]({message_link})"

            await channel.send(content)
            await mark_reminder_completed(reminder_id)

        except discord.Forbidden:
            logging.error(f"Missing permissions to send reminder {reminder_id} in channel {channel_id}")
            await mark_reminder_completed(reminder_id)
        except discord.HTTPException as e:
            logging.error(f"Failed to send reminder {reminder_id}: {str(e)}")
            # Don't mark as completed - will retry next cycle
        except Exception as e:
            logging.error(f"Unexpected error sending reminder {reminder_id}: {str(e)}")

    @discord.slash_command(name="remindme", description="Set a reminder for a message")
    @option("message_link", description="Link to the message (right-click -> Copy Message Link)")
    @option("time", description="When to remind you (e.g., 1h, 30m, 2d, 1w, 1h30m)")
    async def remindme(self, ctx: discord.ApplicationContext, message_link: str, time: str):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        # Parse time interval
        delta = parse_time_interval(time)
        if not delta:
            await ctx.respond(
                "Invalid time format. Use combinations like: `30s`, `5m`, `2h`, `1d`, `1w`, `1h30m`",
                ephemeral=True
            )
            return

        # Parse message link
        parsed = parse_message_link(message_link)
        if not parsed:
            await ctx.respond(
                "Invalid message link. Right-click a message and select 'Copy Message Link'.",
                ephemeral=True
            )
            return

        link_guild_id, link_channel_id, message_id = parsed

        # Verify the message is from this guild
        if link_guild_id != guild.id:
            await ctx.respond(
                "That message is from a different server.",
                ephemeral=True
            )
            return

        # Try to fetch the message to get a preview
        message_preview = None
        try:
            channel = guild.get_channel(link_channel_id)
            if channel:
                source_message = await channel.fetch_message(message_id)
                if source_message.content:
                    message_preview = source_message.content
        except discord.NotFound:
            pass  # Message may have been deleted, that's okay
        except discord.Forbidden:
            pass  # No access to channel, that's okay
        except Exception:
            pass  # Any other error, continue without preview

        # Calculate remind_at time
        remind_at = datetime.utcnow() + delta

        # Store reminder
        await create_reminder(
            guild_id=guild.id,
            user_id=ctx.author.id,
            channel_id=ctx.channel.id,
            message_link=message_link,
            message_preview=message_preview,
            remind_at=remind_at
        )

        # Format confirmation message
        # Show relative time
        await ctx.respond(
            f"Got it! I'll remind you about that message <t:{int(remind_at.timestamp())}:R>.",
            ephemeral=True
        )


def setup(bot):
    bot.add_cog(Reminders(bot))
