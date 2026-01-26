# ABOUTME: Handles weekly 1-1 matching between community members.
# ABOUTME: Users opt in to a pool, get paired each Sunday, and coordinate via threads.

import discord
from discord.ext import commands, tasks
from discord import option, SlashCommandGroup
import logging
import random
from datetime import datetime, timedelta, timezone

from db.actions import (
    join_one_on_one_pool,
    leave_one_on_one_pool,
    get_one_on_one_pool_status,
    set_one_on_one_skip,
    get_available_pool_members,
    mark_user_sat_out,
    get_users_who_sat_out_recently,
    create_one_on_one_match,
    get_match_by_thread,
    get_match_by_id,
    update_match_status,
    complete_match,
    increment_match_reminder,
    get_matches_needing_reminder,
    get_user_match_history,
    get_users_matched_this_week,
    get_user_partners_this_week,
)


def get_week_start() -> str:
    """Get the ISO date string for the start of the current week (Monday)."""
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat()


class OneOnOnes(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.weekly_matching.start()
        self.send_reminders.start()

    def cog_unload(self):
        self.weekly_matching.cancel()
        self.send_reminders.cancel()

    one_on_ones = SlashCommandGroup("1-1s", "Weekly 1-1 matching commands")

    @one_on_ones.command(name="join", description="Join the weekly 1-1 matching pool")
    async def join(self, ctx: discord.ApplicationContext):
        if not ctx.guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        joined = await join_one_on_one_pool(ctx.guild.id, ctx.author.id)
        if joined:
            await ctx.respond(
                "You've joined the 1-1 matching pool! You'll be paired with someone each week.",
                ephemeral=True
            )
        else:
            await ctx.respond("You're already in the 1-1 matching pool.", ephemeral=True)

    @one_on_ones.command(name="leave", description="Leave the weekly 1-1 matching pool")
    async def leave(self, ctx: discord.ApplicationContext):
        if not ctx.guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        left = await leave_one_on_one_pool(ctx.guild.id, ctx.author.id)
        if left:
            await ctx.respond("You've left the 1-1 matching pool.", ephemeral=True)
        else:
            await ctx.respond("You're not in the 1-1 matching pool.", ephemeral=True)

    @one_on_ones.command(name="status", description="Check your 1-1 matching pool status")
    async def status(self, ctx: discord.ApplicationContext):
        if not ctx.guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        status = await get_one_on_one_pool_status(ctx.guild.id, ctx.author.id)
        if not status:
            await ctx.respond(
                "You're not in the 1-1 matching pool. Use `/1-1s join` to join.",
                ephemeral=True
            )
            return

        lines = ["**Your 1-1 Pool Status**", ""]
        lines.append(f"Joined: {status['joined_at'][:10]}")

        if status["skip_until"]:
            skip_date = datetime.fromisoformat(status["skip_until"])
            if skip_date > datetime.now(timezone.utc):
                lines.append(f"Skipping until: {status['skip_until'][:10]}")
            else:
                lines.append("Not skipping any weeks")
        else:
            lines.append("Not skipping any weeks")

        await ctx.respond("\n".join(lines), ephemeral=True)

    @one_on_ones.command(name="skip", description="Skip 1-1 matching for a number of weeks")
    @option("weeks", description="Number of weeks to skip (1-8)", min_value=1, max_value=8)
    async def skip(self, ctx: discord.ApplicationContext, weeks: int):
        if not ctx.guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        status = await get_one_on_one_pool_status(ctx.guild.id, ctx.author.id)
        if not status:
            await ctx.respond(
                "You're not in the 1-1 matching pool. Use `/1-1s join` to join first.",
                ephemeral=True
            )
            return

        skip_until = datetime.now(timezone.utc) + timedelta(weeks=weeks)
        await set_one_on_one_skip(ctx.guild.id, ctx.author.id, skip_until.isoformat())
        await ctx.respond(
            f"You'll skip matching for {weeks} week(s), until {skip_until.strftime('%B %d, %Y')}.",
            ephemeral=True
        )

    @one_on_ones.command(name="history", description="View your past 1-1 matches")
    async def history(self, ctx: discord.ApplicationContext):
        if not ctx.guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        matches = await get_user_match_history(ctx.guild.id, ctx.author.id, limit=10)
        if not matches:
            await ctx.respond("You don't have any past 1-1 matches.", ephemeral=True)
            return

        lines = ["**Your Past 1-1 Matches**", ""]
        for match in matches:
            partner_id = match["user2_id"] if match["user1_id"] == ctx.author.id else match["user1_id"]
            week = match["week_start"]
            status_str = "completed" if match["completed_at"] else "in progress"
            lines.append(f"- Week of {week}: <@{partner_id}> ({status_str})")

        await ctx.respond("\n".join(lines), ephemeral=True)

    @one_on_ones.command(name="run_matching", description="[Admin] Manually trigger weekly matching")
    @commands.has_permissions(administrator=True)
    async def run_matching(self, ctx: discord.ApplicationContext):
        if not ctx.guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)
        count = await self._run_matching_for_guild(ctx.guild)
        await ctx.respond(f"Matching complete! Created {count} match(es).", ephemeral=True)

    async def _get_one_on_ones_channel(self, guild: discord.Guild) -> discord.TextChannel | None:
        """Find the #1-1s channel in a guild."""
        for channel in guild.text_channels:
            if channel.name == "1-1s":
                return channel
        return None

    async def _run_matching_for_guild(self, guild: discord.Guild) -> int:
        """Run the matching algorithm for a guild. Returns number of matches created."""
        week_start = get_week_start()

        # Get available members (not skipping)
        available = await get_available_pool_members(guild.id, week_start)
        if len(available) < 2:
            return 0

        # Filter out those already matched this week
        already_matched = await get_users_matched_this_week(guild.id, week_start)
        available = [u for u in available if u not in already_matched]

        if len(available) < 2:
            return 0

        # Handle odd number by sitting one out (preferring those who haven't sat out recently)
        sit_out_user = None
        if len(available) % 2 == 1:
            # Get users who sat out in the last 4 weeks
            four_weeks_ago = (datetime.now(timezone.utc) - timedelta(weeks=4)).isoformat()
            recently_sat_out = set(await get_users_who_sat_out_recently(guild.id, four_weeks_ago))

            # Prefer to sit out someone who hasn't sat out recently
            candidates = [u for u in available if u not in recently_sat_out]
            if candidates:
                sit_out_user = random.choice(candidates)
            else:
                sit_out_user = random.choice(available)

            available.remove(sit_out_user)
            await mark_user_sat_out(guild.id, sit_out_user)

        # Shuffle and pair
        random.shuffle(available)
        pairs = [(available[i], available[i + 1]) for i in range(0, len(available), 2)]

        # Find the #1-1s channel
        channel = await self._get_one_on_ones_channel(guild)
        if not channel:
            logging.warning(f"No #1-1s channel found in guild {guild.id}")
            return 0

        matches_created = 0
        for user1_id, user2_id in pairs:
            try:
                thread = await channel.create_thread(
                    name=f"1-1: Week of {week_start}",
                    type=discord.ChannelType.public_thread,
                    auto_archive_duration=10080  # 7 days
                )

                # Post instructions
                message = (
                    f"Hey <@{user1_id}> and <@{user2_id}>! You've been matched for a 1-1 this week.\n\n"
                    "**How it works:**\n"
                    "- Find a time to chat (voice call, coffee, whatever works)\n"
                    "- React with :white_check_mark: when you've scheduled/completed your 1-1\n"
                    "- React with :x: if you can't make it this week\n\n"
                    "Have a great conversation!"
                )
                msg = await thread.send(message)
                await msg.add_reaction("\u2705")  # white_check_mark
                await msg.add_reaction("\u274c")  # x

                await create_one_on_one_match(guild.id, week_start, user1_id, user2_id, thread.id)
                matches_created += 1

            except discord.Forbidden:
                logging.error(f"Missing permissions to create thread in guild {guild.id}")
            except discord.HTTPException as e:
                logging.error(f"Failed to create match thread: {e}")

        return matches_created

    @tasks.loop(hours=1)
    async def weekly_matching(self):
        """Run matching every Sunday at 10am UTC."""
        now = datetime.now(timezone.utc)
        # Sunday = 6, 10am = hour 10
        if now.weekday() != 6 or now.hour != 10:
            return

        for guild in self.bot.guilds:
            try:
                await self._run_matching_for_guild(guild)
            except Exception as e:
                logging.error(f"Error running matching for guild {guild.id}: {e}")

    @weekly_matching.before_loop
    async def before_weekly_matching(self):
        await self.bot.wait_until_ready()

    @tasks.loop(hours=12)
    async def send_reminders(self):
        """Send reminders for matches that haven't been confirmed."""
        now = datetime.now(timezone.utc)
        # Only send reminders on Tuesday (1) and Thursday (3)
        if now.weekday() not in (1, 3):
            return

        week_start = get_week_start()

        for guild in self.bot.guilds:
            try:
                matches = await get_matches_needing_reminder(guild.id, week_start, max_reminders=2)
                for match in matches:
                    await self._send_reminder(guild, match)
            except Exception as e:
                logging.error(f"Error sending reminders for guild {guild.id}: {e}")

    @send_reminders.before_loop
    async def before_send_reminders(self):
        await self.bot.wait_until_ready()

    async def _send_reminder(self, guild: discord.Guild, match: dict):
        """Send a reminder in the match thread."""
        thread_id = match.get("thread_id")
        if not thread_id:
            return

        try:
            thread = guild.get_thread(thread_id)
            if not thread:
                thread = await self.bot.fetch_channel(thread_id)

            pending_users = []
            if match["user1_status"] == "pending":
                pending_users.append(f"<@{match['user1_id']}>")
            if match["user2_status"] == "pending":
                pending_users.append(f"<@{match['user2_id']}>")

            if pending_users:
                await thread.send(
                    f"Friendly reminder! {' and '.join(pending_users)} - have you had a chance to "
                    "schedule your 1-1? React with :white_check_mark: when done!"
                )
                await increment_match_reminder(match["id"])

        except discord.NotFound:
            logging.warning(f"Thread {thread_id} not found for reminder")
        except discord.Forbidden:
            logging.error(f"Missing permissions to send reminder in thread {thread_id}")
        except Exception as e:
            logging.error(f"Error sending reminder: {e}")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Handle reactions in match threads."""
        if payload.user_id == self.bot.user.id:
            return

        if str(payload.emoji) not in ("\u2705", "\u274c"):
            return

        match = await get_match_by_thread(payload.channel_id)
        if not match:
            return

        if payload.user_id not in (match["user1_id"], match["user2_id"]):
            return

        if str(payload.emoji) == "\u2705":
            await self._handle_confirm(payload, match)
        elif str(payload.emoji) == "\u274c":
            await self._handle_decline(payload, match)

    async def _handle_confirm(self, payload: discord.RawReactionActionEvent, match: dict):
        """Handle a user confirming their 1-1."""
        await update_match_status(match["id"], payload.user_id, "confirmed")

        # Check if both users have confirmed
        updated_match = await get_match_by_id(match["id"])
        if updated_match["user1_status"] == "confirmed" and updated_match["user2_status"] == "confirmed":
            await complete_match(match["id"])

            try:
                channel = self.bot.get_channel(payload.channel_id)
                if channel:
                    await channel.send("Both of you have confirmed - nice work! This 1-1 is complete.")
            except Exception as e:
                logging.error(f"Error sending completion message: {e}")

    async def _handle_decline(self, payload: discord.RawReactionActionEvent, match: dict):
        """Handle a user declining their 1-1."""
        await update_match_status(match["id"], payload.user_id, "declined")
        await complete_match(match["id"])

        # Find the partner
        partner_id = match["user2_id"] if payload.user_id == match["user1_id"] else match["user1_id"]

        try:
            channel = self.bot.get_channel(payload.channel_id)
            if channel:
                await channel.send(
                    f"<@{payload.user_id}> can't make it this week. Looking for a new match for <@{partner_id}>..."
                )
        except Exception as e:
            logging.error(f"Error sending decline message: {e}")

        # Try to find a new match for the partner
        await self._attempt_rematch(payload.guild_id, partner_id)

    async def _attempt_rematch(self, guild_id: int, user_id: int):
        """Try to find a new match for a user whose partner declined."""
        week_start = get_week_start()

        # Get available pool members
        available = await get_available_pool_members(guild_id, week_start)

        # Get who this user has already been paired with
        already_partnered = await get_user_partners_this_week(guild_id, user_id, week_start)
        already_matched = await get_users_matched_this_week(guild_id, week_start)

        # Find candidates: in pool, not skipping, not already matched, not already partnered with this user
        candidates = [
            u for u in available
            if u != user_id and u not in already_partnered and u not in already_matched
        ]

        if not candidates:
            # DM the user that no match was found
            try:
                user = await self.bot.fetch_user(user_id)
                await user.send(
                    "Sorry, we couldn't find a new 1-1 match for you this week after your partner "
                    "declined. You'll be matched again next week!"
                )
            except Exception as e:
                logging.error(f"Failed to DM user {user_id}: {e}")
            return

        # Pick a random candidate and create a new match
        new_partner_id = random.choice(candidates)

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        channel = await self._get_one_on_ones_channel(guild)
        if not channel:
            return

        try:
            thread = await channel.create_thread(
                name=f"1-1: Week of {week_start} (rematch)",
                type=discord.ChannelType.public_thread,
                auto_archive_duration=10080
            )

            message = (
                f"Hey <@{user_id}> and <@{new_partner_id}>! You've been matched for a 1-1 this week.\n\n"
                "(This is a rematch after the original pairing didn't work out.)\n\n"
                "**How it works:**\n"
                "- Find a time to chat (voice call, coffee, whatever works)\n"
                "- React with :white_check_mark: when you've scheduled/completed your 1-1\n"
                "- React with :x: if you can't make it this week\n\n"
                "Have a great conversation!"
            )
            msg = await thread.send(message)
            await msg.add_reaction("\u2705")
            await msg.add_reaction("\u274c")

            await create_one_on_one_match(guild_id, week_start, user_id, new_partner_id, thread.id)

        except Exception as e:
            logging.error(f"Failed to create rematch: {e}")


def setup(bot):
    bot.add_cog(OneOnOnes(bot))
