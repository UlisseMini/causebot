import discord
from discord.ext import commands, tasks
from discord import option
import logging
from datetime import datetime, timedelta, timezone
from db.actions import get_active_role_id, set_active_role_id, get_all_user_channels, get_active_days, set_active_days


async def get_or_create_active_role(guild: discord.Guild) -> discord.Role | None:
    """Get the active journaling role for a guild, creating it if it doesn't exist.
    Returns None if unable to get or create the role."""
    role_id = await get_active_role_id(guild.id)
    role = None

    if role_id:
        role = guild.get_role(role_id)

    if not role:
        # Role doesn't exist - create it
        try:
            role = await guild.create_role(
                name="Active Journaling",
                color=discord.Color.green(),
                hoist=True,
                reason="Created by bot for active journaling members"
            )
            await set_active_role_id(guild.id, role.id, guild.name)
            logging.info(f"Created Active Journaling role in guild {guild.id}")
        except discord.Forbidden:
            logging.error(f"Missing permissions to create role in guild {guild.id}")
            return None
        except discord.HTTPException as e:
            logging.error(f"Failed to create role in guild {guild.id}: {str(e)}")
            return None

    return role


async def get_last_message_time(channel: discord.TextChannel, user_id: int) -> datetime | None:
    """Get the timestamp of the user's most recent message in the channel."""
    try:
        async for message in channel.history(limit=100):
            if message.author.id == user_id:
                return message.created_at
    except discord.Forbidden:
        logging.error(f"Missing permissions to read history in channel {channel.id}")
    except discord.HTTPException as e:
        logging.error(f"Failed to read channel history {channel.id}: {e}")
    return None


class RoleManagement(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.update_active_roles.start()

    def cog_unload(self):
        self.update_active_roles.cancel()

    @tasks.loop(hours=1)
    async def update_active_roles(self):
        """Periodically check and update active journaling roles for all guilds."""
        for guild in self.bot.guilds:
            try:
                await self._update_guild_active_roles(guild)
            except Exception as e:
                logging.error(f"Error updating active roles for guild {guild.id}: {str(e)}")

    @update_active_roles.before_loop
    async def before_update_active_roles(self):
        """Wait until the bot is ready before starting the loop."""
        await self.bot.wait_until_ready()

    async def _update_guild_active_roles(self, guild: discord.Guild, notify: bool = True):
        """Update active roles for a specific guild based on actual channel activity."""
        # Get or create the active role
        role = await get_or_create_active_role(guild)
        if not role:
            return

        # Get settings
        active_days = await get_active_days(guild.id)
        cutoff = datetime.now(timezone.utc) - timedelta(days=active_days)

        # Get all user channels
        user_channels = await get_all_user_channels(guild.id)
        channel_map = {uc["user_id"]: uc["channel_id"] for uc in user_channels}

        # Check each member
        for member in guild.members:
            if member.bot:
                continue

            has_role = role in member.roles
            channel_id = channel_map.get(member.id)

            # If no personal channel, they shouldn't have the role
            if not channel_id:
                if has_role:
                    try:
                        await member.remove_roles(role, reason="No personal channel assigned")
                    except (discord.Forbidden, discord.HTTPException) as e:
                        logging.error(f"Failed to remove role from {member.id}: {e}")
                continue

            channel = guild.get_channel(channel_id)
            if not channel:
                # Channel was deleted
                if has_role:
                    try:
                        await member.remove_roles(role, reason="Personal channel deleted")
                    except (discord.Forbidden, discord.HTTPException) as e:
                        logging.error(f"Failed to remove role from {member.id}: {e}")
                continue

            # Check last message time
            last_msg_time = await get_last_message_time(channel, member.id)
            is_active = last_msg_time is not None and last_msg_time > cutoff

            try:
                if is_active and not has_role:
                    # Add role
                    await member.add_roles(role, reason=f"Active journaling in last {active_days} days")
                elif not is_active and has_role:
                    # Remove role and notify
                    await member.remove_roles(role, reason=f"No journaling in last {active_days} days")
                    if notify:
                        await self._notify_role_removed(member, channel, active_days)
            except discord.Forbidden:
                logging.error(f"Missing permissions to manage roles for user {member.id} in guild {guild.id}")
            except discord.HTTPException as e:
                logging.error(f"Failed to update role for user {member.id} in guild {guild.id}: {str(e)}")

    async def _notify_role_removed(self, member: discord.Member, channel: discord.TextChannel, days: int):
        """Notify a user that their active journaling role was removed."""
        try:
            await member.send(
                f"Hey {member.display_name}! You've lost your **Active Journaling** role in **{member.guild.name}** "
                f"because you haven't posted in your personal channel ({channel.mention}) in the last {days} days.\n\n"
                f"Post in your channel to get it back!"
            )
        except discord.Forbidden:
            # User has DMs disabled, try posting in their channel instead
            try:
                await channel.send(
                    f"{member.mention} You've lost your **Active Journaling** role because you haven't posted here "
                    f"in the last {days} days. Post a message to get it back!"
                )
            except (discord.Forbidden, discord.HTTPException):
                pass
        except discord.HTTPException as e:
            logging.error(f"Failed to notify user {member.id} about role removal: {e}")

    roles = discord.SlashCommandGroup("roles", "Role management")

    @roles.command(description="[Admin] Manually trigger active role check")
    @discord.default_permissions(administrator=True)
    async def check_active(self, ctx: discord.ApplicationContext):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)

        try:
            await self._update_guild_active_roles(guild, notify=False)
            await ctx.followup.send("Active roles updated successfully!", ephemeral=True)
        except Exception as e:
            logging.error(f"Error manually updating active roles for guild {guild.id}: {str(e)}")
            await ctx.followup.send("An error occurred while updating roles.", ephemeral=True)

    @roles.command(description="[Admin] Set which role to use for active journaling")
    @discord.default_permissions(administrator=True)
    @option("role", description="The role to assign to active journalers")
    async def set_active_role(self, ctx: discord.ApplicationContext, role: discord.Role):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        try:
            await set_active_role_id(guild.id, role.id, guild.name)
            await ctx.respond(f"Active journaling role set to {role.mention}", ephemeral=True)
        except Exception as e:
            logging.error(f"Error setting active role for guild {guild.id}: {str(e)}")
            await ctx.respond("An error occurred while setting the role.", ephemeral=True)

    @roles.command(description="[Admin] Set how many days of inactivity before losing the role")
    @discord.default_permissions(administrator=True)
    @option("days", description="Number of days (1-30)", min_value=1, max_value=30)
    async def set_active_days(self, ctx: discord.ApplicationContext, days: int):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        try:
            await set_active_days(guild.id, days, guild.name)
            await ctx.respond(f"Users will lose Active Journaling role after {days} days of inactivity.", ephemeral=True)
        except Exception as e:
            logging.error(f"Error setting active days for guild {guild.id}: {str(e)}")
            await ctx.respond("An error occurred while setting active days.", ephemeral=True)


def setup(bot):
    bot.add_cog(RoleManagement(bot))
