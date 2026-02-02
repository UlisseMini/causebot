import discord
from discord.ext import commands, tasks
from discord import option
import re
import logging
from db.connection import database
from db.actions import get_user_channel, create_user_channel, delete_user_channel, get_welcome_message, set_welcome_message, get_all_user_channels

async def _set_channel_owner_permissions(channel: discord.TextChannel, member: discord.Member):
    """Give the channel owner manage_permissions so they can control who views their channel."""
    await channel.set_permissions(
        member,
        manage_permissions=True,
        reason=f"Personal channel owner permissions for {member}"
    )


def _generate_channel_name(member: discord.Member, category: discord.CategoryChannel) -> str:
    """Generate a unique channel name for a member.
    Tries in order: display name, username, then adds numeric suffix if needed."""
    candidates = [
        member.display_name.lower().replace(" ", "-"),
        member.name.lower().replace(" ", "-"),
    ]

    for candidate in candidates:
        # Clean the name to only include valid characters
        base_name = re.sub(r'[^a-z0-9_-]', '', candidate)
        if not base_name:
            continue

        # Try the base name first
        if not discord.utils.get(category.channels, name=base_name):
            return base_name

        # Try with numeric suffix
        suffix = 1
        while True:
            channel_name = f"{base_name}-{suffix}"
            if not discord.utils.get(category.channels, name=channel_name):
                return channel_name
            suffix += 1

    # Fallback to user ID if all else fails
    return f"user-{member.id}"


class ChannelManagement(commands.Cog):
    channels = discord.SlashCommandGroup("channel", "Personal channel management")

    def _validate_channel_name(self, name: str) -> tuple[bool, str]:
        """Validate channel name according to Discord's specifications.
        Returns (is_valid, error_message)"""
        # Check length (1-100 characters)
        if len(name) < 1:
            return False, "Channel name cannot be empty."
        if len(name) > 100:
            return False, "Channel name must be 100 characters or less."
        
        # Discord automatically converts channel names to lowercase and replaces spaces with hyphens
        # But we should validate that the resulting name would be valid
        normalized = name.lower().replace(" ", "-")
        
        # Check for invalid characters (only lowercase letters, numbers, hyphens, and underscores allowed)
        if not re.match(r'^[a-z0-9_-]+$', normalized):
            return False, "Channel name can only contain letters, numbers, hyphens, and underscores."
        
        # Cannot start or end with hyphen or underscore
        if normalized.startswith(("-", "_")) or normalized.endswith(("-", "_")):
            return False, "Channel name cannot start or end with a hyphen or underscore."
        
        return True, ""
    
    @channels.command(description="Give yourself a personal channel")
    @option("name", description="Name of the channel")
    async def add(self, ctx, name: str):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return
        
        # Validate channel name
        is_valid, error_message = self._validate_channel_name(name)
        if not is_valid:
            await ctx.respond(f"Invalid channel name: {error_message}", ephemeral=True)
            return
        
        try:
            # Check if user already has a personal channel in this guild
            existing_channel_id = await get_user_channel(guild.id, ctx.author.id)
            if existing_channel_id:
                existing_channel = guild.get_channel(existing_channel_id)
                if existing_channel:
                    await ctx.respond(
                        f"You already have a personal channel in this server: {existing_channel.mention}",
                        ephemeral=True
                    )
                    return
                # Channel was deleted but record still exists - clear the database entry
                await delete_user_channel(guild.id, ctx.author.id)
            
            # Find or create the "Personal Channels" category
            category = discord.utils.get(guild.categories, name="Personal Channels")
            if not category:
                category = await guild.create_category("Personal Channels")
            
            # Check if channel already exists in this category
            existing_channel = discord.utils.get(guild.channels, name=name.lower().replace(" ", "-"), category=category)
            if existing_channel:
                await ctx.respond(f"Channel `{name}` already exists in the Personal Channels category.", ephemeral=True)
                return
            
            # Create the channel
            channel = await guild.create_text_channel(name, category=category)

            # Give the user manage_permissions so they can control who views their channel
            await _set_channel_owner_permissions(channel, ctx.author)

            # Store the channel in the database
            await create_user_channel(
                guild_id=guild.id,
                user_id=ctx.author.id,
                channel_id=channel.id,
                username=str(ctx.author),
                guild_name=guild.name
            )

            await ctx.respond(f"Your personal channel is available at {channel.mention}", ephemeral=True)
        except discord.Forbidden:
            await ctx.respond("I don't have permission to create channels. Please check my permissions.", ephemeral=True)
        except discord.HTTPException as e:
            error_msg = e.text if hasattr(e, 'text') else str(e)
            logging.error(f"Failed to create channel '{name}' in guild {guild.id}: {error_msg}")
            await ctx.respond("Failed to create channel. Please try again later.", ephemeral=True)
        except Exception as e:
            logging.error(f"Unexpected error creating channel '{name}' in guild {guild.id}: {str(e)}")
            await ctx.respond("An unexpected error occurred. Please try again later.", ephemeral=True)
    
    @channels.command(description="Rename your personal channel")
    @option("name", description="New name for the channel")
    async def rename(self, ctx, name: str):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        # Validate channel name
        is_valid, error_message = self._validate_channel_name(name)
        if not is_valid:
            await ctx.respond(f"Invalid channel name: {error_message}", ephemeral=True)
            return

        try:
            # Check if user has a personal channel in this guild
            existing_channel_id = await get_user_channel(guild.id, ctx.author.id)
            if not existing_channel_id:
                await ctx.respond("You don't have a personal channel in this server.", ephemeral=True)
                return

            # Get the channel object
            existing_channel = guild.get_channel(existing_channel_id)
            if not existing_channel:
                # Channel was deleted but record still exists - clear the database entry
                await delete_user_channel(guild.id, ctx.author.id)
                await ctx.respond("Your personal channel was deleted. Please create a new one.", ephemeral=True)
                return

            # Rename the channel
            await existing_channel.edit(name=name)

            await ctx.respond(f"Your personal channel has been renamed to {existing_channel.mention}", ephemeral=True)
        except discord.Forbidden:
            await ctx.respond("I don't have permission to edit channels. Please check my permissions.", ephemeral=True)
        except discord.HTTPException as e:
            error_msg = e.text if hasattr(e, 'text') else str(e)
            logging.error(f"Failed to rename channel in guild {guild.id}: {error_msg}")
            await ctx.respond("Failed to rename channel. Please try again later.", ephemeral=True)
        except Exception as e:
            logging.error(f"Unexpected error renaming channel in guild {guild.id}: {str(e)}")
            await ctx.respond("An unexpected error occurred. Please try again later.", ephemeral=True)

    @channels.command(description="[Admin] Set an existing channel as a user's personal channel")
    @discord.default_permissions(administrator=True)
    @option("user", description="The user to assign the channel to")
    @option("channel", description="The channel to assign")
    async def set(self, ctx, user: discord.Member, channel: discord.TextChannel):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        try:
            # Check if the user already has a personal channel
            existing_channel_id = await get_user_channel(guild.id, user.id)
            if existing_channel_id:
                existing_channel = guild.get_channel(existing_channel_id)
                if existing_channel:
                    await ctx.respond(
                        f"{user.mention} already has a personal channel: {existing_channel.mention}\n"
                        f"Please delete their existing channel first before assigning a new one.",
                        ephemeral=True
                    )
                    return
                # Channel was deleted but record still exists - clear the database entry
                await delete_user_channel(guild.id, user.id)

            # Check if the channel is already assigned to someone else
            query = "SELECT user_id FROM user_private_channels WHERE guild_id = :guild_id AND channel_id = :channel_id"
            result = await database.fetch_one(query=query, values={"guild_id": guild.id, "channel_id": channel.id})
            if result:
                other_user_id = result["user_id"]
                other_user = guild.get_member(other_user_id)
                user_mention = other_user.mention if other_user else f"User ID {other_user_id}"
                await ctx.respond(
                    f"{channel.mention} is already assigned to {user_mention}\n"
                    f"Please unassign it first or choose a different channel.",
                    ephemeral=True
                )
                return

            # Give the user manage_permissions so they can control who views their channel
            await _set_channel_owner_permissions(channel, user)

            # Store the channel in the database
            await create_user_channel(
                guild_id=guild.id,
                user_id=user.id,
                channel_id=channel.id,
                username=str(user),
                guild_name=guild.name
            )

            await ctx.respond(f"Successfully assigned {channel.mention} as {user.mention}'s personal channel.", ephemeral=True)
        except discord.Forbidden:
            await ctx.respond("I don't have permission to manage channels. Please check my permissions.", ephemeral=True)
        except discord.HTTPException as e:
            error_msg = e.text if hasattr(e, 'text') else str(e)
            logging.error(f"Failed to set channel for user {user.id} in guild {guild.id}: {error_msg}")
            await ctx.respond("Failed to set channel. Please try again later.", ephemeral=True)
        except Exception as e:
            logging.error(f"Unexpected error setting channel for user {user.id} in guild {guild.id}: {str(e)}")
            await ctx.respond("An unexpected error occurred. Please try again later.", ephemeral=True)

    @channels.command(description="[Admin] Set welcome message by copying from an existing message")
    @discord.default_permissions(administrator=True)
    @option("message_link", description="Link to the message to copy (right-click message -> Copy Message Link)")
    async def welcome(self, ctx, message_link: str):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        try:
            # Parse message link: https://discord.com/channels/GUILD_ID/CHANNEL_ID/MESSAGE_ID
            parts = message_link.strip().split("/")
            if len(parts) < 3:
                await ctx.respond("Invalid message link. Right-click a message and select 'Copy Message Link'.", ephemeral=True)
                return

            try:
                channel_id = int(parts[-2])
                message_id = int(parts[-1])
            except ValueError:
                await ctx.respond("Invalid message link. Right-click a message and select 'Copy Message Link'.", ephemeral=True)
                return

            # Fetch the message
            channel = guild.get_channel(channel_id)
            if not channel:
                await ctx.respond("Could not find that channel. Make sure the message is in this server.", ephemeral=True)
                return

            try:
                source_message = await channel.fetch_message(message_id)
            except discord.NotFound:
                await ctx.respond("Could not find that message. It may have been deleted.", ephemeral=True)
                return

            if not source_message.content:
                await ctx.respond("That message has no text content.", ephemeral=True)
                return

            # Store the welcome message
            await set_welcome_message(guild.id, source_message.content, guild.name)

            # Show a preview with example substitutions
            preview = source_message.content.replace("{name}", ctx.author.mention).replace("{channel}", "#example-channel")
            await ctx.respond(
                f"Welcome message updated!\n\n**Preview:**\n{preview}",
                ephemeral=True
            )
        except discord.Forbidden:
            await ctx.respond("I don't have permission to read that message.", ephemeral=True)
        except discord.HTTPException as e:
            error_msg = e.text if hasattr(e, 'text') else str(e)
            logging.error(f"Failed to fetch message for welcome in guild {guild.id}: {error_msg}")
            await ctx.respond("Failed to fetch message. Please try again later.", ephemeral=True)
        except Exception as e:
            logging.error(f"Unexpected error setting welcome message in guild {guild.id}: {str(e)}")
            await ctx.respond("An unexpected error occurred. Please try again later.", ephemeral=True)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Create a personal channel for new members and send welcome message."""
        # Ignore bots
        if member.bot:
            return

        guild = member.guild

        try:
            # Check if user already has a personal channel
            existing_channel_id = await get_user_channel(guild.id, member.id)
            if existing_channel_id:
                existing_channel = guild.get_channel(existing_channel_id)
                if existing_channel:
                    return
                # Channel was deleted but record still exists - clear the database entry
                await delete_user_channel(guild.id, member.id)

            # Find or create the "Personal Channels" category
            category = discord.utils.get(guild.categories, name="Personal Channels")
            if not category:
                category = await guild.create_category("Personal Channels")

            # Generate channel name
            channel_name = _generate_channel_name(member, category)

            # Create the channel
            channel = await guild.create_text_channel(channel_name, category=category)

            # Give the user manage_permissions so they can control who views their channel
            await _set_channel_owner_permissions(channel, member)

            # Store the channel in the database
            await create_user_channel(
                guild_id=guild.id,
                user_id=member.id,
                channel_id=channel.id,
                username=str(member),
                guild_name=guild.name
            )

            # Send welcome message if configured
            welcome_template = await get_welcome_message(guild.id)
            if welcome_template:
                welcome_msg = welcome_template.replace("{name}", member.mention).replace("{channel}", channel.mention)
                await channel.send(welcome_msg)

            # Give them the active journaling role
            from cogs.roles import get_or_create_active_role
            role = await get_or_create_active_role(guild)
            if role:
                try:
                    await member.add_roles(role, reason="New member with personal channel")
                except discord.Forbidden:
                    logging.error(f"Missing permissions to add role to user {member.id} in guild {guild.id}")
                except discord.HTTPException as e:
                    logging.error(f"Failed to add role to user {member.id} in guild {guild.id}: {str(e)}")

        except discord.Forbidden:
            logging.error(f"Missing permissions to create channel for {member.id} in guild {guild.id}")
        except discord.HTTPException as e:
            error_msg = e.text if hasattr(e, 'text') else str(e)
            logging.error(f"Failed to create channel for new member {member.id} in guild {guild.id}: {error_msg}")
        except Exception as e:
            logging.error(f"Unexpected error creating channel for new member {member.id} in guild {guild.id}: {str(e)}")

    def __init__(self, bot):
        self.bot = bot
        self.sync_channel_permissions.start()

    def cog_unload(self):
        self.sync_channel_permissions.cancel()

    @tasks.loop(hours=1)
    async def sync_channel_permissions(self):
        """Periodically sync permissions for all personal channels."""
        for guild in self.bot.guilds:
            try:
                await self._sync_guild_channel_permissions(guild)
            except Exception as e:
                logging.error(f"Error syncing channel permissions for guild {guild.id}: {e}")

    @sync_channel_permissions.before_loop
    async def before_sync_channel_permissions(self):
        await self.bot.wait_until_ready()

    async def _sync_guild_channel_permissions(self, guild: discord.Guild):
        """Sync permissions for all personal channels in a guild."""
        user_channels = await get_all_user_channels(guild.id)
        for mapping in user_channels:
            user_id = mapping["user_id"]
            channel_id = mapping["channel_id"]

            channel = guild.get_channel(channel_id)
            if not channel:
                # Channel was deleted, clean up DB record
                await delete_user_channel(guild.id, user_id)
                continue

            member = guild.get_member(user_id)
            if not member:
                # User left the server, skip (don't delete - they might rejoin)
                continue

            # Check if user already has manage_permissions
            overwrites = channel.overwrites_for(member)
            if overwrites.manage_permissions is not True:
                try:
                    await _set_channel_owner_permissions(channel, member)
                    logging.info(f"Synced permissions for {member} on channel {channel.name}")
                except discord.Forbidden:
                    logging.error(f"Missing permissions to set channel perms for {member.id} in {guild.id}")
                except discord.HTTPException as e:
                    logging.error(f"Failed to set channel perms for {member.id} in {guild.id}: {e}")

    @channels.command(description="[Admin] Sync permissions for all personal channels")
    @discord.default_permissions(administrator=True)
    async def sync_permissions(self, ctx):
        """Manually trigger permission sync for all personal channels."""
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)
        try:
            await self._sync_guild_channel_permissions(guild)
            await ctx.respond("Successfully synced permissions for all personal channels.", ephemeral=True)
        except Exception as e:
            logging.error(f"Error in manual permission sync for guild {guild.id}: {e}")
            await ctx.respond("An error occurred while syncing permissions.", ephemeral=True)

    @channels.command(description="[Admin] Discover and link unlinked personal channels")
    @discord.default_permissions(administrator=True)
    async def discover(self, ctx):
        """Find channels in Personal Channels category that aren't linked to users and try to match them using Claude AI."""
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)

        category = discord.utils.get(guild.categories, name="Personal Channels")
        if not category:
            await ctx.respond("No 'Personal Channels' category found.", ephemeral=True)
            return

        # Get all currently linked channel IDs
        linked_channels = await get_all_user_channels(guild.id)
        linked_channel_ids = {m["channel_id"] for m in linked_channels}
        users_with_channels = {m["user_id"] for m in linked_channels}

        # Find unlinked channels
        unlinked = [ch for ch in category.channels if isinstance(ch, discord.TextChannel) and ch.id not in linked_channel_ids]

        if not unlinked:
            await ctx.respond("All channels in Personal Channels are already linked.", ephemeral=True)
            return

        # Get members without channels
        available_members = [m for m in guild.members if not m.bot and m.id not in users_with_channels]

        if not available_members:
            await ctx.respond(f"Found {len(unlinked)} unlinked channels but all members already have channels assigned.", ephemeral=True)
            return

        # Use Claude to match channels to members
        from cogs.claude import get_client
        client = get_client()
        if not client:
            await ctx.followup.send("Claude AI not configured. Set ANTHROPIC_API_KEY to use smart matching.", ephemeral=True)
            return

        # Build the prompt
        channels_list = "\n".join([f"- {ch.name}" for ch in unlinked])
        members_list = "\n".join([f"- {m.display_name} (username: {m.name})" for m in available_members])

        prompt = f"""Match these Discord channel names to member names. Channels are typically named after the person who owns them (with spaces replaced by hyphens).

UNLINKED CHANNELS:
{channels_list}

AVAILABLE MEMBERS (display name + username):
{members_list}

Return ONLY a JSON array of matches. Each match should be:
{{"channel": "channel-name", "member_username": "username", "confidence": "high" or "medium"}}

Only include matches you're reasonably confident about. Use "high" for exact/obvious matches, "medium" for likely but not certain matches. Omit channels you can't match.

Return valid JSON only, no explanation."""

        try:
            response = client.messages.create(
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
                model="claude-sonnet-4-5",
            )
            response_text = response.content[0].text.strip()

            # Parse JSON response
            import json
            # Handle markdown code blocks
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
            matches_data = json.loads(response_text)

        except Exception as e:
            logging.error(f"Claude matching failed: {e}")
            await ctx.followup.send(f"Claude matching failed: {e}", ephemeral=True)
            return

        # Convert to actual objects
        matched = []
        channel_map = {ch.name: ch for ch in unlinked}
        member_map = {m.name: m for m in available_members}

        for match in matches_data:
            ch_name = match.get("channel")
            member_username = match.get("member_username")
            confidence = match.get("confidence", "medium")

            channel = channel_map.get(ch_name)
            member = member_map.get(member_username)

            if channel and member:
                matched.append((channel, member, confidence))

        if not matched:
            await ctx.followup.send("Claude couldn't find any confident matches.", ephemeral=True)
            return

        # Build response with buttons
        lines = [f"**Found {len(matched)} matches:**"]
        for ch, member, conf in matched:
            conf_emoji = "✓" if conf == "high" else "?"
            lines.append(f"{conf_emoji} {ch.mention} → {member.mention}")

        unmatched_channels = [ch for ch in unlinked if ch not in [m[0] for m in matched]]
        if unmatched_channels:
            lines.append(f"\n**{len(unmatched_channels)} channels couldn't be matched:**")
            for ch in unmatched_channels[:5]:
                lines.append(f"• {ch.mention}")
            if len(unmatched_channels) > 5:
                lines.append(f"• ... and {len(unmatched_channels) - 5} more")

        # Create confirmation view with buttons
        view = DiscoverConfirmView(matched, guild, ctx.author.id)
        await ctx.followup.send("\n".join(lines), view=view, ephemeral=True)


class DiscoverConfirmView(discord.ui.View):
    def __init__(self, matched: list, guild: discord.Guild, author_id: int):
        super().__init__(timeout=120)
        self.matched = matched  # list of (channel, member, confidence)
        self.guild = guild
        self.author_id = author_id

    @discord.ui.button(label="Link All", style=discord.ButtonStyle.green)
    async def confirm(self, button: discord.ui.Button, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command invoker can confirm.", ephemeral=True)
            return

        await interaction.response.defer()
        linked_count = 0
        for channel, member, _ in self.matched:
            try:
                await _set_channel_owner_permissions(channel, member)
                await create_user_channel(
                    guild_id=self.guild.id,
                    user_id=member.id,
                    channel_id=channel.id,
                    username=str(member),
                    guild_name=self.guild.name
                )
                linked_count += 1
            except Exception as e:
                logging.error(f"Failed to link {channel.name} to {member}: {e}")

        self.disable_all_items()
        await interaction.edit_original_response(content=f"Linked {linked_count} channels.", view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, button: discord.ui.Button, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command invoker can cancel.", ephemeral=True)
            return

        self.disable_all_items()
        await interaction.response.edit_message(content="Cancelled.", view=self)
        self.stop()

    def disable_all_items(self):
        for item in self.children:
            item.disabled = True


def setup(bot):
    bot.add_cog(ChannelManagement(bot))