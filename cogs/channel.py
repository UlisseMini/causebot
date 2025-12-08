import discord
from discord.ext import commands
from discord import option
import re
import logging
from db.connection import database
from db.actions import get_user_channel, create_user_channel, delete_user_channel, get_welcome_message, set_welcome_message

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

    @channels.command(description="[Admin] Set the welcome message for new members")
    @discord.default_permissions(administrator=True)
    @option("message", description="Welcome message template. Use {name} for username, {channel} for their channel")
    async def welcome(self, ctx, message: str):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        try:
            await set_welcome_message(guild.id, message, guild.name)

            # Show a preview with example substitutions
            preview = message.replace("{name}", ctx.author.mention).replace("{channel}", "#example-channel")
            await ctx.respond(
                f"Welcome message updated!\n\n**Preview:**\n{preview}",
                ephemeral=True
            )
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

            # Generate channel name with fallback
            base_name = member.name.lower().replace(" ", "-")
            # Clean the name to only include valid characters
            base_name = re.sub(r'[^a-z0-9_-]', '', base_name)
            if not base_name:
                base_name = f"user-{member.id}"

            channel_name = base_name
            suffix = 1
            while discord.utils.get(guild.channels, name=channel_name, category=category):
                channel_name = f"{base_name}-{suffix}"
                suffix += 1

            # Create the channel
            channel = await guild.create_text_channel(channel_name, category=category)

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

        except discord.Forbidden:
            logging.error(f"Missing permissions to create channel for {member.id} in guild {guild.id}")
        except discord.HTTPException as e:
            error_msg = e.text if hasattr(e, 'text') else str(e)
            logging.error(f"Failed to create channel for new member {member.id} in guild {guild.id}: {error_msg}")
        except Exception as e:
            logging.error(f"Unexpected error creating channel for new member {member.id} in guild {guild.id}: {str(e)}")

    def __init__(self, bot):
        self.bot = bot
    
    

def setup(bot):
    bot.add_cog(ChannelManagement(bot))