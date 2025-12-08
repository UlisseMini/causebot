import discord
from discord.ext import commands
import random
from db.connection import database
from db.actions import can_award_xp, award_xp, get_user_xp


class XP(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Award XP when a user sends a message."""
        # Ignore bot messages
        if message.author.bot:
            return
        
        # Ignore DMs (only work in guilds)
        if not message.guild:
            return
        
        guild_id = message.guild.id
        user_id = message.author.id
        username = str(message.author)
        guild_name = message.guild.name
        message_content = message.content
        
        # Calculate XP based on rules
        xp_to_award = 0.0
        
        # Rule 1: For at most one message per minute, get random XP between 6 and 10
        if await can_award_xp(guild_id, user_id):
            base_xp = random.uniform(6, 10)
            xp_to_award += base_xp
        
        # Rule 2: For each character above 50, get 0.1 XP
        message_length = len(message_content)
        if message_length > 50:
            extra_chars = message_length - 50
            length_xp = extra_chars * 0.1
            xp_to_award += length_xp
        
        # Award the XP (will be rounded to 3 decimal places in the function)
        await award_xp(
            guild_id=guild_id,
            user_id=user_id,
            xp_amount=xp_to_award,
            username=username,
            guild_name=guild_name
        )
    
    xp = discord.SlashCommandGroup("xp", "XP (experience points) management")
    @xp.command(description="View your XP statistics")
    async def stats(self, ctx: discord.ApplicationContext):
        """Display XP statistics for the user."""
        if not ctx.guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return
        
        guild_id = ctx.guild.id
        user_id = ctx.author.id
        
        # Get XP for both time periods
        xp_3d = await get_user_xp(guild_id, user_id, days=3)
        xp_24h = await get_user_xp(guild_id, user_id, days=1)
        
        # Create embed
        embed = discord.Embed(
            title=f"xp statistics for {ctx.author.display_name}",
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )
        
        # embed.set_thumbnail(url=ctx.author.display_avatar.url)
        
        embed.add_field(
            name="Last 24 hours",
            value=f"{xp_24h:.1f} XP",
            inline=True
        )
        
        embed.add_field(
            name="Last 3 days",
            value=f"{xp_3d:.1f} XP",
            inline=True
        )
        
        await ctx.respond(embed=embed)


def setup(bot):
    bot.add_cog(XP(bot))

