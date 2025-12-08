import os
import time
import asyncio
import discord
from dotenv import load_dotenv
# Create bot instance with intents
intents = discord.Intents.default()
intents.message_content = True  # Required to read message content for XP system
bot = discord.Bot(intents=intents)


@bot.event
async def on_ready():
    """Called when the bot successfully connects to Discord"""
    print(f"Bot is online as {bot.user}")


@bot.slash_command(name="ping", description="Check bot latency and response times")
async def ping(ctx: discord.ApplicationContext):
    """Ping command that displays detailed latency information"""
    # Record time when command is received (start of processing)
    start_time = time.perf_counter()

    # Heartbeat latency
    websocket_latency = bot.latency * 1000  # Convert to milliseconds

    # Create embed for detailed latency information
    embed = discord.Embed(
        title="🏓 Pong!",
        color=discord.Color.blue(),
        timestamp=discord.utils.utcnow()
    )

    # Send response and measure bot latency (round-trip time)
    await ctx.respond(embed=embed)

    # Calculate bot latency (time from command received to response sent)
    bot_latency = (time.perf_counter() - start_time) * 1000

    # Update embed with complete latency information
    embed.add_field(
        name="websocket latency",
        value=f"{websocket_latency:.2f} ms",
        inline=True
    )

    embed.add_field(
        name="bot latency",
        value=f"{bot_latency:.2f} ms",
        inline=True
    )

    # Edit the response with complete information
    await ctx.edit(embed=embed)


async def main():
    """Main function to run the bot"""
    print("Starting bot...")

    # Get token from environment variable
    load_dotenv()
    token = os.getenv("DISCORD_BOT_TOKEN")

    if not token:
        raise ValueError(
            "DISCORD_BOT_TOKEN environment variable is not set. "
            "Please set it before running the bot."
        )

    # Initialize database (create tables if they don't exist)
    from db.actions import init_database
    await init_database()
    print("Database initialized")

    # Connect to the database
    from db.connection import database
    await database.connect()

    try:
        # Load all the cogs
        bot.load_extension("cogs.channel")
        bot.load_extension("cogs.xp")
        print("Cogs loaded")

        # Start the bot
        print("Connecting to Discord...")
        await bot.start(token)
    finally:
        # Disconnect from the database when the bot shuts down
        await database.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

