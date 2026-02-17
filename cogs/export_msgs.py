# ABOUTME: Provides /export-msgs command to export channel messages from a time window as a text file.
# ABOUTME: Parses duration strings like "2h", "3d" and uploads a formatted .txt file to the channel.

import discord
from discord.ext import commands
from discord import option
import logging
import io
from datetime import datetime, timedelta, timezone
from cogs.reminders import parse_time_interval


class ExportMessages(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.slash_command(name="export-msgs", description="Export recent messages from this channel as a text file")
    @option("duration", description="How far back to export (e.g. 2h, 3d, 1w, 1h30m)")
    async def export_msgs(self, ctx: discord.ApplicationContext, duration: str):
        delta = parse_time_interval(duration)
        if not delta:
            await ctx.respond(
                "Invalid duration. Use combinations like: `30m`, `2h`, `3d`, `1w`, `1h30m`",
                ephemeral=True,
            )
            return

        await ctx.defer(ephemeral=True)

        after = datetime.now(timezone.utc) - delta

        try:
            messages = []
            async for message in ctx.channel.history(after=after, oldest_first=True, limit=None):
                messages.append(message)
        except discord.Forbidden:
            await ctx.followup.send("I don't have permission to read this channel's history.", ephemeral=True)
            return

        if not messages:
            await ctx.followup.send(f"No messages found in the last `{duration}`.", ephemeral=True)
            return

        # Format messages
        lines = []
        for msg in messages:
            ts = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            author = msg.author.display_name

            content_parts = []
            if msg.content:
                content_parts.append(msg.content)
            if msg.attachments:
                for a in msg.attachments:
                    content_parts.append(f"[attachment: {a.filename} {a.url}]")
            if msg.embeds:
                for e in msg.embeds:
                    if e.title or e.description:
                        content_parts.append(f"[embed: {e.title or ''} - {e.description or ''}]")

            content = "\n".join(content_parts) if content_parts else "[no content]"
            lines.append(f"[{ts}] {author}: {content}")

        text = "\n\n".join(lines)

        # Build filename
        channel_name = getattr(ctx.channel, "name", "channel")
        start_str = after.strftime("%Y%m%d-%H%M")
        end_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
        filename = f"{channel_name}_{start_str}_to_{end_str}.txt"

        file = discord.File(io.BytesIO(text.encode()), filename=filename)
        await ctx.followup.send(
            f"Exported **{len(messages)}** messages from the last `{duration}`.",
            file=file,
            ephemeral=True,
        )


def setup(bot):
    bot.add_cog(ExportMessages(bot))
