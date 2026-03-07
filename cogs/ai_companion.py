import discord
from discord.ext import commands, tasks
import os
import json
import re
import logging
import aiohttp
from datetime import datetime, timedelta, timezone
from anthropic import Anthropic

from db.actions import (
    get_ai_config,
    upsert_ai_config,
    update_ai_system_prompt,
    get_ai_wakeups,
    set_ai_wakeups,
    get_due_wakeups,
    update_wakeup_next_run,
    get_user_channel,
)


# --- Schedule parsing ---

DAYS_OF_WEEK = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def parse_time(time_str: str) -> tuple[int, int]:
    """Parse HH:MM into (hour, minute)."""
    parts = time_str.split(":")
    return int(parts[0]), int(parts[1])


def compute_next_run(schedule: str, after: datetime | None = None) -> datetime:
    """Compute the next run time for a schedule string.

    Formats:
        daily@HH:MM
        weekly@DAY@HH:MM  (DAY = mon,tue,wed,thu,fri,sat,sun)
        every_Nd@HH:MM
        monthly@D@HH:MM   (D = day of month 1-28)
    """
    if after is None:
        after = datetime.now(timezone.utc)

    parts = schedule.lower().split("@")
    kind = parts[0]

    if kind == "daily":
        hour, minute = parse_time(parts[1])
        candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= after:
            candidate += timedelta(days=1)
        return candidate

    elif kind == "weekly":
        day_name = parts[1]
        hour, minute = parse_time(parts[2])
        target_dow = DAYS_OF_WEEK[day_name]
        candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
        days_ahead = (target_dow - after.weekday()) % 7
        candidate += timedelta(days=days_ahead)
        if candidate <= after:
            candidate += timedelta(weeks=1)
        return candidate

    elif kind.startswith("every_") and kind.endswith("d"):
        n = int(kind[6:-1])
        hour, minute = parse_time(parts[1])
        candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= after:
            candidate += timedelta(days=1)
        # Round up to next multiple of n days from epoch-ish anchor
        return candidate

    elif kind == "monthly":
        day_of_month = int(parts[1])
        hour, minute = parse_time(parts[2])
        candidate = after.replace(day=day_of_month, hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= after:
            # Next month
            if after.month == 12:
                candidate = candidate.replace(year=after.year + 1, month=1)
            else:
                candidate = candidate.replace(month=after.month + 1)
        return candidate

    else:
        raise ValueError(f"Unknown schedule format: {schedule}")


def schedule_to_human(schedule: str) -> str:
    """Convert a schedule string to human-readable text."""
    parts = schedule.lower().split("@")
    kind = parts[0]
    if kind == "daily":
        return f"daily at {parts[1]} UTC"
    elif kind == "weekly":
        return f"weekly on {parts[1].capitalize()} at {parts[2]} UTC"
    elif kind.startswith("every_") and kind.endswith("d"):
        n = kind[6:-1]
        return f"every {n} days at {parts[1]} UTC"
    elif kind == "monthly":
        return f"monthly on day {parts[1]} at {parts[2]} UTC"
    return schedule


def interval_to_timedelta(schedule: str) -> timedelta:
    """Estimate the interval of a schedule as a timedelta (for context window sizing)."""
    parts = schedule.lower().split("@")
    kind = parts[0]
    if kind == "daily":
        return timedelta(days=1)
    elif kind == "weekly":
        return timedelta(weeks=1)
    elif kind.startswith("every_") and kind.endswith("d"):
        n = int(kind[6:-1])
        return timedelta(days=n)
    elif kind == "monthly":
        return timedelta(days=30)
    return timedelta(days=1)


# --- Tool definitions for Claude API ---

TOOLS = [
    {
        "name": "search_channel_history",
        "description": (
            "Search through the user's full channel history for messages matching a text query. "
            "Returns matching messages with surrounding context. Use this when you need to reference "
            "something the user said that isn't in the recent messages provided to you."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Text to search for in message content",
                },
                "before_context": {
                    "type": "integer",
                    "description": "Number of messages to include before each match (default 3)",
                    "default": 3,
                },
                "after_context": {
                    "type": "integer",
                    "description": "Number of messages to include after each match (default 3)",
                    "default": 3,
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of matching messages to return (default 10)",
                    "default": 10,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "set_wakeups",
        "description": (
            "Set ALL scheduled wakeups for this user. This REPLACES the entire wakeup configuration. "
            "Each wakeup triggers you (the AI) at the scheduled time with the given message as context. "
            "You will always see the current full wakeup config in your context, so review it before making changes. "
            "Schedule formats: 'daily@HH:MM', 'weekly@DAY@HH:MM' (mon-sun), 'every_Nd@HH:MM', 'monthly@D@HH:MM'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "wakeups": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {
                                "type": "string",
                                "description": "Short name for this wakeup (e.g. 'morning_checkin')",
                            },
                            "schedule": {
                                "type": "string",
                                "description": "When to trigger (e.g. 'daily@09:00', 'weekly@mon@18:00')",
                            },
                            "message": {
                                "type": "string",
                                "description": "Context message you'll receive when this wakeup fires",
                            },
                        },
                        "required": ["label", "schedule", "message"],
                    },
                },
            },
            "required": ["wakeups"],
        },
    },
    {
        "name": "update_system_prompt",
        "description": (
            "Update your own system prompt. This changes how you behave in future interactions with this user. "
            "Use this when the user asks you to change your personality, approach, or instructions. "
            "Your current system prompt is always shown in your context."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "new_prompt": {
                    "type": "string",
                    "description": "The new system prompt to use for future interactions",
                },
            },
            "required": ["new_prompt"],
        },
    },
]


# --- Default system prompt ---

DEFAULT_SYSTEM_PROMPT = """\
You are a personal self-improvement companion. Your job is to help this person \
stay on track with their goals, reflect on their progress, and maintain good habits.

Be direct and genuine. Reference what they've actually said and done. \
Push back thoughtfully when needed — don't just agree with everything. \
If they set commitments, hold them to it while being understanding about genuine reassessment.

Keep your messages concise. This is Discord, not an essay."""


# --- Anthropic client ---

_client = None


def get_ai_client() -> Anthropic | None:
    global _client
    if _client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return None
        _client = Anthropic(api_key=api_key)
    return _client


# --- Context assembly ---

TEXT_EXTENSIONS = {".txt", ".md", ".py", ".js", ".ts", ".json", ".csv", ".log", ".yml", ".yaml", ".toml", ".cfg", ".ini", ".sh", ".html", ".css", ".xml", ".rs", ".go", ".java", ".c", ".cpp", ".h", ".rb", ".pl"}
MAX_ATTACHMENT_SIZE = 50_000  # 50KB limit per file to avoid blowing up context


async def read_text_attachment(attachment: discord.Attachment) -> str | None:
    """Download and return text content from an attachment, or None if not a text file."""
    ext = os.path.splitext(attachment.filename)[1].lower()
    if ext not in TEXT_EXTENSIONS:
        return None
    if attachment.size > MAX_ATTACHMENT_SIZE:
        return f"[file too large: {attachment.filename} ({attachment.size} bytes)]"
    try:
        content_bytes = await attachment.read()
        return content_bytes.decode("utf-8", errors="replace")
    except Exception as e:
        return f"[error reading {attachment.filename}: {e}]"


async def format_channel_messages(messages: list[discord.Message]) -> str:
    """Format Discord messages into a text block for the AI, including text file contents."""
    lines = []
    for msg in messages:
        ts = msg.created_at.strftime("%Y-%m-%d %H:%M")
        author = msg.author.display_name
        content = msg.content or ""
        if msg.attachments:
            att_parts = []
            for a in msg.attachments:
                file_content = await read_text_attachment(a)
                if file_content is not None:
                    att_parts.append(f"[file: {a.filename}]\n{file_content}\n[/file: {a.filename}]")
                else:
                    att_parts.append(f"[attachment: {a.filename}]")
            content = f"{content}\n{''.join(att_parts)}".strip() if att_parts else content
        if content:
            lines.append(f"[{ts}] {author}: {content}")
    return "\n".join(lines)


def build_wakeup_config_text(wakeups: list[dict]) -> str:
    """Format the full wakeup config for inclusion in context."""
    if not wakeups:
        return "No scheduled wakeups configured."
    lines = ["Current scheduled wakeups:"]
    for w in wakeups:
        status = "enabled" if w.get("enabled", 1) else "disabled"
        human_sched = schedule_to_human(w["schedule"])
        lines.append(f"  - {w['label']}: {human_sched} ({status})")
        if w.get("message"):
            lines.append(f"    Message: {w['message']}")
    return "\n".join(lines)


def build_system_message(user_prompt: str, wakeup_config_text: str) -> str:
    """Build the full system message for the AI."""
    return f"""{user_prompt}

---
CONFIGURATION (always visible to you):

{wakeup_config_text}

---
TOOLS:
You have tools to search the user's channel history for older messages, update your scheduled wakeups, \
and update your own system prompt. Use them when appropriate. When the user asks you to set up \
reminders or check-ins, use set_wakeups. When they ask you to change how you behave, use update_system_prompt.

When using set_wakeups, you REPLACE all wakeups. So always include existing wakeups you want to keep \
alongside any new ones you're adding. Review the current config above before making changes."""


# --- Tool execution ---

async def _message_full_text(msg: discord.Message) -> str:
    """Get full searchable text for a message, including text file attachments."""
    parts = []
    if msg.content:
        parts.append(msg.content)
    for a in msg.attachments:
        file_content = await read_text_attachment(a)
        if file_content is not None:
            parts.append(file_content)
    return "\n".join(parts)


async def _format_search_message(msg: discord.Message, marker: str = "") -> str:
    """Format a single message for search results, including file contents."""
    ts = msg.created_at.strftime("%Y-%m-%d %H:%M")
    content = msg.content or "[no text]"
    att_parts = []
    for a in msg.attachments:
        file_content = await read_text_attachment(a)
        if file_content is not None:
            att_parts.append(f"[file: {a.filename}]\n{file_content}\n[/file: {a.filename}]")
        else:
            att_parts.append(f"[attachment: {a.filename}]")
    if att_parts:
        content = f"{content}\n{''.join(att_parts)}"
    return f"[{ts}] {msg.author.display_name}: {content}{marker}"


async def execute_search(channel: discord.TextChannel, query: str,
                         before_context: int = 3, after_context: int = 3,
                         max_results: int = 10) -> str:
    """Search channel history for messages matching query, with surrounding context."""
    query_lower = query.lower()
    all_messages = []
    try:
        async for msg in channel.history(limit=2000):
            all_messages.append(msg)
    except discord.Forbidden:
        return "Error: no permission to read channel history."
    except discord.HTTPException as e:
        return f"Error reading channel history: {e}"

    # Reverse to chronological order
    all_messages.reverse()

    # Find matching indices — search message text AND file contents
    match_indices = []
    for i, msg in enumerate(all_messages):
        full_text = await _message_full_text(msg)
        if query_lower in full_text.lower():
            match_indices.append(i)
            if len(match_indices) >= max_results:
                break

    if not match_indices:
        return f"No messages found matching '{query}'."

    # Build results with context
    results = []
    seen = set()
    for idx in match_indices:
        start = max(0, idx - before_context)
        end = min(len(all_messages), idx + after_context + 1)
        group = []
        for i in range(start, end):
            if i not in seen:
                seen.add(i)
                marker = " <<< MATCH" if i == idx else ""
                line = await _format_search_message(all_messages[i], marker)
                group.append(line)
        if group:
            results.append("\n".join(group))

    return f"Found {len(match_indices)} match(es) for '{query}':\n\n" + "\n---\n".join(results)


# --- Main AI runner ---

async def run_ai(guild: discord.Guild, user_id: int, channel: discord.TextChannel,
                 trigger_info: str, context_window: timedelta | None = None):
    """Run the AI companion for a user.

    Args:
        guild: The Discord guild
        user_id: The user's ID
        channel: The channel to read from and post to
        trigger_info: Description of why the AI is running (shown in context)
        context_window: How far back to fetch messages (default: 2 days)
    """
    client = get_ai_client()
    if not client:
        logging.error("AI companion: ANTHROPIC_API_KEY not set")
        return

    config = await get_ai_config(guild.id, user_id)
    if not config:
        logging.error(f"AI companion: no config for user {user_id} in guild {guild.id}")
        return

    if not config["enabled"]:
        return

    system_prompt = config["system_prompt"] or DEFAULT_SYSTEM_PROMPT
    wakeups = await get_ai_wakeups(guild.id, user_id)
    wakeup_config_text = build_wakeup_config_text(wakeups)

    # Fetch channel history
    if context_window is None:
        context_window = timedelta(days=2)
    after_time = datetime.now(timezone.utc) - context_window

    history_messages = []
    try:
        async for msg in channel.history(after=after_time, oldest_first=True, limit=200):
            history_messages.append(msg)
    except (discord.Forbidden, discord.HTTPException) as e:
        logging.error(f"AI companion: failed to fetch history for channel {channel.id}: {e}")

    history_text = (await format_channel_messages(history_messages)) if history_messages else "(No recent messages)"

    system = build_system_message(system_prompt, wakeup_config_text)

    user_message = f"""TRIGGER: {trigger_info}

RECENT CHANNEL MESSAGES:
{history_text}"""

    messages = [{"role": "user", "content": user_message}]

    # Tool use loop
    try:
        while True:
            response = client.messages.create(
                model="claude-opus-4-6",
                max_tokens=1500,
                system=system,
                messages=messages,
                tools=TOOLS,
            )

            # Check for tool use
            tool_uses = [b for b in response.content if b.type == "tool_use"]

            if not tool_uses:
                # No tool calls — extract text and send
                text_blocks = [b.text for b in response.content if b.type == "text"]
                final_text = "\n".join(text_blocks).strip()
                if final_text:
                    # Split if over Discord limit
                    for chunk in _split_message(final_text):
                        await channel.send(chunk)
                break

            # Process tool calls
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for tool_use in tool_uses:
                result = await _handle_tool_call(
                    tool_use.name, tool_use.input,
                    guild, user_id, channel, wakeups
                )
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use.id,
                    "content": result,
                })

            messages.append({"role": "user", "content": tool_results})

            # Also send any text that came with the tool calls
            text_blocks = [b.text for b in response.content if b.type == "text"]
            interim_text = "\n".join(text_blocks).strip()
            if interim_text:
                for chunk in _split_message(interim_text):
                    await channel.send(chunk)

            # Safety: limit tool call rounds
            if len(messages) > 12:
                logging.warning(f"AI companion: too many tool rounds for user {user_id}")
                break

    except Exception as e:
        logging.error(f"AI companion error for user {user_id} in guild {guild.id}: {e}")


async def _handle_tool_call(name: str, input_data: dict,
                            guild: discord.Guild, user_id: int,
                            channel: discord.TextChannel,
                            current_wakeups: list[dict]) -> str:
    """Execute a tool call and return the result string."""
    try:
        if name == "search_channel_history":
            return await execute_search(
                channel,
                input_data["query"],
                before_context=input_data.get("before_context", 3),
                after_context=input_data.get("after_context", 3),
                max_results=input_data.get("max_results", 10),
            )

        elif name == "set_wakeups":
            wakeups_data = input_data["wakeups"]
            # Compute next_run_at for each
            now = datetime.now(timezone.utc)
            wakeups_to_store = []
            for w in wakeups_data:
                try:
                    next_run = compute_next_run(w["schedule"], after=now)
                    wakeups_to_store.append({
                        "label": w["label"],
                        "schedule": w["schedule"],
                        "message": w.get("message", ""),
                        "next_run_at": next_run.isoformat(),
                    })
                except ValueError as e:
                    return f"Error with schedule '{w['schedule']}': {e}"

            await set_ai_wakeups(guild.id, user_id, wakeups_to_store)

            # Format confirmation
            lines = ["Wakeups updated:"]
            for w in wakeups_to_store:
                lines.append(f"  - {w['label']}: {schedule_to_human(w['schedule'])} (next: {w['next_run_at'][:16]})")
            return "\n".join(lines)

        elif name == "update_system_prompt":
            new_prompt = input_data["new_prompt"]
            await update_ai_system_prompt(guild.id, user_id, new_prompt)
            return "System prompt updated successfully."

        else:
            return f"Unknown tool: {name}"

    except Exception as e:
        logging.error(f"AI companion tool error ({name}): {e}")
        return f"Error executing {name}: {e}"


def _split_message(text: str, limit: int = 2000) -> list[str]:
    """Split text into chunks that fit Discord's message limit."""
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        # Try to split at a newline
        split_at = text.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = limit
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


# --- The Cog ---

class AICompanion(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_wakeups.start()
        # Track bot message IDs so we know when users reply to us
        self._bot_message_ids: set[int] = set()

    def cog_unload(self):
        self.check_wakeups.cancel()

    @tasks.loop(seconds=60)
    async def check_wakeups(self):
        """Check for due wakeups and fire them."""
        try:
            due = await get_due_wakeups()
            for wakeup in due:
                await self._fire_wakeup(wakeup)
        except Exception as e:
            logging.error(f"AI companion wakeup loop error: {e}")

    @check_wakeups.before_loop
    async def before_check_wakeups(self):
        await self.bot.wait_until_ready()

    async def _fire_wakeup(self, wakeup: dict):
        """Fire a single wakeup: run the AI and reschedule."""
        guild_id = wakeup["guild_id"]
        user_id = wakeup["user_id"]

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        # Find user's personal channel
        channel_id = await get_user_channel(guild_id, user_id)
        if not channel_id:
            return
        channel = guild.get_channel(channel_id)
        if not channel:
            return

        # Context window = 2x the wakeup interval
        interval = interval_to_timedelta(wakeup["schedule"])
        context_window = interval * 2
        # Clamp to reasonable range
        context_window = max(context_window, timedelta(days=1))
        context_window = min(context_window, timedelta(days=60))

        trigger_info = f"Scheduled wakeup: {wakeup['label']}"
        if wakeup.get("message"):
            trigger_info += f"\nContext: {wakeup['message']}"

        try:
            await run_ai(guild, user_id, channel, trigger_info, context_window)
        except Exception as e:
            logging.error(f"AI companion: error firing wakeup {wakeup['id']}: {e}")

        # Reschedule
        try:
            now = datetime.now(timezone.utc)
            next_run = compute_next_run(wakeup["schedule"], after=now)
            await update_wakeup_next_run(wakeup["id"], next_run.isoformat())
        except Exception as e:
            logging.error(f"AI companion: error rescheduling wakeup {wakeup['id']}: {e}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Respond when a user replies to the AI's messages in their personal channel."""
        if message.author.bot:
            return
        if not message.guild:
            return

        # Check if this is a reply to one of our messages
        if not message.reference or not message.reference.message_id:
            return

        # Check if the replied-to message is from us
        try:
            replied_to = await message.channel.fetch_message(message.reference.message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return

        if replied_to.author.id != self.bot.user.id:
            return

        # Check if this is in the user's personal channel
        channel_id = await get_user_channel(message.guild.id, message.author.id)
        if not channel_id or message.channel.id != channel_id:
            return

        # Check if user has AI config
        config = await get_ai_config(message.guild.id, message.author.id)
        if not config or not config["enabled"]:
            return

        trigger_info = f"User replied to your message: \"{message.content}\""
        await run_ai(
            message.guild, message.author.id, message.channel,
            trigger_info, timedelta(days=2)
        )

    @discord.slash_command(name="ai", description="Talk to your AI companion")
    async def ai_command(self, ctx: discord.ApplicationContext):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        if not get_ai_client():
            await ctx.respond("AI companion not configured (ANTHROPIC_API_KEY not set).", ephemeral=True)
            return

        # Find user's personal channel
        channel_id = await get_user_channel(guild.id, ctx.author.id)
        if not channel_id:
            await ctx.respond(
                "You need a personal channel first. Use `/channel add` to create one.",
                ephemeral=True,
            )
            return
        channel = guild.get_channel(channel_id)
        if not channel:
            await ctx.respond("Your personal channel couldn't be found.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)

        config = await get_ai_config(guild.id, ctx.author.id)
        if not config:
            # First time setup
            await upsert_ai_config(guild.id, ctx.author.id, DEFAULT_SYSTEM_PROMPT)
            trigger = (
                "User is setting up their AI companion for the first time. "
                "Introduce yourself briefly. Ask what they'd like help with — "
                "goals, habits, accountability, reflections, etc. "
                "Let them know they can ask you to set up scheduled check-ins."
            )
        else:
            trigger = "User initiated conversation with /ai"

        await run_ai(guild, ctx.author.id, channel, trigger, timedelta(days=2))
        await ctx.followup.send(f"Check {channel.mention}!", ephemeral=True)


def setup(bot):
    bot.add_cog(AICompanion(bot))
