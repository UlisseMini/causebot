import discord
from discord.ext import commands, tasks
import os
import json
import logging
import asyncio
import time
import math
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
    store_message,
    get_messages_page,
    search_messages_db,
    count_channel_messages,
    get_latest_stored_message_id,
    get_earliest_stored_message_id,
    get_recent_messages_db,
    get_memory_notes,
    set_memory_notes,
)


# --- Schedule parsing ---

DAYS_OF_WEEK = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def parse_time(time_str: str) -> tuple[int, int]:
    """Parse HH:MM into (hour, minute)."""
    parts = time_str.split(":")
    return int(parts[0]), int(parts[1])


def compute_next_run(schedule: str, after: datetime | None = None) -> datetime:
    """Compute the next run time for a schedule string."""
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
        return candidate

    elif kind == "monthly":
        day_of_month = int(parts[1])
        hour, minute = parse_time(parts[2])
        candidate = after.replace(day=day_of_month, hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= after:
            if after.month == 12:
                candidate = candidate.replace(year=after.year + 1, month=1)
            else:
                candidate = candidate.replace(month=after.month + 1)
        return candidate

    else:
        raise ValueError(f"Unknown schedule format: {schedule}")


def schedule_to_human(schedule: str) -> str:
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


# --- Text attachment reading (for Discord import) ---

TEXT_EXTENSIONS = {".txt", ".md", ".py", ".js", ".ts", ".json", ".csv", ".log", ".yml", ".yaml", ".toml", ".cfg", ".ini", ".sh", ".html", ".css", ".xml", ".rs", ".go", ".java", ".c", ".cpp", ".h", ".rb", ".pl"}
MAX_ATTACHMENT_SIZE = 50_000


async def read_text_attachment(attachment: discord.Attachment) -> str | None:
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


# --- Cost estimation ---

MODEL_PRICING = {
    "claude-opus-4-6": {"input": 5.0, "output": 25.0},
    "claude-sonnet-4-6": {"input": 3.0, "output": 15.0},
    "claude-haiku-4-5": {"input": 1.0, "output": 5.0},
}

MODEL_ALIASES = {
    "opus": "claude-opus-4-6",
    "sonnet": "claude-sonnet-4-6",
    "haiku": "claude-haiku-4-5",
}

# Max input tokens per scan call (leave room for instructions + output)
SCAN_MAX_INPUT_TOKENS = 150_000


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token."""
    return len(text) // 4


def estimate_scan_cost(total_input_tokens: int, num_batches: int, model: str) -> dict:
    """Estimate cost for a scan given actual token counts."""
    model_id = MODEL_ALIASES.get(model, model)
    pricing = MODEL_PRICING.get(model_id, MODEL_PRICING["claude-sonnet-4-6"])

    # Output: ~2000 tokens per batch (memory update)
    total_output = num_batches * 2000

    input_cost = (total_input_tokens / 1_000_000) * pricing["input"]
    output_cost = (total_output / 1_000_000) * pricing["output"]

    return {
        "model": model_id,
        "estimated_input_tokens": total_input_tokens,
        "estimated_output_tokens": total_output,
        "num_batches": num_batches,
        "input_cost": round(input_cost, 4),
        "output_cost": round(output_cost, 4),
        "total_cost": round(input_cost + output_cost, 4),
    }


# --- Tool definitions ---

TOOLS = [
    {
        "name": "search_channel_history",
        "description": (
            "Search through the user's full channel message history (stored in database) for messages "
            "matching a text query. Returns matching messages with surrounding context messages."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Text to search for"},
                "before_context": {"type": "integer", "description": "Messages to include before each match (default 3)", "default": 3},
                "after_context": {"type": "integer", "description": "Messages to include after each match (default 3)", "default": 3},
                "max_results": {"type": "integer", "description": "Max matches to return (default 10)", "default": 10},
            },
            "required": ["query"],
        },
    },
    {
        "name": "set_wakeups",
        "description": (
            "Set ALL scheduled wakeups. REPLACES the entire config. "
            "Include existing wakeups you want to keep. "
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
                            "label": {"type": "string"},
                            "schedule": {"type": "string"},
                            "message": {"type": "string"},
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
        "description": "Update your own system prompt. Your current prompt is always shown in your context.",
        "input_schema": {
            "type": "object",
            "properties": {
                "new_prompt": {"type": "string", "description": "The new system prompt"},
            },
            "required": ["new_prompt"],
        },
    },
    {
        "name": "update_memory",
        "description": (
            "Update your persistent memory notes about this user. These notes survive across all conversations "
            "and are always loaded into your context. Store distilled understanding — goals, patterns, "
            "commitments, preferences, key context. NOT conversation transcripts. Keep concise."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "memory": {"type": "string", "description": "The complete updated memory notes (replaces all existing notes)"},
            },
            "required": ["memory"],
        },
    },
    {
        "name": "read_messages",
        "description": (
            "Read stored messages from the user's channel in pages. "
            "Returns messages + total count + estimated total tokens for the range. "
            "Use for browsing history or quick lookups beyond what's in your recent context."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "page": {"type": "integer", "description": "Page number (default 1)", "default": 1},
                "page_size": {"type": "integer", "description": "Messages per page (default 50, max 200)", "default": 50},
                "after_date": {"type": "string", "description": "Only messages after this ISO date (optional)"},
                "before_date": {"type": "string", "description": "Only messages before this ISO date (optional)"},
            },
            "required": [],
        },
    },
    {
        "name": "start_scan",
        "description": (
            "Scan the user's full message history and distill it into memory. "
            "TWO MODES:\n"
            "1. PREVIEW (confirmed=false): Imports messages from Discord if needed, returns message count "
            "and cost estimates per model. Use this first to show the user what the scan will involve.\n"
            "2. RUN (confirmed=true): Requires 'instructions' and 'model'. Runs the scan in the background. "
            "The instructions are the COMPLETE prompt used for each batch — they define exactly what to "
            "extract and how to update memory. Draft these carefully and show them to the user for approval "
            "before running.\n\n"
            "IMPORTANT: Always preview first, show the user the plan + cost + your drafted instructions, "
            "and only run after they confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "confirmed": {"type": "boolean", "description": "false=preview, true=run", "default": False},
                "instructions": {"type": "string", "description": "Complete prompt for the distillation. Required when confirmed=true."},
                "model": {"type": "string", "description": "Model to use: 'opus', 'sonnet', or 'haiku' (default 'sonnet')", "default": "sonnet"},
            },
            "required": [],
        },
    },
]


# --- Default system prompt ---

DEFAULT_SYSTEM_PROMPT = """\
You are a companion for self-understanding. Most people approach growth as a war with \
themselves — fixing, forcing, shoulding. You take a different approach: help them see \
themselves clearly, and growth happens naturally. Your deepest goal is to help people \
improve their relationship with themselves.

You're warm, curious, and direct. You know this person — reference what they've actually \
said and done. Pull over push, want over should, experience over advice.

You have persistent memory across conversations. After meaningful interactions, update \
your memory with what you learned — patterns, preferences, what matters to them, where \
they're at. Store understanding, not transcripts.

This is your default style. When you first meet someone, describe how you tend to work \
and ask what kind of support they'd prefer — then adapt and update your system prompt \
to remember. Keep messages concise — this is Discord. Be casual but substantive."""


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

def format_db_messages(messages: list[dict]) -> str:
    """Format messages from DB into text for the AI."""
    lines = []
    for msg in messages:
        ts = msg["created_at"][:16]  # trim to YYYY-MM-DDTHH:MM
        content = msg.get("content") or ""
        att = msg.get("attachment_text")
        if att:
            content = f"{content}\n{att}".strip()
        if content:
            lines.append(f"[{ts}] (user {msg['author_id']}): {content}")
    return "\n".join(lines) if lines else "(No messages)"


def build_wakeup_config_text(wakeups: list[dict]) -> str:
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


def build_system_message(user_prompt: str, wakeup_config_text: str, memory_text: str) -> str:
    now_utc = datetime.now(timezone.utc)
    return f"""{user_prompt}

---
CURRENT TIME: {now_utc.strftime('%Y-%m-%d %H:%M')} UTC ({now_utc.strftime('%A')})

All timestamps in messages and schedules are UTC. When users mention times in other
timezones (e.g. "11pm EST", "9am Pacific"), convert to UTC for storage/scheduling.
Common offsets: EST=UTC-5, CST=UTC-6, MST=UTC-7, PST=UTC-8, EDT=UTC-4, CDT=UTC-5, MDT=UTC-6, PDT=UTC-7.

---
MEMORY (persistent notes about this user):

{memory_text}

---
CONFIGURATION:

{wakeup_config_text}

---
TOOLS:
- search_channel_history: Search stored messages by text
- set_wakeups: Manage scheduled check-ins (REPLACES all — include ones to keep)
- update_system_prompt: Edit your own system prompt
- update_memory: Update your persistent memory notes about this user
- read_messages: Browse stored message history in pages
- start_scan: Bulk scan + distill message history into memory (preview first, then confirm)"""


# --- Discord import ---

async def _import_batch(channel, history_iter, progress_callback, start_count=0) -> int:
    """Import messages from a discord history iterator. Returns count of new non-bot messages."""
    count = start_count
    async for msg in history_iter:
        if msg.author.bot:
            continue
        att_parts = []
        for a in msg.attachments:
            ext = os.path.splitext(a.filename)[1].lower()
            if ext in TEXT_EXTENSIONS and a.size <= MAX_ATTACHMENT_SIZE:
                try:
                    content_bytes = await a.read()
                    att_parts.append(f"[file: {a.filename}]\n{content_bytes.decode('utf-8', errors='replace')}\n[/file: {a.filename}]")
                except Exception:
                    att_parts.append(f"[attachment: {a.filename}]")
            elif a.filename:
                att_parts.append(f"[attachment: {a.filename}]")
        attachment_text = "\n".join(att_parts) if att_parts else None
        await store_message(
            guild_id=msg.guild.id if msg.guild else 0,
            channel_id=channel.id,
            author_id=msg.author.id,
            content=msg.content,
            attachment_text=attachment_text,
            discord_message_id=msg.id,
            created_at=msg.created_at.isoformat(),
        )
        count += 1
        if progress_callback and count % 500 == 0:
            await progress_callback(count)
    return count


async def import_channel_messages(channel: discord.TextChannel, progress_callback=None) -> int:
    """Import messages from Discord into the DB. Incremental — imports before earliest and after latest stored."""
    earliest_id = await get_earliest_stored_message_id(channel.id)
    latest_id = await get_latest_stored_message_id(channel.id)

    count = 0

    if earliest_id is None:
        # No stored messages — full import
        count = await _import_batch(channel, channel.history(limit=None, oldest_first=True), progress_callback)
        return count

    # Phase 1: import everything BEFORE the earliest stored message
    try:
        earliest_msg = await channel.fetch_message(earliest_id)
        count = await _import_batch(channel, channel.history(limit=None, oldest_first=True, before=earliest_msg), progress_callback)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        pass

    # Phase 2: import everything AFTER the latest stored message
    if latest_id:
        try:
            latest_msg = await channel.fetch_message(latest_id)
            count = await _import_batch(channel, channel.history(limit=None, oldest_first=True, after=latest_msg), progress_callback, start_count=count)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    return count


# --- Scan execution ---

def _make_progress_bar(current: int, total: int, width: int = 16) -> str:
    filled = int(width * current / max(total, 1))
    bar = "\u2588" * filled + "\u2591" * (width - filled)
    return f"[{bar}]"


def _build_scan_batches(messages: list[dict], max_tokens: int = SCAN_MAX_INPUT_TOKENS,
                         overhead_tokens: int = 3000) -> list[list[dict]]:
    """Split messages into batches that fit within max_tokens (based on actual text length).
    overhead_tokens accounts for instructions + memory per batch."""
    batches = []
    current_batch = []
    current_tokens = overhead_tokens

    for msg in messages:
        content = msg.get("content") or ""
        att = msg.get("attachment_text") or ""
        # Actual token estimate from the formatted line
        msg_tokens = estimate_tokens(f"[xxxx-xx-xxTxx:xx] (user {msg['author_id']}): {content}\n{att}".strip())
        if current_batch and current_tokens + msg_tokens > max_tokens:
            batches.append(current_batch)
            current_batch = []
            current_tokens = overhead_tokens
        current_batch.append(msg)
        current_tokens += msg_tokens

    if current_batch:
        batches.append(current_batch)
    return batches


async def run_scan(guild_id: int, user_id: int, channel: discord.TextChannel,
                   channel_id: int, instructions: str, model: str = "sonnet"):
    """Run the scan in background: large batches up to ~150k tokens each."""
    client = get_ai_client()
    if not client:
        await channel.send("Error: ANTHROPIC_API_KEY not set.")
        return

    model_id = MODEL_ALIASES.get(model, model)

    # Fetch ALL messages from DB
    all_msgs, total = await get_messages_page(channel_id, page=1, page_size=999999)
    if not all_msgs:
        await channel.send("No messages to scan.")
        return

    batches = _build_scan_batches(all_msgs)
    num_batches = len(batches)
    show_progress = num_batches > 1

    if show_progress:
        progress_msg = await channel.send(
            f"Scanning... {_make_progress_bar(0, num_batches)} 0/{num_batches} batches | starting..."
        )
    else:
        progress_msg = await channel.send("Scanning...")

    start_time = time.monotonic()
    memory = await get_memory_notes(guild_id, user_id) or ""

    for batch_num, batch in enumerate(batches, 1):
        try:
            batch_text = format_db_messages(batch)

            response = client.messages.create(
                model=model_id,
                max_tokens=8192,
                system=instructions,
                messages=[{
                    "role": "user",
                    "content": f"Current memory notes:\n\n{memory}\n\n---\n\nMessages batch {batch_num}/{num_batches} ({len(batch)} messages):\n\n{batch_text}",
                }],
            )

            text_blocks = [b.text for b in response.content if b.type == "text"]
            new_memory = "\n".join(text_blocks).strip()
            if new_memory:
                memory = new_memory
                await set_memory_notes(guild_id, user_id, memory)

        except Exception as e:
            logging.error(f"Scan batch {batch_num} error: {e}")
            await channel.send(f"Error on batch {batch_num}: {e}")
            break

        if show_progress:
            elapsed = time.monotonic() - start_time
            avg_per_batch = elapsed / batch_num
            remaining = avg_per_batch * (num_batches - batch_num)
            eta_min = int(remaining // 60)
            eta_sec = int(remaining % 60)
            try:
                await progress_msg.edit(
                    content=f"Scanning... {_make_progress_bar(batch_num, num_batches)} "
                            f"{batch_num}/{num_batches} batches | "
                            f"~{eta_min}m{eta_sec:02d}s remaining"
                )
            except discord.HTTPException:
                pass

    total_time = time.monotonic() - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    try:
        await progress_msg.edit(content=f"Scan complete! ({minutes}m{seconds:02d}s)")
    except discord.HTTPException:
        pass

    await _send_long(channel, f"**Scan finished.** Here's what I've captured in memory:\n\n{memory}")


# --- Main AI runner ---

async def run_ai(guild: discord.Guild, user_id: int, channel: discord.TextChannel,
                 trigger_info: str, context_window: timedelta | None = None):
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
    memory = config.get("memory_notes") or "(No memory notes yet. Use update_memory to start building knowledge about this user.)"

    # Fetch recent context from DB, fall back to Discord
    if context_window is None:
        context_window = timedelta(days=2)
    after_date = (datetime.now(timezone.utc) - context_window).isoformat()

    # Try DB first
    personal_channel_id = await get_user_channel(guild.id, user_id)
    ctx_channel_id = personal_channel_id or channel.id
    db_messages = await get_recent_messages_db(ctx_channel_id, limit=200, after_date=after_date)

    if db_messages:
        history_text = format_db_messages(db_messages)
    else:
        # Fall back to Discord API
        history_messages = []
        after_time = datetime.now(timezone.utc) - context_window
        try:
            async for msg in channel.history(after=after_time, oldest_first=True, limit=200):
                history_messages.append(msg)
        except (discord.Forbidden, discord.HTTPException) as e:
            logging.error(f"AI companion: failed to fetch history: {e}")

        if history_messages:
            lines = []
            for msg in history_messages:
                ts = msg.created_at.strftime("%Y-%m-%d %H:%M")
                content = msg.content or ""
                if content:
                    lines.append(f"[{ts}] {msg.author.display_name}: {content}")
            history_text = "\n".join(lines)
        else:
            history_text = "(No recent messages)"

    system = build_system_message(system_prompt, wakeup_config_text, memory)

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

            tool_uses = [b for b in response.content if b.type == "tool_use"]

            if not tool_uses:
                text_blocks = [b.text for b in response.content if b.type == "text"]
                final_text = "\n".join(text_blocks).strip()
                if final_text:
                    await _send_long(channel, final_text)
                break

            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for tool_use in tool_uses:
                result = await _handle_tool_call(
                    tool_use.name, tool_use.input,
                    guild, user_id, channel
                )
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use.id,
                    "content": result,
                })

            messages.append({"role": "user", "content": tool_results})

            # Send interim text
            text_blocks = [b.text for b in response.content if b.type == "text"]
            interim_text = "\n".join(text_blocks).strip()
            if interim_text:
                await _send_long(channel, interim_text)

            if len(messages) > 16:
                logging.warning(f"AI companion: too many tool rounds for user {user_id}")
                break

    except Exception as e:
        logging.error(f"AI companion error for user {user_id} in guild {guild.id}: {e}")


async def _handle_tool_call(name: str, input_data: dict,
                            guild: discord.Guild, user_id: int,
                            channel: discord.TextChannel) -> str:
    try:
        if name == "search_channel_history":
            personal_channel_id = await get_user_channel(guild.id, user_id)
            search_channel_id = personal_channel_id or channel.id
            results = await search_messages_db(
                search_channel_id,
                input_data["query"],
                before_context=input_data.get("before_context", 3),
                after_context=input_data.get("after_context", 3),
                max_results=input_data.get("max_results", 10),
            )
            if not results:
                return f"No messages found matching '{input_data['query']}'."

            output_parts = [f"Found {len(results)} match(es) for '{input_data['query']}':"]
            for r in results:
                group_lines = []
                for msg in r["context"]:
                    ts = msg["created_at"][:16]
                    content = msg.get("content") or ""
                    att = msg.get("attachment_text") or ""
                    text = f"{content}\n{att}".strip() if att else content
                    marker = " <<< MATCH" if msg["id"] == r["match"]["id"] else ""
                    group_lines.append(f"[{ts}] (user {msg['author_id']}): {text}{marker}")
                output_parts.append("\n".join(group_lines))
            return "\n---\n".join(output_parts)

        elif name == "set_wakeups":
            now = datetime.now(timezone.utc)
            wakeups_to_store = []
            for w in input_data["wakeups"]:
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
            lines = ["Wakeups updated:"]
            for w in wakeups_to_store:
                lines.append(f"  - {w['label']}: {schedule_to_human(w['schedule'])} (next: {w['next_run_at'][:16]})")
            return "\n".join(lines)

        elif name == "update_system_prompt":
            await update_ai_system_prompt(guild.id, user_id, input_data["new_prompt"])
            return "System prompt updated successfully."

        elif name == "update_memory":
            await set_memory_notes(guild.id, user_id, input_data["memory"])
            return "Memory updated successfully."

        elif name == "read_messages":
            personal_channel_id = await get_user_channel(guild.id, user_id)
            read_channel_id = personal_channel_id or channel.id
            page = input_data.get("page", 1)
            page_size = min(input_data.get("page_size", 50), 200)
            messages, total = await get_messages_page(
                read_channel_id, page=page, page_size=page_size,
                after_date=input_data.get("after_date"),
                before_date=input_data.get("before_date"),
            )
            total_chars = sum(len(m.get("content") or "") + len(m.get("attachment_text") or "") for m in messages)
            text = format_db_messages(messages)
            return (
                f"Page {page} ({len(messages)} messages, {total} total in range, "
                f"~{estimate_tokens(text)} tokens this page):\n\n{text}"
            )

        elif name == "start_scan":
            confirmed = input_data.get("confirmed", False)
            personal_channel_id = await get_user_channel(guild.id, user_id)
            if not personal_channel_id:
                return "Error: user has no personal channel."
            scan_channel = guild.get_channel(personal_channel_id)
            if not scan_channel:
                return "Error: personal channel not found."

            if not confirmed:
                # Preview mode: import if needed, then return stats
                imported = await import_channel_messages(scan_channel)
                total = await count_channel_messages(personal_channel_id)

                # Get ALL messages to compute actual text size and batches
                all_msgs, _ = await get_messages_page(personal_channel_id, page=1, page_size=999999)
                total_chars = sum(len(m.get("content") or "") + len(m.get("attachment_text") or "") for m in all_msgs)
                total_tokens = estimate_tokens(
                    "\n".join(
                        f"[xxxx-xx-xxTxx:xx] (user {m['author_id']}): {(m.get('content') or '')} {(m.get('attachment_text') or '')}".strip()
                        for m in all_msgs
                    )
                )
                batches = _build_scan_batches(all_msgs)
                num_batches = len(batches)

                lines = [f"**Scan Preview**"]
                lines.append(f"Messages stored: {total}" + (f" ({imported} newly imported)" if imported else ""))
                lines.append(f"Total text: {total_chars:,} chars (~{total_tokens:,} tokens)")
                lines.append(f"Batches (~150k tokens each): {num_batches}")
                lines.append("")
                lines.append("**Estimated cost per model:**")
                for alias in MODEL_ALIASES:
                    est = estimate_scan_cost(total_tokens, num_batches, alias)
                    lines.append(f"  {alias}: ~${est['total_cost']:.3f}")
                lines.append("")
                lines.append("Draft your instructions for what to extract, show them to the user, "
                             "and call start_scan again with confirmed=true, instructions=..., and model=...")
                return "\n".join(lines)

            else:
                # Run mode
                instructions = input_data.get("instructions")
                if not instructions:
                    return "Error: 'instructions' required when confirmed=true."
                model = input_data.get("model", "sonnet")

                # Launch background task
                asyncio.create_task(
                    run_scan(guild.id, user_id, channel, personal_channel_id,
                             instructions, model)
                )
                return "Scan started in background. Progress will be posted in the channel."

        else:
            return f"Unknown tool: {name}"

    except Exception as e:
        logging.error(f"AI companion tool error ({name}): {e}")
        return f"Error executing {name}: {e}"


def _split_message(text: str, limit: int = 2000) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = limit
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


async def _send_long(channel: discord.TextChannel, text: str, max_chunks: int = 3):
    """Send text to Discord, uploading as a file if it would need too many messages."""
    chunks = _split_message(text)
    if len(chunks) <= max_chunks:
        for chunk in chunks:
            await channel.send(chunk)
    else:
        # Too long for Discord messages — upload as file
        import io
        file = discord.File(io.BytesIO(text.encode("utf-8")), filename="response.txt")
        preview = text[:500] + "...\n\n*(full response attached as file)*"
        await channel.send(preview, file=file)


# --- The Cog ---

class AICompanion(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_wakeups.start()

    def cog_unload(self):
        self.check_wakeups.cancel()

    @tasks.loop(seconds=60)
    async def check_wakeups(self):
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
        guild_id = wakeup["guild_id"]
        user_id = wakeup["user_id"]

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        channel_id = await get_user_channel(guild_id, user_id)
        if not channel_id:
            return
        channel = guild.get_channel(channel_id)
        if not channel:
            return

        interval = interval_to_timedelta(wakeup["schedule"])
        context_window = max(interval * 2, timedelta(days=1))
        context_window = min(context_window, timedelta(days=60))

        trigger_info = f"Scheduled wakeup: {wakeup['label']}"
        if wakeup.get("message"):
            trigger_info += f"\nContext: {wakeup['message']}"

        try:
            await run_ai(guild, user_id, channel, trigger_info, context_window)
        except Exception as e:
            logging.error(f"AI companion: error firing wakeup {wakeup['id']}: {e}")

        try:
            now = datetime.now(timezone.utc)
            next_run = compute_next_run(wakeup["schedule"], after=now)
            await update_wakeup_next_run(wakeup["id"], next_run.isoformat())
        except Exception as e:
            logging.error(f"AI companion: error rescheduling wakeup {wakeup['id']}: {e}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Store messages + respond to @mentions and replies."""
        if not message.guild:
            return

        # Store every non-bot message in DB
        if not message.author.bot:
            att_parts = []
            for a in message.attachments:
                ext = os.path.splitext(a.filename)[1].lower()
                if ext in TEXT_EXTENSIONS and a.size <= MAX_ATTACHMENT_SIZE:
                    try:
                        content_bytes = await a.read()
                        att_parts.append(f"[file: {a.filename}]\n{content_bytes.decode('utf-8', errors='replace')}\n[/file: {a.filename}]")
                    except Exception:
                        att_parts.append(f"[attachment: {a.filename}]")
                elif a.filename:
                    att_parts.append(f"[attachment: {a.filename}]")

            await store_message(
                guild_id=message.guild.id,
                channel_id=message.channel.id,
                author_id=message.author.id,
                content=message.content,
                attachment_text="\n".join(att_parts) if att_parts else None,
                discord_message_id=message.id,
                created_at=message.created_at.isoformat(),
            )

        if message.author.bot:
            return

        triggered = False
        trigger_info = ""

        # Check for @mention
        if self.bot.user in message.mentions:
            triggered = True
            clean_content = message.content.replace(f"<@{self.bot.user.id}>", "").replace(f"<@!{self.bot.user.id}>", "").strip()
            trigger_info = f"User @mentioned you in #{message.channel.name}: \"{clean_content}\""

        # Check if reply to bot
        if not triggered and message.reference and message.reference.message_id:
            try:
                replied_to = await message.channel.fetch_message(message.reference.message_id)
                if replied_to.author.id == self.bot.user.id:
                    triggered = True
                    trigger_info = f"User replied to your message in #{message.channel.name}: \"{message.content}\""
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        if not triggered:
            return

        config = await get_ai_config(message.guild.id, message.author.id)
        if not config or not config["enabled"]:
            return

        await run_ai(
            message.guild, message.author.id, message.channel,
            trigger_info, timedelta(days=2)
        )

    @discord.slash_command(name="import-messages", description="Import/reimport all message history for your personal channel")
    async def import_messages_command(self, ctx: discord.ApplicationContext):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        channel_id = await get_user_channel(guild.id, ctx.author.id)
        if not channel_id:
            await ctx.respond("You need a personal channel first.", ephemeral=True)
            return
        channel = guild.get_channel(channel_id)
        if not channel:
            await ctx.respond("Your personal channel couldn't be found.", ephemeral=True)
            return

        await ctx.defer()

        # Delete all existing messages for this channel and reimport from scratch
        from db.connection import database
        from db.schema import channel_messages
        await database.execute(
            channel_messages.delete().where(channel_messages.c.channel_id == channel_id)
        )

        status_msg = await ctx.followup.send("Importing messages...")

        async def progress_cb(count):
            try:
                await status_msg.edit(content=f"Importing messages... {count:,} so far")
            except discord.HTTPException:
                pass

        imported = await import_channel_messages(channel, progress_callback=progress_cb)
        await status_msg.edit(content=f"Done! Imported {imported:,} messages.")

    @discord.slash_command(name="ai", description="Talk to your AI companion")
    async def ai_command(self, ctx: discord.ApplicationContext):
        guild = ctx.guild
        if not guild:
            await ctx.respond("This command can only be used in a server.", ephemeral=True)
            return

        if not get_ai_client():
            await ctx.respond("AI companion not configured (ANTHROPIC_API_KEY not set).", ephemeral=True)
            return

        channel_id = await get_user_channel(guild.id, ctx.author.id)
        if not channel_id:
            await ctx.respond("You need a personal channel first. Use `/channel add` to create one.", ephemeral=True)
            return
        channel = guild.get_channel(channel_id)
        if not channel:
            await ctx.respond("Your personal channel couldn't be found.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)

        config = await get_ai_config(guild.id, ctx.author.id)
        if not config:
            await upsert_ai_config(guild.id, ctx.author.id, DEFAULT_SYSTEM_PROMPT)
            trigger = (
                "User is setting up their AI companion for the first time. "
                "Introduce yourself briefly. Ask what they'd like help with — "
                "goals, habits, accountability, reflections, etc. "
                "Let them know they can ask you to set up scheduled check-ins "
                "and that you can scan their message history to learn about them."
            )
        else:
            trigger = "User initiated conversation with /ai"

        await run_ai(guild, ctx.author.id, channel, trigger, timedelta(days=2))
        await ctx.followup.send(f"Check {channel.mention}!", ephemeral=True)


def setup(bot):
    bot.add_cog(AICompanion(bot))
