# before you run locally

## token setup
You first need to create a .env file (in the same folder as `main.py`) which looks something like
```
DISCORD_BOT_TOKEN=your token here
```
Fill in "your token here" with the token that can be copied from `discord.com/developers/applications` -> The bot you want to run (under "My Applications") -> Bot -> Token

## uv setup
I used 'uv' to handle the Python packages for this project. Please install 'uv' on your local machine (if you haven't already) - instructions are available at [this link from astral.sh](https://docs.astral.sh/uv/getting-started/installation/)

Once you download and clone the project please run `uv sync`. This will download all the Python packages that make this bot work.

Then run `uv run main.py` and the bot should come online.

# other info
## database
Currently the bot uses SQLite as its database. (specified in `db/connection.py`)

## message privacy
Even though the bot is able to see message content, only the length of the message, the server it was sent in, and who sent it, is recorded for XP purposes. No other information is retained.

## how does xp work?
Please see `xp-rules.md`