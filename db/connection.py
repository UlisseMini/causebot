from databases import Database

DATABASE_URL = "sqlite+aiosqlite:///./data.db"
database = Database(DATABASE_URL)