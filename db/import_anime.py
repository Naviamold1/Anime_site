import sqlite3
import csv
import os
import glob
import sys
import json
import re

DB_NAME = "media.db"

# Columns after removing urls except poster_url, and removing ranked/score/popularity
MEDIA_COLUMNS = [
    "id",
    "name", "english_name", "japanese_name", "other_name", "russian",
    "type", "episodes", "episodes_aired",
    "volumes", "chapters", "aired", "aired_on", "released_on",
    "premiered", "producers", "licensors", "studios", "source",
    "duration", "rating",
    "genres", "status", "synopsis", "poster_url"
]

HEADER_MAPPING = {
    "MAL_ID": "id", "anime_id": "id", "id": "id",
    "Name": "name", "name": "name", "English name": "english_name",
    "Japanese name": "japanese_name", "Other name": "other_name", "russian": "russian",
    "Image URL": "poster_url",
    "kind": "type", "Type": "type", "type": "type",
    "Episodes": "episodes", "episodes": "episodes", "episodes_aired": "episodes_aired",
    "volumes": "volumes", "chapters": "chapters",
    "Aired": "aired", "aired_on": "aired_on", "released_on": "released_on", "Premiered": "premiered",
    "Producers": "producers", "Licensors": "licensors", "Studios": "studios", "Source": "source",
    "Duration": "duration", "Rating": "rating",
    "Genres": "genres", "genre": "genres",
    "Status": "status", "status": "status",
    "Synopsis": "synopsis", "synopsis": "synopsis", "sypnopsis": "synopsis"
}

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def find_files(script_dir, ext):
    return glob.glob(os.path.join(script_dir, f"*.{ext}"))

def create_media_table(cursor):
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS media (
        id INTEGER PRIMARY KEY,
        name TEXT, english_name TEXT, japanese_name TEXT,
        other_name TEXT, russian TEXT, type TEXT,
        episodes INTEGER, episodes_aired INTEGER, volumes INTEGER, chapters INTEGER,
        aired TEXT, aired_on TEXT, released_on TEXT, premiered TEXT,
        producers TEXT, licensors TEXT, studios TEXT, source TEXT,
        duration TEXT, rating TEXT,
        genres TEXT, status TEXT, synopsis TEXT, poster_url TEXT
    )
    """)

def map_row(row):
    return {col: row.get(csv_col, None) for csv_col, col in HEADER_MAPPING.items() if csv_col in row}

def import_csv(cursor, filename):
    with open(filename, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            mapped = {col: None for col in MEDIA_COLUMNS}
            mapped.update(map_row(row))
            rows.append([mapped[col] for col in MEDIA_COLUMNS])
        cursor.executemany(
            f"INSERT OR REPLACE INTO media ({','.join(MEDIA_COLUMNS)}) VALUES ({','.join('?' for _ in MEDIA_COLUMNS)})",
            rows
        )
    print(f"✅ Imported/updated {os.path.basename(filename)}")

def import_json(cursor, filename):
    with open(filename, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("data", data) if isinstance(data, dict) else data
    rows = []

    for entry in entries:
        mapped = {col: None for col in MEDIA_COLUMNS}
        mapped["name"] = entry.get("title")
        mapped["type"] = entry.get("type")
        mapped["episodes"] = entry.get("episodes")
        mapped["status"] = entry.get("status")
        mapped["genres"] = ", ".join(entry.get("tags", []))
        mapped["poster_url"] = entry.get("picture")

        season = entry.get("animeSeason", {}).get("season")
        year = entry.get("animeSeason", {}).get("year")
        if year:
            mapped["aired_on"] = f"{year}-01-01"
            mapped["premiered"] = f"{season} {year}" if season else str(year)

        rows.append([mapped[col] for col in MEDIA_COLUMNS])

    cursor.executemany(
        f"INSERT OR REPLACE INTO media ({','.join(MEDIA_COLUMNS)}) VALUES ({','.join('?' for _ in MEDIA_COLUMNS)})",
        rows
    )
    print(f"✅ Imported {len(rows)} entries from {os.path.basename(filename)}")

def remove_duplicates(cursor):
    cursor.execute("""
        DELETE FROM media
        WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM media GROUP BY id, name
        )
    """)
    print("🗑️ Removed duplicate rows (kept latest).")

def main():
    script_dir = get_script_dir()
    db_path = os.path.join(script_dir, DB_NAME)

    csv_files = find_files(script_dir, "csv")
    json_files = find_files(script_dir, "json")

    if not csv_files and not json_files:
        print("❌ No CSV or JSON files found. Exiting.")
        sys.exit()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        create_media_table(cursor)

        for file in csv_files:
            import_csv(cursor, file)
        for file in json_files:
            import_json(cursor, file)

        remove_duplicates(cursor)
        conn.commit()

    print(f"🎉 Database '{db_path}' created/updated successfully!")

if __name__ == "__main__":
    main()