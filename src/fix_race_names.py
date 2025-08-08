#!/usr/bin/env python3

import sqlite3
import re

def clean_race_name(race_name):
    if not race_name:
        return 'Unknown'
    
    # Remove PCS prefix formatting like '» ' at the start
    race_name = re.sub(r'^»\s*', '', race_name)
    
    # Handle edition numbers at the start like '103rdTour de France' -> 'Tour de France'
    race_name = re.sub(r'^\d+\w*(?=[A-Z])', '', race_name)
    
    # Remove year prefixes like '2024 ' or '2024 - '
    race_name = re.sub(r'^\d{4}\s*[-–—]?\s*', '', race_name)
    
    # Remove edition numbers like ' (2024)' or ' - 2024'
    race_name = re.sub(r'\s*[-–—]?\s*\d{4}\s*$', '', race_name)
    
    # Remove edition numbers like ' (1)' or ' - 1'
    race_name = re.sub(r'\s*[-–—]?\s*\(\d+\)\s*$', '', race_name)
    race_name = re.sub(r'\s*[-–—]?\s*\d+\s*$', '', race_name)
    
    # Remove extra whitespace and clean up
    race_name = race_name.strip()
    
    return race_name if race_name else 'Unknown'

def main():
    # Connect to database
    conn = sqlite3.connect('data/cycling_data.db')
    cursor = conn.cursor()

    # Get all race names that need cleaning
    cursor.execute('SELECT DISTINCT id, race_name FROM races WHERE race_name LIKE "%»%"')
    races_to_update = cursor.fetchall()

    print(f'Found {len(races_to_update)} race names to clean')

    # Update each race name
    updated_count = 0
    for race_id, old_name in races_to_update:
        new_name = clean_race_name(old_name)
        if new_name != old_name:
            cursor.execute('UPDATE races SET race_name = ? WHERE id = ?', (new_name, race_id))
            print(f'{old_name} -> {new_name}')
            updated_count += 1

    # Commit changes
    conn.commit()
    conn.close()

    print(f'Race name cleaning completed! Updated {updated_count} race names.')

if __name__ == "__main__":
    main()