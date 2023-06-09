import csv
import sqlite3

# Step 1: Read and parse the CSV file
csv_file = r'E:\vianova\geonames-all-cities-with-a-population-1000.csv'
data = []
with open(csv_file, 'r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=';')
    next(reader)  # Skip header row
    for row in reader:
        data.append(row)

# Step 2: Create a SQLite database and table
database = sqlite3.connect('cities.db')
cursor = database.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS cities (
        geoname_id INTEGER,
        name TEXT,
        ascii_name TEXT,
        alternate_names TEXT,
        feature_class TEXT,
        feature_code TEXT,
        country_code TEXT,
        country_name_en TEXT,
        country_code_2 TEXT,
        admin1_code TEXT,
        admin2_code TEXT,
        admin3_code TEXT,
        admin4_code TEXT,
        population INTEGER,
        elevation INTEGER,
        digital_elevation_model INTEGER,
        timezone TEXT,
        modification_date TEXT,
        label_en TEXT,
        coordinates TEXT
    )
''')

# Step 3: Insert data into the table
cursor.executemany('''
    INSERT INTO cities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', data)
database.commit()

# Step 4: Execute SQL query to retrieve countries without a megapolis
query = '''
    SELECT DISTINCT country_code, country_name_en
    FROM cities
    WHERE population <= 10000000
      AND country_code NOT IN (
          SELECT country_code
          FROM cities
          WHERE population > 10000000
      )
    ORDER BY country_name_en
'''

# Step 5: Execute the query and fetch the results
cursor.execute(query)
results = cursor.fetchall()

# Step 6: Save the results as a tab-separated value (TSV) file
tsv_file = 'countries_without_megapolis.tsv'
with open(tsv_file, 'w', encoding='utf-8', newline='') as file:
    writer = csv.writer(file, delimiter='\t')
    writer.writerow(['Country Code', 'Country Name'])
    writer.writerows(results)

# Close the database connection
database.close()
