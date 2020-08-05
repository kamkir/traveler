import sqlite3


def create_providers_db():
    conn = sqlite3.connect("rai.db")
    c = conn.cursor()       
   
    c.execute('''
        ATTACH 'results.db' AS new
        ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS new.providers(
            id INTEGER PRIMARY KEY,          
            provider TEXT NOT NULL UNIQUE)    
    ''')

    c.execute('''
        INSERT OR REPLACE INTO new.providers (id, provider) SELECT (?), provider FROM rainbow        
    ''', '1')

    conn.commit()
    conn.close()

def create_hotels_db():
    conn = sqlite3.connect("rai.db")
    c = conn.cursor()       
   
    c.execute('''
        ATTACH 'results.db' AS new
        ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS new.hotels(
            id INTEGER PRIMARY KEY AUTOINCREMENT,          
            name TEXT NOT NULL UNIQUE,
            category INTEGER,
            country TEXT,
            destination TEXT,
            provider_id INTEGER NOT NULL,
            FOREIGN KEY (provider_id) REFERENCES providers(id)                   
            )    
    ''')

    c.execute('''
        INSERT OR REPLACE INTO new.hotels (name, category, country, destination, provider_id) SELECT hotel_name, category, country, destination, (select id from providers where provider = 'Rainbow') FROM rainbow        
    ''')

    conn.commit()
    conn.close()


def create_offers_db():
    conn = sqlite3.connect("rai.db")
    c = conn.cursor()    

    c.execute("PRAGMA foreign_keys = ON")   
   
    c.execute('''
        ATTACH 'results.db' AS new
        ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS new.offers(          
            airport TEXT,
            date TEXT,
            board TEXT,
            price INTEGER,
            hotel_id INTEGER NOT NULL,
            FOREIGN KEY (hotel_id) REFERENCES hotels(id)                   
            )    
    ''')

    c.execute('''
        INSERT INTO new.offers (airport, date, board, price, hotel_id) SELECT airport, date, board, price, (select id from hotels where name = rainbow.hotel_name) FROM rainbow        
    ''')

    conn.commit()
    conn.close()



create_providers_db()
create_hotels_db()
create_offers_db()
