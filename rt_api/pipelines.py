from itemadapter import ItemAdapter
import sqlite3

class RapiPipeline:

    def open_spider(self, spider):
        self.connection = sqlite3.connect("rai.db")
        self.c = self.connection.cursor()
        try:
            self.c.execute('''
            CREATE TABLE rainbow(
                provider TEXT,
                country TEXT,
                destination TEXT,
                hotel_name TEXT,
                category TEXT,
                board TEXT,
                date TEXT,
                airport TEXT,
                num_days TEXT,
                price TEXT
            )        
            ''')
            self.connection.commit()
        except sqlite3.OperationalError:
            pass

    def close_spider(self, spider):
        self.connection.close()



    def process_item(self, item, spider):
        self.c.execute('''
            INSERT into rainbow (provider, country, destination, hotel_name, category, board, 
            date, airport, num_days, price) 
            VALUES(?,?,?,?,?,?,?,?,?,?)''',
            (
                item.get('provider'),
                item.get('country'),
                item.get('destination'),
                item.get('hotel_name'),
                item.get('category'),
                item.get('board'),
                item.get('date'),
                item.get('airport'),
                item.get('num_days'),
                item.get('price'),
            )
        )
        self.connection.commit()
        return item
