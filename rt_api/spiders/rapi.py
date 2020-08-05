import scrapy
import json
from ..items import RainbowItem
from scrapy.loader import ItemLoader



class RapiSpider(scrapy.Spider):
    name = 'rapi'

       
    def start_requests(self):
        # dla kazdego lotniska wysyłamy zapytanie 

        initial_query = {
            "KategorieWyjazdu": [
                "wypoczynek"
            ],
            "MiastaWyjazdu": [

            ],
            "Panstwa": [
                "grecja"
            ],
            "TerminWyjazduMin": "2020-07-22",
            "TerminWyjazduMax": "2020-11-26",
            "TypyTransportu": [
                "air"
            ],
            "Konfiguracja": {
                "LiczbaPokoi": "1",
                "Wiek": [
                    "1990-07-19",
                    "1990-07-19"
                ]
            },
            "Sortowanie": {
                "CzyPoDacie": False,
                "CzyPoCenie": False,
                "CzyPoOcenach": False,
                "CzyPoPolecanych": True,
                "CzyDesc": False
            },
            "CzyGrupowac": True,
            "CzyCenaZaWszystkich": False,
            "Paginacja": {
                "Przeczytane": "0",
                "IloscDoPobrania": "18"
            },
            "CzyImprezaWeekendowa": False
        }   

        all_airports = ["wroclaw", "warszawa", "poznan", "katowice", "gdansk", "lodz", "bydgoszcz", "rzeszow"]

        for airport in all_airports:
            initial_query["MiastaWyjazdu"] = [airport]
            initial_query["Paginacja"]["Przeczytane"] = "0"

            yield scrapy.Request(
                    url = 'https://rpl-api.r.pl/v3/wyszukiwarka/api/wyszukaj',
                    method='POST',
                    body=json.dumps(initial_query),
                    headers={
                        'Content-Type' : "application/json"
                    },
                    callback=self.parse,
                    cb_kwargs={"initial_airport" : airport, "initial_query" : initial_query}                
                 )
        

    def parse(self, response, initial_airport, initial_query):
        # z listy wyswietlonych hoteli bierzemy kazdy po kolei i wysylamy zapytanie dotyczace tylko tego hotelu
        
        resp_dict = json.loads(response.body) 
        blocks = resp_dict.get('Bloczki')  
        hotel_query = {
                "HotelUrl":"",
                "ProduktUrl":"",
                "CzyCenaZaWszystkich":False,
                "TerminWyjazdu":"",
                "DlugoscPobytu":8,
                "MiastoWyjazduUrl":"",
                "Wiek":[
                    "1990-07-19",
                    "1990-07-19"
                
                    ],
                    "LiczbaPokoi":"1"
                    }  

        hotel_query["MiastoWyjazduUrl"] = initial_airport
        pagination_query = initial_query
        pagination_airport = initial_airport
        
        for block in blocks:
            if initial_airport == block.get("Przystanki")[0].get("MiastoURL"):
                dest_url = block.get('BazoweInformacje').get("OfertaURLDlaGoogle").split('/')[1]
                hotel_url = block.get('BazoweInformacje').get("OfertaURLDlaGoogle").split('/')[2]
                hotel_query["HotelUrl"] = hotel_url
                hotel_query["ProduktUrl"] = dest_url

                yield scrapy.Request(
                    url = 'https://rpl-api.r.pl/wyszukiwarka/szczegoly/wyszukaj',
                    method='POST',
                    body=json.dumps(hotel_query),
                    headers={
                        'Content-Type' : "application/json"
                    },
                    callback=self.parse_hotel                                                    
                )
        


        position = pagination_query.get("Paginacja").get("Przeczytane")
        increment = 18
        if len(blocks) == increment:            
            position = int(position) + increment
            pagination_query["Paginacja"]["Przeczytane"] = str(position)

        yield scrapy.Request(
        url = 'https://rpl-api.r.pl/v3/wyszukiwarka/api/wyszukaj',
        method='POST',
        body=json.dumps(pagination_query),
        headers={
            'Content-Type' : "application/json"
        },
        callback=self.parse,
        cb_kwargs={"initial_airport" : pagination_airport, "initial_query" : pagination_query}
    )



    def parse_hotel(self, response):   
        # z listy dostepnych ofert na kazdy mozliwy termin odfiltrowujemy te ktore nas interesują np: rok

        hotel_dict = json.loads(response.body)
        terms = hotel_dict.get("DostepneFiltry").get("TerminyWyjazdu")     

        for term in terms:
            if term.get("MiastoWyjazdu") == None:
                if term.get('TerminWyjazdu')[0:4] == "2020":
                    loader = ItemLoader(item=RainbowItem(), response=response)
                    loader.add_value('provider', 'Rainbow')
                    loader.add_value('date', term.get('TerminWyjazdu'))
                    loader.add_value('airport', term.get('Cena').get('MiastoWyjazdu'))
                    loader.add_value('num_days', term.get('Cena').get('DlugoscPobytu'))
                    loader.add_value('price', term.get('Cena').get('CenaZaOsobeAktualna'))
                    loader.add_value('country', hotel_dict.get('Bloczek').get('Blok1').get('Lokalizacja')[0].get("NazwaLokalizacji"))
                    loader.add_value('hotel_name', hotel_dict.get('Bloczek').get('Blok1').get('NazwaHotelu'))
                    loader.add_value('category', int(hotel_dict.get('Bloczek').get('Blok1').get('GwiazdkiHotelu')))
                    loader.add_value('board', hotel_dict.get('Bloczek').get('Wyzywienia')[0].get('Nazwa'))
                    loader.add_value('destination', hotel_dict.get('Bloczek').get('Blok1').get('Lokalizacja')[-1].get("NazwaLokalizacji"))

                    yield loader.load_item()
            
