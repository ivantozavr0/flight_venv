import requests
import datetime
import csv
import pandas
import time
from FlightRadar24 import FlightRadar24API
import logging

# в этом модуле считываются информация с сайта FlightRadar24

logging.basicConfig(
    filename='logs/collector.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

def parse_data():
    print("\033[1;37;40m --------COLLECTOR: Мы начинаем парсинг данных с сайта FlightRadar24. Это займет 1-3 минуты в зависимости от количества самолетов над Черным морем (приходится делать интервалы между запросами, иначе если слишком часто отдавать запросы, может произойти блокировка)")
    logging.info("Starting parsing")
    
    minlat, maxlat, minlon, maxlon = 41.0, 46.0, 28.0, 42.0
    
    BBOX = str(maxlat) + "," + str(minlat) + "," + str(minlon) + "," + str(maxlon)  # черное море: max lat, min lat, min lon, max lon
    
    # первичная (не подробная информация). Для более подробной придется отдавать запросы по отдельности для каждого маршрута
    url = f"https://data-cloud.flightradar24.com/zones/fcgi/feed.js?bounds={BBOX}"
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    data = response.json() 
    
    # первые 2 ключа - это служебная информация, а дальше уже то, что нужно (ключи - это id самолета) 
    flight_ids = list(data.keys())[2:]
    
    # время считывания данной информации
    cur_time = datetime.datetime.now()
    
    # вспомогательный класс. fr_api.get_flight_details принимает объект у которого есть поле id
    class Foo:
        def __init__(self, id_=None):
            self.id = id_
    
    # открываем файл для записи полученной информации
    with open("data/parse.csv", "w+", encoding="utf-8") as file:
        writer = csv.writer(file)
        
        # данные, которые будем хранить
        writer.writerow(['icao', 'callsign', 'model', 'airline', 'trail', 'time'])

        foo = Foo()
        
        fr_api = FlightRadar24API()
        
        for j, flight_id in enumerate(flight_ids):
            #print(j)
            foo.id = flight_id
            # посылаем запросы на FlightRadar24. Быть может, какие-то маршруты скрыты
            try:
                flight_details = fr_api.get_flight_details(foo)
                if not flight_details:
                    continue
                
                airline_name = None
                airline = flight_details.get("airline", {})
                if airline:
                    airline_name = airline.get("name")
        
                flighttrack = None
                try:
                    flighttrack = flight_details.get("trail") 
                except KeyError:
                    print ("No trail data available")
        
                filteredtrack = []
            
                for point in flighttrack:
                    latitude = point.get("lat") # широта
                    longitude = point.get("lng") # долгота
                    
                    # отслеживаем маршрут только в акватории Черного моря
                    if minlat <= latitude <= maxlat and minlon <= longitude <= maxlon:
                        filteredtrack.append([latitude, longitude])
                    
                row = [
                    data[flight_id][0],          # ICAO-код
                    data[flight_id][16],     # Позывной
                    flight_details.get("aircraft", {}).get("model", {}).get("text"),         # Модель самолета
                    airline_name,     # Авиакомпания
                    filteredtrack,      # Маршрут в области черного моря
                    cur_time     # Время получения данного маршрута
                ]
                
                # записываем
                writer.writerow(row)
                time.sleep(0.6)
                # если вдруг что-то пошло не так
            except Exception as e:
                logging.info(f"Произошла ошибка при обработке flight_id {flight_id}: {e}")
                print(f"Произошла ошибка при обработке flight_id {flight_id}: {e}")
                time.sleep(0.6)  
                
    print("--------COLLECTOR: Данные загружены!\n")
    logging.info("Load data")
    

if __name__ == "__main__":
    parse_data()







