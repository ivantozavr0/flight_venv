import pandas as pd
import datetime
import logging

# здесь будет обработка. щаполним таблицу с маршрутами в пределах последнего часа, составим статистику по количеству бортов по авиалиниям и маршрутам

logging.basicConfig(
    filename='logs/processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

def process_data():
    #-------------------------------удаляем все строки, которым больше часа
    print("--------PROCESSING: готовим часовую статистику. Сохраняем новые маршруты, удаляем те, что были загружены более часа назад. Также формируем данные по количеству маршрутов в зависимости от авиакомпаний и моделей, они будет представлены на дашборде")
    logging.info("Starting processing")
    
    # достаем загруженные данные
    
    df_parse = pd.read_csv("data/parse.csv")
    # время последней загрузки маршрутов
    cur_time = datetime.datetime.fromisoformat(df_parse["time"][0])
    
    # датафрей для маршрутов за последний час
    df_hourly_report = None
    # если таблицы еще нет, то считывать нечего
    try:
        df_hourly_report = pd.read_csv("data/hourly_report.csv")
        # приводим столбец времени к формату isoformat
        df_hourly_report['time'] = pd.to_datetime(df_hourly_report['time'])
        # оставим только те данные, которые записаны не более часа назад
        df_hourly_report = df_hourly_report[(cur_time - df_hourly_report['time']) < pd.Timedelta(seconds=3600)]
    except FileNotFoundError as e:
        df_hourly_report = pd.DataFrame(columns=df_parse.columns)
    
    
    # если какие-то маршруты встречались за последний час и были в файле hourly_report.csv, но при этом они есть и в свежих данных, то значит информация по ним обновилась и мы должны такие icao перезаписать 
    mask = ~df_hourly_report['icao'].isin(df_parse['icao'])   
    df_hourly_unique = df_hourly_report[mask]
    
    # формируем новый часовой отчет
    df_hourly_report = pd.concat([df_hourly_unique, df_parse], ignore_index=True)
    
    # записали отчет 
    with open("data/hourly_report.csv", "w+", encoding="utf-8") as file:
        df_hourly_report.to_csv(file, index=False)
        
    # теперь отчеты по количеству маршрутов, сгруппированных по моделям и авиалиниям
    grouped = df_hourly_report.groupby('airline').size()
    sorted_grouped = grouped.sort_values(ascending=True)
    df_airline = sorted_grouped.reset_index(name='numb')

    grouped = df_hourly_report.groupby('model').size()
    sorted_grouped = grouped.sort_values(ascending=True)
    df_model = sorted_grouped.reset_index(name='numb')
    
    with open("data/airline_report.csv", "w+", encoding="utf-8") as file:
        df_airline.to_csv(file, index=False)

    with open("data/model_report.csv", "w+", encoding="utf-8") as file:
        df_model.to_csv(file, index=False)
    
    print("--------PROCESSING: статистика обновлена!\n")
    logging.info("Successful processing")

if __name__ == "__main__":
    process_data()








