# -*- coding: utf-8 -*-
import pathlib

def creatTables(connect):

    cursor = connect.cursor()
    try:
        cursor.execute('''
            create table city(
                city_code integer not null,
                city_name_zh varchar(20) not null,
                city_name_en varchar(20) not null,
                primary key(city_code)
            )
        ''')
    except Exception as e:
        cursor.execute('rollback')
        print(e)

    try:
        cursor.execute('''
            create table work (
                time_point timestamp not null,
                city_code integer not null,
                aqi integer,
                aqi_predict integer,
                o3 integer,
                o3_predict integer,
                co numeric(4,1),
                co_predict numeric(4,1),
                so2 integer,
                so2_predict integer,
                no2 integer,
                no2_predict integer,
                pm2_5 integer,
                pm2_5_predict integer,
                pm10 integer,
                pm10_predict integer,
                primary key(time_point,city_code),
                foreign key(city_code) references city(city_code)
            )
            ''')
    except Exception as e:
        cursor.execute('rollback')
        print(e)
    
    connect.commit()
    cursor.close()


def fillCityTable(connect):
    with open(str(pathlib.Path(__file__).parent.joinpath('cities.csv')), 'r', encoding = 'utf-8') as f:
        data = f.read()

    sql = 'insert into city values (%s, %s, %s)'
    params = []

    cities = data.split('\n')
    for city in cities:
        content = city.split(',')
        city_code = content[0]
        city_name_zh = content[1]
        city_name_en = content[2]
        params.append([city_code, city_name_zh, city_name_en])

    cursor = connect.cursor()
    try:
        cursor.executemany(sql, params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)