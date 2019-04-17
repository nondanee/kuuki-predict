# -*- coding: utf-8 -*-
import re, base64, zlib, io
import requests
import xml.dom.minidom, datetime
from . import wcf, patch
import os
from sklearn.externals import joblib

def get_all_stations_data():

    output = io.StringIO()
    output.write('<GetAllAQIPublishLive xmlns="http://tempuri.org/"></GetAllAQIPublishLive>')
    output.seek(0)

    data = wcf.xml2records.XMLParser.parse(output)
    data = wcf.records.dump_records(data)

    raw = requests.request(
        method = 'POST',
        url = 'http://106.37.208.233:20035/ClientBin/Env-CnemcPublish-RiaServices-EnvCnemcPublishDomainService.svc/binary/GetAllAQIPublishLive',
        data = data,
        headers = {'Content-Type': 'application/msbin1'}
    ).content

    raw = io.BytesIO(raw)
    raw = wcf.records.Record.parse(raw)

    wcf.records.print_records(raw, fp = output)
    output.seek(0)

    pattern = re.compile('<[^>]+>')
    records = pattern.sub('', output.readlines()[1][1:])[:-1]

    records = base64.b64decode(records)
    records = zlib.decompress(records)

    dom_tree = xml.dom.minidom.parseString(records)
    collection = dom_tree.documentElement
    return collection.getElementsByTagName('AQIDataPublishLive')    

def check_value(value, reserve = 0):
    if check_empty(value) == '':
        return None
    elif reserve != 0:
        return round(float(value) + 0.00000002, reserve)
    elif reserve == 0:
        return int(float(value))
        
def check_empty(value):
    if value == '\u2014' or value is None:
        return ''
    else:
        return value

def calculate_iaqi(value, value_type):
    
    if value is None: return 0
    
    boundary = [0, 50, 100, 150, 200, 300, 400, 500]

    if value_type == 'SO2':
        measure = [0, 150, 500, 650, 800]
    elif value_type == 'SO2_24h':
        measure = [0, 50, 150, 475, 800, 1600, 2100, 2620]
    elif value_type == 'NO2':
        measure = [0, 100, 200, 700, 1200, 2340, 3090, 3840]
    elif value_type == 'NO2_24h':
        measure = [0, 40, 80, 180, 280, 565, 750, 940]
    elif value_type == 'CO':
        measure = [0, 5, 10, 35, 60, 90, 120, 150]
    elif value_type == 'CO_24h':
        measure = [0, 2, 4, 14, 24, 36, 48, 60]
    elif value_type == 'O3':
        measure = [0, 160, 200, 300, 400, 800, 1000, 1200]
    elif value_type == 'O3_8h':
        measure = [0, 100, 160, 215, 265, 800]
    elif value_type == 'PM2_5_24h':
        measure = [0, 35, 75, 115, 150, 250, 350, 500]
    elif value_type == 'PM10_24h':
        measure = [0, 50, 150, 250, 350, 420, 500, 600]

    for i in range(0, len(measure)-1):
        if measure[i] <= value and value < measure[i+1]:
            iaqi =  float(boundary[i+1] - boundary[i]) / float(measure[i+1] - measure[i]) * (value - measure[i]) + boundary[i]
            return int(round(iaqi))

    return 0

def get_tag_data(node, tag_name):

    return node.getElementsByTagName(tag_name)[0].childNodes[0].data.replace(u'\u3000', u' ').strip()

def average(array):
    array = list(filter(lambda value: not (value is None), array))
    if not array: return None
    else: return 1.0 * sum(array) / len(array)

def pull(connect, all_stations_data = None):
    
    all_stations_data = get_all_stations_data() if not all_stations_data else all_stations_data
    aggregation = {}
    
    for station_data in all_stations_data:

        time_point = datetime.datetime.strptime(get_tag_data(station_data, 'TimePoint'), '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M')

        station_code = get_tag_data(station_data, 'StationCode')

        city_code = get_tag_data(station_data, 'CityCode')
        area = get_tag_data(station_data, 'Area')
        if area in patch.correct: city_code = patch.correct[area]
        if city_code not in aggregation: aggregation[city_code] = {'aqi': [], 'o3': [], 'co': [], 'so2': [], 'no2': [], 'pm2_5': [], 'pm10': []}
        
        aggregation[city_code]['aqi'].append(check_value(get_tag_data(station_data, 'AQI')))
        aggregation[city_code]['o3'].append(check_value(get_tag_data(station_data, 'O3')))
        aggregation[city_code]['co'].append(check_value(get_tag_data(station_data, 'CO'), 1))
        aggregation[city_code]['so2'].append(check_value(get_tag_data(station_data, 'SO2')))
        aggregation[city_code]['no2'].append(check_value(get_tag_data(station_data, 'NO2')))
        aggregation[city_code]['pm2_5'].append(check_value(get_tag_data(station_data, 'PM2_5')))
        aggregation[city_code]['pm10'].append(check_value(get_tag_data(station_data, 'PM10')))

    cursor = connect.cursor()

    payload = []
    cities = set(aggregation)

    for city_code in aggregation:
        data = aggregation[city_code]
        data = {key: average(data[key]) for key in data}

        payload.append([data['aqi'], data['o3'], data['co'], data['so2'], data['no2'], data['pm2_5'], data['pm10'], time_point, city_code])


    cursor.execute("SELECT city_code FROM work WHERE time_point = %s", (time_point,))
    exist = cursor.fetchall()
    exist = set([str(item[0]) for item in exist])
    insert = cities - exist
    insert = [[item,] for item in insert]

    if insert:
        try:
            cursor.executemany("INSERT INTO work (time_point, city_code) VALUES ('{}', %s)".format(time_point), insert)
            connect.commit()
        except Exception as e:
            print('insert', e)
            cursor.execute('rollback')

    try:
        cursor.executemany("UPDATE work SET aqi = %s, o3 = %s, co = %s, so2 = %s, no2 = %s, pm2_5 = %s, pm10 = %s WHERE time_point = %s AND city_code = %s", payload)
        connect.commit()
    except Exception as e:
        print('update', e)


def predict(connect):

    cursor = connect.cursor()
    cursor.execute("SELECT time_point, city_code, o3, co, so2, no2, pm2_5, pm10 FROM work WHERE time_point > (SELECT MAX(time_point) - INTERVAL '25' HOUR FROM work)")
    data = cursor.fetchall()

    end = max(set([line[0] for line in data]))
    delta = lambda a, b: int((a - b).total_seconds() // 3600)

    work = {}

    for line in data:
        time_point, city_code, o3, co, so2, no2, pm2_5, pm10 = line
        co = float(co) if co is not None else None
        
        key_o3 = '{}-o3'.format(city_code)
        key_co = '{}-co'.format(city_code)
        key_so2 = '{}-so2'.format(city_code)
        key_no2 = '{}-no2'.format(city_code)
        key_pm25 = '{}-pm25'.format(city_code)
        key_pm10 = '{}-pm10'.format(city_code)

        if key_o3 not in work: work[key_o3] = [None] * 24
        if key_co not in work: work[key_co] = [None] * 24
        if key_so2 not in work: work[key_so2] = [None] * 24
        if key_no2 not in work: work[key_no2] = [None] * 24
        if key_pm25 not in work: work[key_pm25] = [None] * 24
        if key_pm10 not in work: work[key_pm10] = [None] * 24

        locate = 23 - delta(end, time_point)

        work[key_o3][locate] = o3
        work[key_co][locate] = co
        work[key_so2][locate] = so2
        work[key_no2][locate] = no2
        work[key_pm25][locate] = pm2_5
        work[key_pm10][locate] = pm10

    def aggregate(array):
        array.reverse()
        means = [average(array[i:i + 6]) for i in range(4)]
        if any(map(lambda value: (value is None), means)):
            return None
        else:
            return means

    next_point = (end + datetime.timedelta(hours = 1)).strftime('%Y-%m-%d %H:%M')
    
    payload = {}
    cities = []

    for key in work:
        aggregation = aggregate(work[key])
        path = './repack/c{}.kp1'.format(key)
        if not os.path.exists(path): continue
        if not aggregation: continue
        regressor = joblib.load(path)
        result = regressor.predict([aggregation])[0][0]
        city_code, measure = key.split('-')
        cities.append(city_code)
        if city_code not in payload: payload[city_code] = [None, None, None, None, None, None]
        
        mapping = {'o3': 0, 'co': 1, 'so2': 2, 'no2': 3, 'pm25': 4, 'pm10': 5}
        payload[city_code][mapping[measure]] = result

    payload = [payload[city_code] + [next_point, city_code] for city_code in payload]

    cities = set(cities)
    cursor.execute("SELECT city_code FROM work WHERE time_point = %s", (next_point,))
    exist = cursor.fetchall()
    exist = set([str(item[0]) for item in exist])
    insert = cities - exist
    insert = [[item,] for item in insert]

    if insert:
        try:
            cursor.executemany("INSERT INTO work (time_point, city_code) VALUES ('{}', %s)".format(next_point), insert)
            connect.commit()
        except Exception as e:
            print('insert', e)
            cursor.execute('rollback')

    try:
        cursor.executemany("UPDATE work SET o3_predict = %s, co_predict = %s, so2_predict = %s, no2_predict = %s, pm2_5_predict = %s, pm10_predict = %s WHERE time_point = %s AND city_code = %s", payload)
        connect.commit()
    except Exception as e:
        print('update', e)

def compact(connect):

    cursor = connect.cursor()
    sql_work = '''
        DELETE FROM work WHERE work.time_point < (SELECT max(time_point) - interval '25 hour' FROM work)
    '''

    try:
        cursor.execute(sql_raw)
        connect.commit()
        cursor.close()
    except Exception as e:
        cursor.execute('rollback')
        print('compact', e)
    
