from flask import g, request, abort, jsonify
from . import main, code

@main.route('/latest')
def latest():

    cities = request.args.get('cities')
    if not cities: abort(400)

    cities = cities.split(',')
    if len(list(set(cities))) != len(cities): abort(400)

    for city in cities: 
        if not code.available(city): 
            abort(400)

    sql = '''
        SELECT 
        time_point,
        city_code,
        aqi,
        o3,
        o3_predict,
        co,
        co_predict,
        so2,
        so2_predict,
        no2,
        no2_predict,
        pm2_5,
        pm2_5_predict,
        pm10,
        pm10_predict 
        FROM (
            SELECT 
            time_point,
            city_code,
            aqi,
            o3,
            o3_predict,
            co,
            co_predict,
            so2,
            so2_predict,
            no2,
            no2_predict,
            pm2_5,
            pm2_5_predict,
            pm10,
            pm10_predict 
            FROM work
            WHERE time_point = (SELECT MAX(time_point) FROM work)
        ) nearest_data
        WHERE city_code IN ({})
    '''.format(','.join(cities))

    cursor = g.db.cursor()
    cursor.execute(sql)
    out = cursor.fetchall()
    cursor.close()

    json_back = {'cities': [None] * len(cities)}
    json_back['time_point'] = out[0][0].strftime('%Y-%m-%d %H:%M') if out else None
    
    for city_data in out:

        time_point, city_code, aqi, o3, o3_predict, co, co_predict, so2, so2_predict, no2, no2_predict, pm2_5, pm2_5_predict, pm10, pm10_predict = city_data

        city = {
            'city_code': city_code,
            'aqi': aqi,
            'o3': o3,
            'o3_predict': o3_predict,
            'co': float(co) if co is not None else None,
            'co_predict': float(co_predict) if co_predict is not None else None,
            'so2': so2,
            'so2_predict': so2_predict,
            'no2': no2,
            'no2_predict': no2_predict,
            'pm2_5': pm2_5,
            'pm2_5_predict': pm2_5_predict,
            'pm10': pm10,
            'pm10_predict': pm10_predict
        }
        
        json_back['cities'][cities.index(str(city_data[1]))] = city

    return jsonify(json_back)