from flask import g, request, jsonify
from . import main

@main.route('/rank')
def rank():

    order = 'ASC'
    reverse = request.args.get('reverse')
    if reverse in ['1', 'true']: order = 'DESC'

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
        FROM work 
        WHERE time_point = (SELECT MAX(time_point) FROM work) 
        ORDER BY aqi {}
    '''.format(order)

    cursor = g.db.cursor()
    cursor.execute(sql)
    out = cursor.fetchall()
    cursor.close()

    json_back = {'cities': []}
    json_back['time_point'] = out[0][0].strftime('%Y-%m-%d %H:%M') if out else None

    for city_data in out:

        time_point, city_code, aqi, o3, o3_predict, co, co_predict, so2, so2_predict, no2, no2_predict, pm2_5, pm2_5_predict, pm10, pm10_predict = city_data

        # if aqi == None: continue

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

        json_back['cities'].append(city)

    return jsonify(json_back)