from flask import g, request, abort, jsonify
from . import main, code

@main.route('/last<int:hours>h')
def last(hours):

    if hours < 1 or hours > 12: abort(400)

    city = request.args.get('city')
    if not city: abort(400)
    if not code.available(city): abort(400)

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
        WHERE city_code = {}
        AND time_point > (SELECT MAX(time_point) - INTERVAL '{}' HOUR FROM work)
        ORDER BY time_point
    '''.format(city, hours)

    cursor = g.db.cursor()
    cursor.execute(sql)
    out = cursor.fetchall()
    cursor.close()

    json_back = [None] * hours

    for hour_data in out:

        time_point, city_code, aqi, o3, o3_predict, co, co_predict, so2, so2_predict, no2, no2_predict, pm2_5, pm2_5_predict, pm10, pm10_predict = hour_data

        hour = {
            'time_point': time_point.strftime('%Y-%m-%d %H:%M'),
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
        
        json_back[hours - int((out[-1][0] - hour_data[0]).total_seconds()//3600) - 1] = hour

    return jsonify(json_back)