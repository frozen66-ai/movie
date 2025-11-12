# -*- coding: utf-8 -*-
import sys
import importlib
importlib.reload(sys)
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import happybase
from happybase import ConnectionPool
import os

app = Flask(__name__)
CORS(app)  # 解决跨域问题

# 配置 HBase 连接池
pool = ConnectionPool(
    size=5,
    host='192.168.56.102',
    port=9090,
    timeout=15000
)

@app.route('/')
def index():
    return """
    <h1>电影评分 API 服务</h1>
    <p>可用接口：</p>
    <ul>
        <li>电影查询：/api/movies?movie_name=电影名（例如：/api/movies?movie_name=Toy）</li>
        <li>评分查询：/api/ratings?user_id=用户ID&movie_id=电影ID（例如：/api/ratings?user_id=1&movie_id=1）</li>
    </ul>
    """

@app.route('/favicon.ico')
def favicon():
    if not os.path.exists('static'):
        os.makedirs('static')
    return send_from_directory('static', 'favicon.ico', 
                             mimetype='image/vnd.microsoft.icon',
                             default_filename='favicon.ico')

# 电影查询接口（包含平均分计算）
@app.route('/api/movies', methods=['GET'])
def get_movies():
    movie_name = request.args.get('movie_name', '').strip()
    movie_list = []
    
    if movie_name:
        try:
            with pool.connection() as conn:
                movies_table = conn.table('movies')
                ratings_table = conn.table('ratings')
                
                # 优化点1：使用更高效的扫描方式，只获取需要的字段
                for row_key, data in movies_table.scan(
                    columns=['info:title', 'info:genres'],
                    limit=200
                ):
                    title = data.get(b'info:title', b'').decode()
                    # 优化点2：更精确的字符串匹配处理
                    if movie_name.lower() in title.lower():
                        genres = data.get(b'info:genres', b'').decode() or "未标注"
                        movie_id = row_key.decode()
                        
                        # 计算该电影的平均评分
                        total_score = 0.0
                        count = 0
                        # 扫描所有评分记录，筛选出该电影的评分（行键格式：user_id_movie_id）
                        for rk, rating_data in ratings_table.scan(columns=['rating:rating']):
                            # 解码行键，判断是否包含当前电影ID（格式：_movie_id）
                            if f"_{movie_id}" in rk.decode():
                                score_bytes = rating_data.get(b'rating:rating', b'0')
                                try:
                                    score = float(score_bytes.decode())
                                    total_score += score
                                    count += 1
                                except ValueError:
                                    continue
                        
                        avg_score = round(total_score / count, 1) if count > 0 else 0.0
                        
                        movie_list.append({
                            'id': movie_id,
                            'title': title,
                            'genres': genres,
                            'avg_rating': avg_score,
                            'rating_count': count
                        })
            
            # 优化点4：按平均分降序排序结果
            movie_list.sort(key=lambda x: x['avg_rating'], reverse=True)
            
            return jsonify({
                'success': True,
                'data': movie_list
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': f"电影查询错误：{str(e)}"
            })
    
    return jsonify({
        'success': True,
        'data': []
    })

@app.route('/api/ratings', methods=['GET'])
def get_ratings():
    user_id = request.args.get('user_id', '').strip()
    movie_id = request.args.get('movie_id', '').strip()
    
    if not (user_id and movie_id and user_id.isdigit() and movie_id.isdigit()):
        return jsonify({
            'success': False,
            'error': '请输入有效的用户ID和电影ID'
        })
    
    try:
        row_key = f"{user_id}_{movie_id}".encode()
        with pool.connection() as conn:
            ratings_table = conn.table('ratings')
            row = ratings_table.row(row_key, columns=['rating:rating'])
            
            if row:
                return jsonify({
                    'success': True,
                    'data': {
                        'score': row.get(b'rating:rating', b'0').decode()
                    }
                })
            else:
                return jsonify({
                    'success': True,
                    'data': None
                })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f"评分查询失败：{str(e)}"
        })

if __name__ == '__main__':
    print("=== Flask 服务启动 ===")
    print("后端API地址：http://192.168.56.102:5000")
    print("测试电影查询API：/api/movies?movie_name=Toy")
    print("测试评分查询API：/api/ratings?user_id=1&movie_id=1")
    app.run(host='0.0.0.0', port=5000, debug=True)