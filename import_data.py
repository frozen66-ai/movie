import pandas as pd
import happybase

# 连接HBase（替换为你的HBase主机名，伪分布式一般是localhost）
connection = happybase.Connection('192.168.56.102', port=9090)                                      #连接 192.168.56.102 的 Thrift 服务（端口 9090）
connection.open()

# 导入movies.csv到'movies'表（列族'info'）
def import_movies(csv_path):
    try:
        df = pd.read_csv(csv_path)
        table = connection.table('movies')  # 表名必须是'movies'
        with table.batch(batch_size=1000) as batch:
            for idx, row in df.iterrows():
                # 打印前10行数据，确认是否读取正确（调试用）
                if idx < 10:
                    print(f"导入movies第{idx}行：{row}")
                movie_id = str(row['movieId'])                                                      #以 movieId 为行键，将 title 和 genres 写入info列族
                batch.put(
                    movie_id,
                    {
                        'info:title': str(row['title']),  # 列族必须是'info'
                        'info:genres': str(row['genres'])
                    }
                )
        print("movies.csv导入完成")
    except Exception as e:
        print(f"movies导入失败：{str(e)}")

# 导入ratings.csv到'ratings'表（修正列族为'rating'）
def import_ratings(csv_path):
    try:
        df = pd.read_csv(csv_path)
        table = connection.table('ratings')  # 表名必须是'ratings'
        with table.batch(batch_size=1000) as batch:
            for idx, row in df.iterrows():
                # 打印前10行数据，确认是否读取正确（调试用）
                if idx < 10:
                    print(f"导入ratings第{idx}行：{row}")
                row_key = f"{int(row['userId'])}_{int(row['movieId'])}"  # 确保是整数拼接
                batch.put(
                    row_key,
                    {
                        # 关键修改：将列族从'info'改为实际存在的'rating'
                        'rating:rating': str(row['rating']),
                        'rating:timestamp': str(row['timestamp'])
                    }
                )
        print("ratings.csv导入完成")
    except Exception as e:
        print(f"ratings导入失败：{str(e)}")

if __name__ == '__main__':
    # 务必替换为CSV文件的绝对路径（避免路径错误）
    movies_path = 'movies.csv'
    ratings_path = 'ratings.csv'
    
    import_movies(movies_path)
    import_ratings(ratings_path)
    connection.close()