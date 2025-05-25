import mysql.connector
from loadRatings import LoadRatings

DATABASE_NAME = 'dds_assgn1'

RATINGS_TABLE = 'ratings'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 20

if __name__ == "__main__":
    # Kết nối cơ sở dữ liệu
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="mancity1903",
        database=DATABASE_NAME
    )

    # Gọi hàm LoadRatings với các tham số cần thiết
    LoadRatings(conn, INPUT_FILE_PATH, RATINGS_TABLE, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)

    # Đóng kết nối sau khi hoàn thành
    conn.close()
    print("ok")