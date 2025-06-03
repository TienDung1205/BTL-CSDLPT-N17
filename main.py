DATABASE_NAME = 'dds_assgn1'

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10000054  # Số dòng trong file dữ liệu đầu vào

import mysql.connector
import traceback
import testHelper
import Interface as MyAssignment

if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        conn = testHelper.getopenconnection(dbname=DATABASE_NAME)

        testHelper.deleteAllPublicTables(conn)

        [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
        if result:
            print("loadratings function pass!")
        else:
            print("loadratings function fail!")

        [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
        if result:
            print("rangepartition function pass!")
        else:
            print("rangepartition function fail!")

        # CẢNH BÁO:: Chỉ sử dụng một dòng tại một thời điểm, tức là chỉ bỏ comment một dòng và chạy script
        [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
        # [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 0, conn, '0')
        if result:
            print("rangeinsert function pass!")
        else:
            print("rangeinsert function fail!")

        testHelper.deleteAllPublicTables(conn)
        MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

        [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
        if result:
            print("roundrobinpartition function pass!")
        else:
            print("roundrobinpartition function fail!")

        # CẢNH BÁO:: Thay đổi chỉ số phân vùng theo thứ tự kiểm thử của bạn.
        [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
        # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '1')
        # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '2')
        if result:
            print("roundrobininsert function pass!")
        else:
            print("roundrobininsert function fail!")

        choice = input('Nhấn enter để xóa tất cả các bảng? ')
        if choice == '':
            testHelper.deleteAllPublicTables(conn)
        conn.close()

    except Exception as detail:
        traceback.print_exc()