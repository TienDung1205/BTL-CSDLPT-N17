import mysql.connector

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='root', password='123456', dbname='dds_assgn1'):
    """
    Tạo kết nối đến MySQL.
    """
    connection = mysql.connector.connect(
        host='localhost',
        user=user,
        password=password,
        database=dbname
    )
    return connection

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Hàm để nạp dữ liệu từ file @ratingsfilepath vào bảng @ratingstablename.
    """
    create_db(DATABASE_NAME)
    cur = openconnection.cursor()
    # Xóa bảng nếu đã tồn tại
    cur.execute("DROP TABLE IF EXISTS {0}".format(ratingstablename))
    # Tạo bảng ratings
    cur.execute("CREATE TABLE {0} (userid INT, movieid INT, rating FLOAT)".format(ratingstablename))
     # Đọc và chèn dữ liệu
    batch = []
    count = 0

    with open(ratingsfilepath, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("::")
            if len(parts) == 4:
                user, movie, rating, _ = parts
                batch.append((int(user), int(movie), float(rating)))
                count += 1

                if len(batch) == 1000000:
                    cur.executemany(
                        f"INSERT IGNORE INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
                        batch
                    )
                    openconnection.commit()
                    print(f"Đã thêm {count} bản ghi vào bảng {ratingstablename}.")  # In tổng số dòng đã thêm
                    batch.clear()

    if batch:
        cur.executemany(
            f"INSERT IGNORE INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
            batch
        )
        openconnection.commit()
        print(f"Đã thêm {count} bản ghi vào bảng {ratingstablename}.")  # Thông báo cho batch cuối

    cur.close()
    print(f"Tải dữ liệu thành công: {count} bản ghi đã thêm vào bảng {ratingstablename}.")

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm để tạo các phân vùng của bảng chính dựa trên khoảng giá trị rating.
    """
    cur = openconnection.cursor()
    delta = 5.0 / numberofpartitions
    boundaries = [i * delta for i in range(numberofpartitions + 1)]
    for i in range(numberofpartitions):
        table_name = f"range_part{i}"
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INT,
                movieid INT,
                rating FLOAT
            )
        """)
        lower_bound = boundaries[i]
        upper_bound = boundaries[i + 1]
        if i == 0:
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM {ratingstablename}    
                WHERE rating >= %s AND rating <= %s
            """, (lower_bound, upper_bound))
        else:
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM {ratingstablename}
                WHERE rating > %s AND rating <= %s
            """, (lower_bound, upper_bound))
    openconnection.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm để tạo các phân vùng của bảng chính theo phương pháp round-robin.
    """
    cur = openconnection.cursor()
    # Tạo bảng phân mảnh
    for i in range(numberofpartitions):
        table_name = f"rrobin_part{i}"
        cur.execute("DROP TABLE IF EXISTS {0}".format(table_name))
        cur.execute("CREATE TABLE {0} (userid INT, movieid INT, rating FLOAT)".format(table_name))
        cur.execute(f"""INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating
                    FROM (
                        SELECT 
                            ROW_NUMBER() OVER () AS rnum,
                            userid,
                            movieid,
                            rating
                        FROM ratings
                    ) AS temp
                    WHERE (rnum-1) % {numberofpartitions} = {i}
        """)
        print(f"Đã tạo phân vùng {table_name} với round-robin.")
    openconnection.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm để chèn một dòng mới vào bảng chính và phân vùng round-robin.
    """
    cur = openconnection.cursor()
    # Chèn vào bảng chính
    cur.execute("INSERT INTO {0} (userid, movieid, rating) VALUES ({1}, {2}, {3})".format(ratingstablename, userid, itemid, rating))
    # Tìm phân mảnh round-robin
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE 'rrobin_part%'")
    numberofpartitions = cur.fetchone()[0]
    table_name = f"rrobin_part{(total_rows - 1) % numberofpartitions}"
    cur.execute(f"""
        INSERT INTO {table_name} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, itemid, rating))
    openconnection.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm để chèn một dòng mới vào bảng chính và phân vùng dựa trên rating.
    """
    cur = openconnection.cursor()
    # Chèn vào bảng chính
    cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, itemid, rating))
    # Tìm phân mảnh dựa trên rating
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE 'range_part%'")
    numberofpartitions = cur.fetchone()[0]
    delta = 5.0 / numberofpartitions
    index = min(int(rating / delta) - 1 if rating == int(rating) else int(rating / delta), numberofpartitions - 1)
    if index < 0:
        index = 0
    table_name = f"range_part{index}"
    cur.execute(f""" 
        INSERT INTO {table_name} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, itemid, rating))
    openconnection.commit()
    cur.close()

def create_db(dbname):
    """
    Tạo một cơ sở dữ liệu bằng cách kết nối tới MySQL.
    """
    con = mysql.connector.connect(host='localhost', user='root', password='123456')
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s", (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("CREATE DATABASE %s", (dbname,))

    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Hàm để đếm số bảng có tên bắt đầu với @prefix.
    """
    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE %s", (f"{prefix}%",))
    count = cur.fetchone()[0]
    cur.close()
    return count