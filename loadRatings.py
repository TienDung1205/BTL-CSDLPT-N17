BATCH_SIZE = 100000

def LoadRatings(conn, file_path, table_name, user_col, movie_col, rating_col, batch_size=BATCH_SIZE):
    cur = conn.cursor()

    # Tạo bảng nếu chưa có, dùng các tham số truyền vào
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {user_col} INT,
            {movie_col} INT,
            {rating_col} FLOAT
        );
    """)
    conn.commit()

    batch = []
    count = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("::")
            if len(parts) == 4:
                user, movie, rating, _ = parts
                batch.append((int(user), int(movie), float(rating)))
                count += 1

                if len(batch) == batch_size:
                    cur.executemany(
                        f"INSERT IGNORE INTO {table_name} ({user_col}, {movie_col}, {rating_col}) VALUES (%s, %s, %s)",
                        batch
                    )
                    conn.commit()
                    print(f"Đã thêm {count} dòng vào bảng {table_name}.")  # In tổng số dòng đã thêm
                    batch.clear()

    if batch:
        cur.executemany(
            f"INSERT IGNORE INTO {table_name} ({user_col}, {movie_col}, {rating_col}) VALUES (%s, %s, %s)",
            batch
        )
        conn.commit()
        print(f"Đã thêm {count} dòng vào bảng {table_name}.")  # Thông báo cho batch cuối

    cur.close()
    print(f"Tải dữ liệu thành công: {count} dòng được chèn vào bảng {table_name}.")
