import os
import pymysql.cursors
import hashlib

# specify your folder location
folder_location = '/path/to/your/folder'

# setup database connection
try:
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='yourpassword',
        db='yourdatabase',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
except pymysql.MySQLError as e:
    print(f"Error while connecting to MySQL: {e}")
    exit(1)

def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None
    return hash_md5.hexdigest()

def get_file_size(file_path):
    try:
        return os.path.getsize(file_path)
    except Exception as e:
        print(f"Error getting size of file {file_path}: {e}")
        return None

try:
    with connection.cursor() as cursor:
        for root, dirs, files in os.walk(folder_location):
            for filename in files:
                file_path = os.path.join(root, filename)
                try:
                    sql = "SELECT * FROM file_info WHERE filename=%s AND folder_location=%s"
                    cursor.execute(sql, (filename, root))
                    result = cursor.fetchone()
                    if not result:
                        md5_hash = calculate_md5(file_path)
                        file_size = get_file_size(file_path)
                        if md5_hash is None or file_size is None:
                            continue
                        sql = "INSERT INTO file_info (filename, folder_location, md5_hash, file_size) VALUES (%s, %s, %s, %s)"
                        cursor.execute(sql, (filename, root, md5_hash, file_size))
                except pymysql.MySQLError as e:
                    print(f"Error executing SQL: {e}")
    connection.commit()
except pymysql.MySQLError as e:
    print(f"Error while interacting with MySQL: {e}")
finally:
    connection.close()
