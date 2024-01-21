import argparse
import sys
import os
import threading
import time
import requests
import csv
import io
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra import ReadTimeout, WriteTimeout, InvalidRequest, Unauthorized, AuthenticationFailed
from cassandra.query import BatchStatement, BatchType
from cassandra.concurrent import execute_concurrent_with_args

def main():
    parser = argparse.ArgumentParser(description="ScyllaDB performance test")

    # Mandatory argument for selecting the test type
    parser.add_argument('test_type', choices=['random', 'fromfile'], help='Test type: random or fromfile')

    # Optional arguments with default values
    parser.add_argument('--row-count', type=int, default=1000000, help='Number of data rows (default: 1000000)')
    parser.add_argument('--db', default='127.0.0.1', help='IP address of ScyllaDB database (default: 127.0.0.1)')
    parser.add_argument('--db-port', type=int, default=9042, help='ScyllaDB database port (default: 9042)')
    parser.add_argument('--threads', type=int, default=1, help='Number of threads for parallel recording (default: 1)')

    # Argument for the test fromfile
    parser.add_argument('--data-list-file', help='Path to the test data file fromfile')

    args = parser.parse_args()

    # Logic for various tests
    if args.test_type == 'random':
        run_random_test(args)
    elif args.test_type == 'fromfile':
        run_fromfile_test(args)
    else:
        print("Unknown test type")
        sys.exit(1)
def connect_to_scylladb(ip, port):
    # auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')
    # cluster = Cluster([ip], port=port, auth_provider=auth_provider)
    cluster = Cluster([ip], port=port)
    session = cluster.connect()
    print(f"Имя кластера: {cluster.metadata.cluster_name}")
    return session, cluster

def disconnect_from_scylladb(session, cluster):
    session.shutdown()
    cluster.shutdown()

def create_test_table(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS testkeyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
    """)
    session.set_keyspace("testkeyspace")
    session.execute("""
        CREATE TABLE IF NOT EXISTS testtable (
            id blob PRIMARY KEY,
            data text
        ) WITH compression = {};
    """)
    
    
def cleanup_test_data(session):
    # try:
    #     # Пытаемся выполнить запрос SELECT к таблице
    #     session.execute("SELECT * FROM testkeyspace.testtable LIMIT 1;")
    #     # Если запрос выполнен успешно, таблица существует, и мы можем ее очистить
    #     session.execute("TRUNCATE testkeyspace.testtable;", timeout=60)
    # except InvalidRequest:
    #     # Исключение InvalidRequest будет вызвано, если таблица не существует
    #     print("Таблица не существует, пропускаем TRUNCATE.")
    session.execute("DROP KEYSPACE IF EXISTS testkeyspace;", timeout=360)

def generate_test_data(row_count):
    test_data = []
    for i in range(row_count):
        # Генерация 32-байтового уникального идентификатора
        id_bytes = os.urandom(32)

        # Создание строки данных
        data_str = f"Test performance, тестdata {i}"

        # Добавление кортежа (id, data) в список
        test_data.append((id_bytes, data_str))

    return test_data

# def insert_data(session, data_chunk):
#     batch = BatchStatement()
#     for id_bytes, data_str in data_chunk:
#         batch.add("INSERT INTO testkeyspace.testtable (id, data) VALUES (%s, %s)", (id_bytes, data_str))
#     session.execute(batch, timeout = 120)
# def insert_data(session, data_chunk):
#     insert_query = "INSERT INTO testkeyspace.testtable (id, data) VALUES (%s, %s)"
#     # Выполнение запросов асинхронно
#     print(f'insert_data {len(data_chunk)} concurrency:100')
#     execute_concurrent_with_args(session, insert_query, data_chunk, concurrency=36)

# def insert_data(session, data_chunk):
#     batch = BatchStatement(BatchType.UNLOGGED)
#     for id_bytes, data_str in data_chunk:
#        batch.add("INSERT INTO testtable (id, data) VALUES (%s, %s)", (id_bytes, data_str))
#     session.execute(batch, timeout = 120)
    
def insert_data(session: Session, data_chunk):
    # batch = BatchStatement(BatchType.UNLOGGED)
    for id_bytes, data_str in data_chunk:
        session.execute("INSERT INTO testtable (id, data) VALUES (%s, %s)", (id_bytes, data_str))

def worker(session, data_chunk, counter):
    start_time = time.time()
    insert_data(session, data_chunk)
    elapsed_time = time.time() - start_time
    rows_inserted = len(data_chunk)
    counter.append(rows_inserted)
    print(f"Поток завершил вставку {rows_inserted} строк за {elapsed_time:.2f} секунд. Скорость: {rows_inserted/elapsed_time:.2f} строк/сек")

def run_random_test(args):
    # Here will be the logic for the random test
    print(f"Running a 'random' test with {args.row_count} rows")
    try:
        print(f"Connecting to ScyllaDB {args.db}:{args.db_port}...")
        session, cluster = connect_to_scylladb(args.db, args.db_port)
        cleanup_test_data(session)

        print(f"Creating test table...")
        create_test_table(session)

        # Here will be the logic for inserting the data
        # Генерация тестовых данных
        test_data = generate_test_data(args.row_count)

        start_time = time.time()
         # Разбиваем данные на части для потоков
        chunks = [test_data[i::args.threads] for i in range(args.threads)]
        
        threads = []
        counter = []
        for chunk in chunks:
            t = threading.Thread(target=worker, args=(session, chunk, counter))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        elapsed_time = time.time() - start_time
        total_rows = sum(counter)
        print(f"Всего вставлено {total_rows} строк за {elapsed_time} сек. Скорость: {total_rows/elapsed_time:.2f} строк/сек")

        rows = session.execute("SELECT COUNT(1) FROM testtable", timeout=500)
        print("rows:", rows[0])

    except NoHostAvailable:
        print(f"Connection error: Failed to connect to ScyllaDB at {args.db}:{args.db_port}. Check the IP address and port.")
    except AuthenticationFailed:
        print("Authentication error: Incorrect credentials.")
    except (ReadTimeout, WriteTimeout):
        print("Timeout error: The read/write operation took too long.")
    except InvalidRequest as e:
        print(f"Query execution error: {e}")
    except Unauthorized:
        print("Access error: insufficient permissions to perform the operation.")
    except Exception as e:
        print(f"Unknown error: {e}")
    finally:
        # Clear test data and disconnect even if errors occurred
        if 'session' in locals() and 'cluster' in locals():
            print(f"Cleanup test data...")
            cleanup_test_data(session)
            print(f"Disconnecting from scylladb...")
            disconnect_from_scylladb(session, cluster)

def run_fromfile_test(args):
    # Here is the logic for the fromfile test
    print(f"Running the 'fromfile' test with a file {args.data_list_file}")
    try:
        print(f"Connecting to ScyllaDB {args.db}:{args.db_port}...")
        session, cluster = connect_to_scylladb(args.db, args.db_port)
        cleanup_test_data(session)

        print(f"Creating test table...")
        create_test_table(session)

        # Here will be the logic for inserting the data
        # Генерация тестовых данных
        test_data = generate_test_data(args.row_count)

        output = io.StringIO()
        writer = csv.writer(output)

        for row in test_data:
            writer.writerow([row[0].hex(), row[1]])

        # Перемещение указателя в начало файла
        output.seek(0)

        # Словарь для передачи дополнительных данных
        additional_data = {
            'keyspace': 'testkeyspace',
            'table': 'testtable'
        }

        start_time = time.time()

        response = requests.post(
            f"http://{args.db}:5001/upload_csv",
            files={"file": output},
            data=additional_data
        )

        # Вывод ответа сервера
        print(response.text)

        elapsed_time = time.time() - start_time
        total_rows = args.row_count
        print(f"Всего вставлено {total_rows} строк за {elapsed_time} сек. Скорость: {total_rows/elapsed_time:.2f} строк/сек")

        rows = session.execute("SELECT COUNT(1) FROM testtable", timeout=500)
        print("rows:", rows[0])

    except NoHostAvailable:
        print(f"Connection error: Failed to connect to ScyllaDB at {args.db}:{args.db_port}. Check the IP address and port.")
    except AuthenticationFailed:
        print("Authentication error: Incorrect credentials.")
    except (ReadTimeout, WriteTimeout):
        print("Timeout error: The read/write operation took too long.")
    except InvalidRequest as e:
        print(f"Query execution error: {e}")
    except Unauthorized:
        print("Access error: insufficient permissions to perform the operation.")
    except Exception as e:
        print(f"Unknown error: {e}")
    finally:
        # Clear test data and disconnect even if errors occurred
        if 'session' in locals() and 'cluster' in locals():
            print(f"Cleanup test data...")
            cleanup_test_data(session)
            print(f"Disconnecting from scylladb...")
            disconnect_from_scylladb(session, cluster)

if __name__ == "__main__":
    main()