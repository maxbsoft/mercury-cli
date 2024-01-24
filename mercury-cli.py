import argparse
import sys
import os
import threading
import time
import requests
import csv
import io
from cassandra.cluster import Cluster, NoHostAvailable, Session, ResultSet
from cassandra.auth import PlainTextAuthProvider
from cassandra import ReadTimeout, WriteTimeout, InvalidRequest, Unauthorized, AuthenticationFailed, ConsistencyLevel
from cassandra.query import BatchStatement, BatchType, SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args

def main():
    parser = argparse.ArgumentParser(description="Mercury CLI")

    # Mandatory argument for selecting the command
    parser.add_argument(
        'command',
        choices=['createdb', 'fillbaselist', 'removedata', 'checkbaselist'],
        help='Command type: createdb, removedata or fillbaselist'
    )

    # Optional arguments with default values
    parser.add_argument('--keyspace', default='mercure', help='Keyspace for the scylladb database  (default: mercure)')
    parser.add_argument('--db', default='127.0.0.1', help='IP address of ScyllaDB database (default: 127.0.0.1)')
    parser.add_argument('--db-port', type=int, default=9042, help='ScyllaDB database port (default: 9042)')


    parser.add_argument('--base-list-file', help='Path to the list data file')
    parser.add_argument('--base-list-group', type=int, help='Indicates which data group or source RY is 1, RP is 2, etc...')

    args = parser.parse_args()

    # Logic for various commands
    if args.command == 'createdb':
        run_createdb(args)
    elif args.command == 'fillbaselist':
        run_fillbaselist(args)
    elif args.command == 'removedata':
        run_removedata(args)
    elif args.command == 'checkbaselist':
        run_checkbaselist(args)
    else:
        print("Unknown command type")
        sys.exit(1)
def connect_to_scylladb(ip, port):
    # auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')
    # cluster = Cluster([ip], port=port, auth_provider=auth_provider)
    cluster = Cluster([ip], port=port)
    session = cluster.connect()
    print(f"Cluster name: {cluster.metadata.cluster_name}")
    # print(f"Cluster name: {cluster.metadata}")
    return session, cluster

def disconnect_from_scylladb(session, cluster):
    session.shutdown()
    cluster.shutdown()

def create_db_structure(session, keyspace):
    print(f"Try to create keyspace: {keyspace} if it does not exist...")
    result = session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 1}};
    """)
    print(f"result:{result}")
    print(f"Set keyspace: {keyspace}")
    result = session.set_keyspace(keyspace)
    print(f"result:{result}")

    print("Trying to create a baselist table...")
    result = session.execute("""
        CREATE TABLE IF NOT EXISTS baselist (
            data text PRIMARY KEY,
            group smallint
        )
    """)
    # WITH compression = {};
    print(f"result:{result}")

    # try:
    #     print("Trying to create an index for baselist (group)...")
    #     result = session.execute("CREATE INDEX ON baselist (group);")
    #     print(f"result:{result}")
    # except InvalidRequest:  # Замените на соответствующее исключение
    #     print("Index already exists.")
    
# def cleanup_test_data(session):
#     # try:
#     #     # Пытаемся выполнить запрос SELECT к таблице
#     #     session.execute("SELECT * FROM testkeyspace.testtable LIMIT 1;")
#     #     # Если запрос выполнен успешно, таблица существует, и мы можем ее очистить
#     #     session.execute("TRUNCATE testkeyspace.testtable;", timeout=60)
#     # except InvalidRequest:
#     #     # Исключение InvalidRequest будет вызвано, если таблица не существует
#     #     print("Таблица не существует, пропускаем TRUNCATE.")
#     session.execute("DROP KEYSPACE IF EXISTS testkeyspace;", timeout=360)

# def generate_test_data(row_count):
#     test_data = []
#     for i in range(row_count):
#         # Генерация 32-байтового уникального идентификатора
#         id_bytes = os.urandom(32)

#         # Создание строки данных
#         data_str = f"Test performance, тестdata {i}"

#         # Добавление кортежа (id, data) в список
#         test_data.append((id_bytes, data_str))

#     return test_data

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
    
# def insert_data(session: Session, data_chunk):
#     # batch = BatchStatement(BatchType.UNLOGGED)
#     for id_bytes, data_str in data_chunk:
#         session.execute("INSERT INTO testtable (id, data) VALUES (%s, %s)", (id_bytes, data_str))

# def worker(session, data_chunk, counter):
#     start_time = time.time()
#     insert_data(session, data_chunk)
#     elapsed_time = time.time() - start_time
#     rows_inserted = len(data_chunk)
#     counter.append(rows_inserted)
#     print(f"Поток завершил вставку {rows_inserted} строк за {elapsed_time:.2f} секунд. Скорость: {rows_inserted/elapsed_time:.2f} строк/сек")

def run_createdb(args):
    print(f"Running a 'createdb' structure with keyspace:{args.keyspace}")
    try:
        print(f"Connecting to ScyllaDB {args.db}:{args.db_port}...")
        session, cluster = connect_to_scylladb(args.db, args.db_port)
       
        print(f"Creating db structure...")
        create_db_structure(session, args.keyspace)

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
        # Disconnect even if errors occurred
        if 'session' in locals() and 'cluster' in locals():
            print(f"Disconnecting from scylladb...")
            disconnect_from_scylladb(session, cluster)

def run_removedata(args):
    print(f"Running a 'run_removedata' with keyspace:{args.keyspace}")
    try:
        print(f"Connecting to ScyllaDB {args.db}:{args.db_port}...")
        session, cluster = connect_to_scylladb(args.db, args.db_port)
       
        session.execute(f"DROP KEYSPACE IF EXISTS {args.keyspace};", timeout=360)
        # create_db_structure(session, args.keyspace)

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
        # Disconnect even if errors occurred
        if 'session' in locals() and 'cluster' in locals():
            print(f"Disconnecting from scylladb...")
            disconnect_from_scylladb(session, cluster)

def run_fillbaselist(args):
    # Here is the logic for the fromfile test
    print(f"Running the 'fillbaselist' command fills the data into the baselist table from the {args.base_list_file} file. Keyspace:{args.keyspace}")
    try:
        total_lines = 2000000000 # sum(1 for line in open(args.base_list_file, 'r', encoding='ascii', errors='replace'))
        # for line in open(args.base_list_file, 'r', errors='ignore'):
        #     total_lines = total_lines + 1
        #     if total_lines % 1000000 == 0:
        #         print("Lines count:", total_lines)
        # print(f"Total lines in file: {total_lines}")

        processed_lines = 0
        additional_data={
            'keyspace': args.keyspace,
            'table': 'baselist'
        }
        with open(args.base_list_file, 'r', encoding='utf-8', errors='replace') as file:
            csv_output = io.StringIO()
            writer = csv.writer(csv_output, quotechar='"', quoting=csv.QUOTE_ALL)

            count = 0
            skip = 0

            for line in file:
                
                stripped_line = line.strip()
                escaped_line = stripped_line.replace("\\", "\\\\")
                escaped_line = escaped_line.replace(",", "\,")
                # Преобразование строки файла в формат CSV (подставьте нужные данные)
                if escaped_line:
                    skip = skip + 1
                    if skip < 237500000:
                        continue
                    writer.writerow([escaped_line, args.base_list_group])
                else:
                    continue

                count += 1
                processed_lines += 1
                if count >= 500000:  # Проверка на достижение порога в 1 миллион строк
                    # Отправка данных
                    csv_output.seek(0)
                    isComplete = False
                    while isComplete == False:
                        print("Try upload csv...")
                        response = requests.post(
                            f"http://{args.db}:5001/upload_csv",
                            files={"file": csv_output},
                            data=additional_data
                        )
                        if response.status_code == 200:
                            isComplete = True
                        else:
                            print(f"Error: {response.text}")
                            print(f"Neet repeat. Wait 10 seconds...")
                            time.sleep(10)
                     



                    # Отображение ответа сервера
                    print(f"Processed {processed_lines} of {total_lines} lines. Response: {response.text}")
                    
                    # Сброс счётчика и очистка буфера
                    count = 0
                    csv_output.seek(0)
                    csv_output.truncate(0)

            # Отправка оставшихся данных (если они есть)
            if count > 0:
                csv_output.seek(0)
                response = requests.post(
                    f"http://{args.db}:5001/upload_csv",
                    files={"file": csv_output},
                    data=additional_data
                )
                print(f"Processed {processed_lines} of {total_lines} lines. Response: {response.text}")

    except FileNotFoundError:
        print(f"File not found: {args.base_list_file}")
    except Exception as e:
        print(f"An error occurred: {e}")
        print(e)

def run_checkbaselist(args):
    print(f"Running the 'checkbaselist' command. Keyspace:{args.keyspace}")
    try:
        print(f"Connecting to ScyllaDB {args.db}:{args.db_port}...")
        session, cluster = connect_to_scylladb(args.db, args.db_port)
        session.set_keyspace(args.keyspace)
        start = time.time()
        query = SimpleStatement("SELECT * FROM baselist WHERE data='!root'", consistency_level=ConsistencyLevel.LOCAL_ONE)
        rows: ResultSet = session.execute(query, timeout=200)
        count = 0
        print('len(rows):', rows.one())
        print('time:', time.time() - start)
        # rows.fetch_next_page()

        # for row in rows:
        #     count = count + 1
        #     if count % 1000 == 0:
        #         print('count:', count)
        # print('count:', count)
        # print(f"Creating db structure...")
        # create_db_structure(session, args.keyspace)

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
        # Disconnect even if errors occurred
        if 'session' in locals() and 'cluster' in locals():
            print(f"Disconnecting from scylladb...")
            disconnect_from_scylladb(session, cluster)
    

if __name__ == "__main__":
    main()