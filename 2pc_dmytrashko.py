import psycopg2

def postgre_conn(host='localhost', db='mydb', user='postgres', password='123456'):
    conn = psycopg2.connect(
        host=host,
        database=db,
        user=user,
        password=password)
    return conn

def init_tpc(*args):
    conn_num = 0
    for connection in args:
        conn_num += 1
        x_id = connection.xid(42, 'transaction ID', 'connection %s' %conn_num)
        connection.tpc_begin(x_id)

def commit(*args):
    for conn in args:
        conn.tpc_commit()

def conn_close(*args):
    for conn in args:
        conn.close()

def exec_command(conn, query, params = ''):
    cur = conn.cursor()
    cur.execute(query, params)
    return cur


def main():
    conn_fly = postgre_conn()
    conn_hotel= postgre_conn()
    conn_account = postgre_conn()
    init_tpc(conn_fly, conn_hotel, conn_account)

    fly_max_id = int(exec_command(conn_fly, query = '''SELECT MAX(booking_id) FROM fly.fly_booking;''').fetchone()[0])
    hotel_max_id = int(exec_command(conn_hotel, query = '''SELECT MAX(booking_id) FROM hotel.hotel_booking;''').fetchone()[0])

    try:
        exec_command(conn_fly, query = '''insert into fly.fly_booking(booking_id, client_name, fly_number, "from", "to", date) values (%s,%s,%s,%s,%s,%s);''', params = (fly_max_id+1, 'Nik', 'KLM 1382', 'KBP', 'AMS','01/05/2015'))
        conn_fly.tpc_prepare()
        exec_command(conn_hotel, query='''insert into hotel.hotel_booking(booking_id, client_name, hotel_name, arrival, departure) values (%s,%s,%s,%s,%s);''', params=(hotel_max_id+1, 'Nik', 'Hilton', '01/05/2015', '07/05/2015'))
        conn_hotel.tpc_prepare()
    except psycopg2.ProgrammingError:
        conn_fly.tpc_rollback()
        conn_hotel.tpc_rollback()
    else:
        try:
            exec_command(conn_account, query = '''UPDATE mydb.account.account_info SET ammount = ammount - 10 WHERE client_name = 'Nik';''')
            conn_account.tpc_prepare()
        except (psycopg2.ProgrammingError, psycopg2.DatabaseError) as errors:
            conn_account.tpc_rollback()
        else:
            # if comment first and uncomment second commit then transaction will fail and will be in wait status. If run script after that once again, fly and hotel commits will be in wait status
            commit(conn_fly, conn_hotel, conn_account)
            # commit(conn_fly, conn_hotel)

    conn_close(conn_fly, conn_hotel, conn_account)

main()