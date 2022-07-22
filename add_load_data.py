import sqlite3

def main_add_load_func(name_database):

    connection = sqlite3.connect(name_database)
    cursor = connection.cursor()

    get_link_query = 'SELECT time FROM main_data'
    cursor.execute(get_link_query)
    data = cursor.fetchall()
    out_data = [element[0] for element in data if element[0][0:5] == 'https']

    return out_data

if __name__ == '__main__':
    mass = main_add_load_func('trolleybus_data.db')
    print(len(mass))