# Обработка данных, исправление некорректных данных

import sqlite3

def main_problem_data_func(name_base):

    # Получение данных из базы
    connection = sqlite3.connect(name_base)
    cursor = connection.cursor()
    get_data_query = 'SELECT * FROM main_data'
    cursor.execute(get_data_query)
    data_mass = cursor.fetchall()

    # Преобразование данных из строки в словарь
    # Проверка и поиск некорректных данных
    # Исправление некорректных данных

    return data_mass

if __name__ == '__main__':
    mass = main_problem_data_func('trolleybus_data.db')
