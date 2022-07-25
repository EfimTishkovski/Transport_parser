# Обработка недогруженных данных

import concurrent.futures
import sqlite3
from pars_file import get_time_list
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


def main_add_load_func(name_database, max_workers=20):
    """
    Функция догрузки строк по которым данные не бвли получены.
    Сама связывается с базой, чситает количество и догружает.
    Работает в цикле пока недогруженных строк не останется.
    :param name_database: База данных
    :param max_workers: Количество потоков при загрузке
    :return: True если работа завершена успешно False при ошибке во время работы
    """
    connection = sqlite3.connect(name_database)
    cursor = connection.cursor()

    # Получение начальных данных перед запуском цикла
    out_mass = []
    get_link_query = 'SELECT time FROM main_data'
    cursor.execute(get_link_query)
    data_from_base = cursor.fetchall()
    # Поиск ссылок в базе и добавление их в массив
    link_data = [element[0] for element in data_from_base if element[0][0:5] == 'https']
    print(f'Найдено {len(link_data)} недогруженных страниц')

    # Цикл запуска парсинга, пока не останется не догруженных страниц
    endless_loop_count = 0
    while len(link_data) != 0:
        # Досрочное завершение цикла, только на момент отладки
        if endless_loop_count == 2:
            print(f'Цикл завершён на {endless_loop_count} итерации')
            cursor.close()
            connection.close()
            return True

        # Парсинг
        try:
            s_bar = tqdm(total=len(link_data), colour='BLUE', desc='Повторная загрузка страниц')
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                new_data_mass = {executor.submit(get_time_list, URL=link, wait_time=3, iteration=10):
                                 link for link in link_data}
                for future in concurrent.futures.as_completed(new_data_mass):
                    try:
                        data = future.result()
                    except:
                        out_mass.append({'' : ''})
                        s_bar.update()
                    else:
                        out_mass.append(data)
                        s_bar.update()
            s_bar.close()
        except:
            print('Ошибка парсинга')
            cursor.close()
            connection.close()
            return False

        # Запись в базу новых данных
        try:
            for line in out_mass:
                link = list(line.items())[0][0]
                arr_time = list(line.items())[0][1]
                if arr_time != '':
                    query = "UPDATE main_data SET time = ? WHERE time = ?"
                    parameters = (str(arr_time), str(link))
                    cursor.execute(query, parameters)
            connection.commit()
            print('OK')
            endless_loop_count += 1
        except:
            print('Ошибка записи новых данных в базу')
            cursor.close()
            connection.close()
            link_data.clear()
            new_data_mass.clear()
            return False

        link_data.clear()
        new_data_mass.clear()

        try:
            # Получение ссылок из базы
            get_link_query = 'SELECT time FROM main_data'
            cursor.execute(get_link_query)
            data_from_base = cursor.fetchall()
            link_data = [element[0] for element in data_from_base if element[0][0:5] == 'https']
            print(f'Найдено {len(link_data)} недогруженных страниц')
        except sqlite3.Error as error:
            print('Ошибка базы данных:', error)
            return False

    else:
        print('Все строки догружены, цикл завершён.')
        cursor.close()
        connection.close()
        link_data.clear()
        return True  # Выход из цикла


if __name__ == '__main__':
    main_add_load_func('trolleybus_data.db', max_workers=30)
