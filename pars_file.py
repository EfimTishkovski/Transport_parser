import concurrent.futures
import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import json
from tqdm import tqdm
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from add_load_data import main_add_load_func


# Функция запуска соединения с сервером
def launch():
    """
    Функция запуска, проверяет соединение с сайтом.
    :return: tru or false
    """
    try:
        # Создание объекта вэб драйвера
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument(argument='--headless')
        driver = webdriver.Chrome(options=chrome_options)
        # Проверка соединения с сайтом
        for num in range(5):
            try:
                driver.get('https://minsktrans.by/')
                print('Есть соединение с сайтом')
                driver.quit()
                return True  # Выход из цикла если соединение установлено
            except Exception as exc:
                print(exc)
                time.sleep(2)  # Задержка перед следующей попыткой  ?
        else:
            print('Ошибка : нет соединения с сайтом')
            driver.quit()
            return False

    except:
        print('Ошибка инициализации')
        driver.quit()
        return False


# Получение маршрутов автобусов и ссылок на их расписания
def routs(url, delay=2):
    """
    Функция получения маршрутов
    :param url: ссылка на страницу с маршрутами
    :param delay: Задержка после загрузки страницы
    :return: track_data словарь с данными, название и номер маршрута : ссылка
    """
    # Создание объекта вэб драйвера
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(argument='--headless')
    driver = webdriver.Chrome(options=chrome_options)

    driver.get(url)
    time.sleep(delay)
    data = driver.find_element(By.ID, 'routeList')
    item_data = data.find_elements(By.TAG_NAME, 'a')
    track_data = {}
    s_bar = tqdm(total=len(item_data), colour='yellow', desc='Маршруты')
    for element in item_data:
        track = element.find_element(By.TAG_NAME, 'h3')
        link = element.get_attribute('href')
        track_data[track.text] = link
        s_bar.update()
    s_bar.close()
    driver.quit()
    return track_data


# Функция получения остановок и ссылок на расписания по остановкам
def stops_transport_info(data, delay=2, iteration=5):
    """
    Функция получения ссылок названий остановок
    :param web_browser: Объект вэб драйвера
    :param URL: Ссылка на маршрут
    :param delay: Задержка после загрузки страницы
    :param iteration: Количество повторений при недогрузке страницы
    :return: словарь с выходными данными
    """

    # Запуск вэб драйвера
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(argument='--headless')
    driver = webdriver.Chrome(options=chrome_options)

    direct = {}  # Прямое направление маршрута
    reverse = {}  # Обратное
    name_rout = data[0]
    URL = data[1]
    for i in range(iteration):
        try:
            driver.get(URL)
            time.sleep(delay)
            data_from_list_stationA = driver.find_element(By.ID, 'tripA')
            name_stations_tripA = data_from_list_stationA.find_elements(By.TAG_NAME, 'a')
            data_from_list_stationB = driver.find_element(By.ID, 'tripB')
            name_stations_tripB = data_from_list_stationB.find_elements(By.TAG_NAME, 'a')
            # Прямое направление
            direct.clear()
            for element in name_stations_tripA:
                name_station = element.find_element(By.TAG_NAME, 'h6')
                link = element.get_attribute('href')
                direct[name_station.text] = link
            # Обратное направление
            reverse.clear()
            for element in name_stations_tripB:
                name_station = element.find_element(By.TAG_NAME, 'h6')
                link = element.get_attribute('href')
                reverse[name_station.text] = link
            station_data = {name_rout: {'Прямое направление': direct.copy(), 'Обратное направление': reverse.copy()}}
            break
        except:
            continue
    else:
        station_data = {name_rout: {'Прямое направление': '', 'Обратное направление': ''}}  # НЕ успешное завершение
    driver.quit()
    return station_data


# Функция получения расписания по остановкам
def get_time_list(url, wait_time=3, iteration=5):
    """
    Функция получения времени отправления по остановке
    :param URL: ссылка на страницу
    :param iteration: Количество попыток загрузки страницы
    :return: словарь с днями недели и временем отправления или '' если не удалось получить данные
    """
    # Дописать цикл извлечения ссылки из входного словаря
    # Создание подключения
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(argument='--headless')
    driver = webdriver.Chrome(options=chrome_options)

    week_days = {1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 4: 'Четверг',
                 5: 'Пятница', 6: 'Суббота', 7: 'Воскресенье'}

    temp_time = {}
    out_data_mass = {}

    # Переписать, защита не работает, поменять циклы местами.
    for i in range(iteration):
        try:
            driver.get(url)  # Подгружаем страницу
            time.sleep(wait_time)
            temp_time.clear()
            out_data_mass.clear()
            for day in range(1, 8):
                button = driver.find_element(By.XPATH,
                                             f'/html/body/div[2]/div/div[2]/div[4]/div/div[3]/div[2]/div[1]/button[{day}]')
                button.click()  # Щёлкает по кнопкам дней недели
                time.sleep(0.3)
                data_from_timelist = driver.find_element(By.ID, 'schedule')
                data_mass = data_from_timelist.text.split('\n')
                data_mass.pop(0)  # Убираем первый элемент "часы минуты" это лишнее
                # Сортировка данных
                for i in range(0, len(data_mass) - 1, 2):
                    temp_time[data_mass[i]] = tuple(data_mass[i + 1].split(' '))
                    out_data_mass[week_days[day]] = temp_time.copy()
            else:
                driver.quit()
                return {url: out_data_mass}  # Успешная отработка цикла

        except Exception:
            continue
    else:
        driver.quit()  # Закрытие драйвера если цикл отработал безуспешно
        return {url: ''}


################################### Проверить и тоже вынести в отдельный файл
"""
def complex_mass(mass):
    
    # Функция обработки нестандартного массива данных
    #типа: ['12', '34', '56']
    #:param mass: нестандартный массив на входе
    #:return: обработанный массив на выходе
    
    out = {}
    hour = mass[0]
    temp = []
    for i in range(1, len(mass) - 1):
        if int(mass[i]) == int(hour):
            continue
        elif int(mass[i]) < int(mass[i + 1]):
            temp.append(mass[i])
        else:
            temp.append(mass[i])
            out[hour] = tuple(temp.copy())
            hour = mass[i + 1]
            temp.clear()
    else:
        temp.append(mass[i + 1])
        out[hour] = tuple(temp.copy())
        temp.clear()
    return out

"""
################################### Проверить и тоже вынести в отдельный файл

# Функция закрытия соединений
def stop_func(base_object, cursor_object):
    """
    Функция завершения работы
    закрывает соединение с интернетом и БД
    :param base_object: Объект БД
    :param cursor_object: Объект курсора в БД
    """
    cursor_object.close()
    base_object.close()
    print('Соединения закрыты')


def main_get_data(url, base_name, reserve_file_copy=True, correct_data_test=False, max_workers=20):
    """
    Главня функция парсинга
    :param max_workers: количество потоков
    :param url: Ссылка (автобус, троллейбус, трамвай)
    :param base_name: Имя файла с базой данных
    :param reserve_file_copy: Резервное промежуточное копирование в файлы
    :param correct_data_test: проверка корректности времени отправления
    :return:
    """
    # Настройки
    speed = 3  # Задержка для загрузки страницы
    iteration = 10  # Количество повторений при недогрузке страницы

    # Соединение с базой
    base = sqlite3.connect(base_name)
    cursor = base.cursor()

    flag_launch = True  # Флаг запуска

    # Информационные сообщения
    if reserve_file_copy:
        print('Резервное копирование включено')

    if correct_data_test:
        print('Проверка корректности данных включена')

    # Запуск основной программы
    # Получение данных о маршрутах (номер - ссылка)
    if flag_launch:
        for i in range(iteration):
            try:
                routs_data = routs(url=url, delay=speed)
                break
            except:
                continue
        else:
            flag_launch = False
            print('Ошибка получения данных о маршрутах')

    # Сохранение данных
    if flag_launch:
        try:
            # Сохранение данных в файл
            if reserve_file_copy:
                with open('temp_roads.txt', 'w') as routs_data_file:
                    json.dump(routs_data, routs_data_file)

            # Запись данных в БД
            clear_routs_link_table_qwery = "DELETE FROM routs_link"  # Очистка таблицы от предыдущих записей
            cursor.execute(clear_routs_link_table_qwery)
            base.commit()
            for rout, link in routs_data.items():
                qwery_for_write_routs_link = "INSERT INTO routs_link (rout, link) VALUES (?, ?)"
                cursor.execute(qwery_for_write_routs_link, (rout, link))
            base.commit()
        except:
            print('Ошибка сохранения данных о маршрутах')
        else:
            print('OK')
    time.sleep(0.3)  # Задержка для более ровного вывода

    # Получение данных об остановках
    # [{маршрут0 : {'Прямое направление : {'остановка' : ссылка, ...}, 'Обратное направление' : {'остановка' : ссылка, ...}}}, {маршрут1 : ...}]
    no_load_page = 0
    if flag_launch:
        try:
            size = len(routs_data)
            stops_data = []
            s_bar = tqdm(total=size, colour='yellow', desc='Остановки')
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                stops_info = {executor.submit(stops_transport_info, data=line, delay=3, iteration=8): line for line in
                              routs_data.items()}
                for future in concurrent.futures.as_completed(stops_info):
                    try:
                        data = future.result()
                    except:
                        stops_data.append({})
                        no_load_page += 1
                        s_bar.update()
                    else:
                        stops_data.append(data)
                        s_bar.update()
            s_bar.close()
        except:
            print('Ошибка, данные об остановках не получены')

    if no_load_page > 0:
        print('Есть недогруженные страницы, количество:', no_load_page)

    # Сохранение данных
    if flag_launch:
        try:
            if reserve_file_copy:
                # Сохранение в файл
                if reserve_file_copy:
                    with open('temp_station.txt', 'w') as tram_station_data_file:
                        json.dump(stops_data, tram_station_data_file)

            # Запись данных в БД
            clear_routs_link_table_query = "DELETE FROM main_data"  # Очистка таблицы от предыдущих записей
            cursor.execute(clear_routs_link_table_query)
            base.commit()
            for element in stops_data:
                for rout, data in element.items():
                    for direction, stop_link in data.items():
                        for stop, link in stop_link.items():
                            query_for_write = "INSERT INTO main_data (rout, direction, stop, time, link) " \
                                              "VALUES (?, ?, ?, ?, ?)"
                            cursor.execute(query_for_write, (rout, direction, stop, link, link))
            base.commit()
        except:
            print('Ошибка сохранения данных об остановках')
        else:
            print('OK')

    time.sleep(0.3)
    # Подсчёт количества записей в таблице
    num_query = 'SELECT count(*) FROM main_data'
    cursor.execute(num_query)
    lines = cursor.fetchone()[0]
    print('Данные по остановкам получены, строк:', lines)

    #flag_launch = False

    # Получение времени отправления по остановкам
    if flag_launch:
        temp_mass = []  # Временный массив для данных для ссылок
        arrive_time_mass = []  # Массив для полученных данных

        for element_rout in stops_data:
            for name_rout, rout in element_rout.items():
                for direction, stops in rout.items():
                    for name_station, link_first in stops.items():
                        temp_mass.append(link_first)
        """
        temp_mass_1 = []
        for i in range(1000):
            temp_mass_1.append(temp_mass[i])
        """
        arrive_time_statusbar = tqdm(total=len(temp_mass), colour='yellow',
                                     desc='Расписания по остановкам')  # создание статус бара
        # Многопоточная обработка ссылок, на выходе [{ссылка : время},{ссылка : время},...]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            arrive_time = {executor.submit(get_time_list, url=link, wait_time=speed, iteration=iteration):
                               link for link in temp_mass}
            for future in concurrent.futures.as_completed(arrive_time):
                try:
                    data = future.result()
                except:
                    arrive_time_mass.append({'': ''})
                    arrive_time_statusbar.update()
                else:
                    arrive_time_mass.append(data)
                    arrive_time_statusbar.update()

        arrive_time_statusbar.close()
    else:
        flag_launch = False
        print('Ошибка, данные о времени отправления не получены')

    # Запись результатов в базу и подсчёт недогруженных страниц
    no_load_page_count = 0  # Недогруженные страницы
    if flag_launch:
        for line in arrive_time_mass:
            link = list(line.items())[0][0]
            arr_time = list(line.items())[0][1]
            if arr_time == '':
                no_load_page_count += 1
            else:
                query = "UPDATE main_data SET time = ? WHERE time = ?"
                parameters = (str(arr_time), str(link))
                cursor.execute(query, parameters)
        base.commit()
        print('OK')

    # Проверка и обработка недогрузки
    if no_load_page_count > 0:
        print('Есть недогруженные страницы, количество:', no_load_page_count)
        main_add_load_func(base_name, max_workers=25, loop=2)  # Функция догрузки недостающих строк
    else:
        print('Все страницы загружены')

    # Проверка и исправление некорректных строк
    # Проверка не корректных данных вынесена в отдельный файл
    # Вставить сюда функцию дя проверки

    stop_func(base_object=base, cursor_object=cursor)
    print('Завершение работы')


def data_base_file(base_bus, base_trolleybus, base_tram):
    """
    Функция проверки баз и таблиц в базах, если нет, то создание.
    :param base_bus:  Файл базы с автобусами
    :param base_trolleybus: Файл базы с троллейбусами
    :param base_tram: Файл базы с трамваями
    :return: try or false
    """

    base = [base_bus, base_trolleybus, base_tram]

    try:
        for name_base in base:
            base = sqlite3.connect(name_base)
            cursor = base.cursor()
            # Проверка и создание таблицы routs_link
            query = 'CREATE TABLE IF NOT EXISTS "routs_link" (' \
                    'rout TEXT, ' \
                    'link TEXT);'
            cursor.execute(query)
            # Проверка и создание таблицы main_data
            query = 'CREATE TABLE IF NOT EXISTS "main_data" (' \
                    'rout TEXT, ' \
                    'direction TEXT,' \
                    'stop TEXT,' \
                    'time TEXT,' \
                    'link TEXT);'
            cursor.execute(query)
            base.commit()
    except sqlite3.Error as error:
        print('Ошибка при проверке баз')
        print(error)
        return False
    else:
        print('Базы в порядке')
        return True


if __name__ == '__main__':
    # Ссылки на транспорт
    URL_BUS = ''  # Автобусы
    URL_TROLLEYBUS = 'https://minsktrans.by/lookout_yard/Home/Index/minsk#/routes/trolleybus'  # Троллейбусы
    URL_TRAM = 'https://minsktrans.by/lookout_yard/Home/Index/minsk#/routes/tram'  # Трамваи

    # Файлы с базами
    BASE_BUS = 'bus_data.db'  # База с данными о автобусах
    BASE_TROLLEYBUS = 'trolleybus_data.db'  # База с данными о троллейбусах
    BASE_TRAM = 'tram_data.db'  # База с данными о трамваях

    # Проверка соединения с сайтом
    net_flag = launch()

    # Проверка баз данных
    data_base_flag = data_base_file(BASE_BUS, BASE_TROLLEYBUS, BASE_TRAM)

    # Запуск основной функции
    if net_flag and data_base_flag:
        # main_get_data(URL_TRAM, BASE_TRAM, correct_data_test=False)
        main_get_data(URL_TROLLEYBUS, BASE_TROLLEYBUS, correct_data_test=False, max_workers=25)
