import concurrent.futures
import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import json
from tqdm import tqdm
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor


# Функция запуска соединения с сервером
def launch(base_name, attempts=3):
    """
    Функция запуска, проверяет соединение с сайтом и базой.
    :param attempts : количество попыток проверки
    :param base_name : имя файла с базой
    :return: base, cursor : объект базы, объект курсора
    """
    try:
        # Создание объекта вэб драйвера
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument(argument='--headless')
        driver = webdriver.Chrome(options=chrome_options)
        # Проверка соединения с сайтом
        for num in range(attempts):
            try:
                driver.get('https://minsktrans.by/')
                break  # Выход из цикла если соединение установлено
            except Exception as exc:
                print(exc)
                time.sleep(2)  # Задержка перед следующей попыткой
        else:
            print('Ошибка инициализации: нет соединения с сайтом')
            return False
        # Создание соединения с БД
        try:
            base_object = sqlite3.connect(base_name)  # Создание объекта базы
            cursor_object = base_object.cursor()  # Создание объекта курсора
        except:
            print('Ошибка инициализации: нет соединения с базой')
            return False
        # Если все соединения установлены, то возвращаем объекты
        return base_object, cursor_object
    except:
        print('Ошибка инициализации')
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
        station_data = {name_rout: {'Прямое направление': '', 'Обратное направление': ''}}
    driver.quit()
    return station_data


def get_time_list(URL, wait_time=3, iteration=5):
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

    driver.get(URL)  # Подгружаем страницу
    time.sleep(wait_time)
    week_days = {1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 4: 'Четверг',
                 5: 'Пятница', 6: 'Суббота', 7: 'Воскресенье'}

    temp_time = {}
    out_data_mass = {}

    # Переписать, защита не работает, поменять циклы местами.
    for day in range(1, 8):
        for i in range(iteration):
            try:
                temp_time.clear()
                button_monday = driver.find_element(By.XPATH,
                                                         f'/html/body/div[2]/div/div[2]/div[4]/div/div[3]/div[2]/div[1]/button[{day}]')
                button_monday.click()  # Щёлкает по кнопкам дней недели
                time.sleep(0.3)
                data_from_time_list = driver.find_element(By.ID, 'schedule')
                data_mass = data_from_time_list.text.split('\n')
                data_mass.pop(0)  # Убираем первый элемент "часы минуты" это лишнее
                # Сортировка данных
                for i in range(0, len(data_mass) - 1, 2):
                    temp_time[data_mass[i]] = tuple(data_mass[i + 1].split(' '))
                    out_data_mass[week_days[day]] = temp_time.copy()
                break
            except Exception:
                continue
        else:
            driver.quit()  # Закрытие драйвера если цикл отработал безуспешно
            return {URL : ''}
    driver.quit()          # Закрытие драйвера если цикл завершён нормально
    return {URL : out_data_mass}


def complex_mass(mass):
    """
    Функция обработки нестандартного массива данных
    типа: ['12', '34', '56']
    :param mass: нестандартный массив на входе
    :return: обработанный массив на выходе
    """
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

###################################### Блок вынесен в отдельный файл
def hours_digit_test(mass):
    """
    Функция проверки корректности значений часов в массиве
    :param mass: Массив с данными для анализа
    :return: True or False и сообщение об ошибке
    """
    for digit in mass:
        if digit != ' ' and len(digit) <= 2 and 0 <= int(digit) <= 23:
            continue
        else:
            return False, 'Часы не в рамках 0 < 23'
    # Проверка на 00 в конце в 24 часовом формате
    if int(mass[-1]) == 0:
        n = 2
    else:
        n = 1
    # Проверка на не убывание часов, что идут по порядку
    for i in range(len(mass) - n):
        if mass[i] < mass[i + 1]:
            continue
        else:
            return False, 'Часы расположены не по порядку'
    else:
        out_flag = True
    return out_flag, ''


def correct_time_data(data_dikt):
    """
    Функция проверки на корректность данных о времени отправления с остановки
    :param data_dikt: Входные данные в виде словаря
    :return: True or False и текст ошибки
    """
    week_days_mass = []  # массив с днями недели
    tims_mass = []  # Массив с расписанием по дням недели
    # проверка 1 кол-во дней недели совпадет с кол-вом времени отправления по дням
    for week_days, tims in data_dikt.items():
        week_days_mass.append(week_days)
        tims_mass.append(tims)
    if len(tims_mass) != len(week_days_mass):
        return False, 'Пропущен день недели'

    # Проверка 2 каждому часу соответствует массив с минутами
    hours_mass = []  # Массив для часов
    minute_mass = []  # Массив для минут
    for line in tims_mass:
        hours_mass.clear()
        minute_mass.clear()
        for hour, minute in line.items():
            hours_mass.append(hour)
            minute_mass.append(minute)
        if len(hours_mass) != len(minute_mass):
            return False, 'Количество часов и массивов минут не совпадают'
        # Проверка на корректность часов в расписании
        hours_flag, error_hours_digit_test = hours_digit_test(hours_mass)
        if hours_flag:
            return True, ''
        else:
            return False, error_hours_digit_test
    return True, ''
###################################### Блок вынесен в отдельный файл

def defense_of_noload():
    pass


def main_get_data(URL, base_name, reserve_file_copy=True, correct_data_test=False, max_workers=20, deep_step_work=3):
    """
    Главня функция парсинга
    :param URL: Ссылка (автобус, троллейбус, трамвай)
    :param base_name: Имя файла с базой данных
    :param reserve_file_copy: Резервное промежуточное копирование в файлы
    :param correct_data_test: проверка корректности времени отправления
    :return:
    """
    # Настройки
    speed = 3  # Задержка для загрузки страницы

    # Запуск
    flag_launch = False  # Флаг запуска
    objects = launch(base_name=base_name)  # Запуск инициализации
    base, cursor = objects  # Получение объектов вэб драйвер, соединение с БД и курсор
    # Фильтрация входного параметра работы главной функции по частям (отладочная часть)
    if 1 <= deep_step_work <= 3:
        pass
    else:
        deep_step_work = 3

    # Дописать проверку файла базы данных и/или его создание

    # Информационные сообщения
    if reserve_file_copy:
        print('Резервное копирование включено')

    if correct_data_test:
        print('Проверка корректности данных включена')

    # Проверка инициализации
    if objects:
        print('Запуск выполнен успешно')
        flag_launch = True
    else:
        print('Запуск НЕ выполнен')

    # Запуск основной программы
    # Получение данных о маршрутах (номер - ссылка)
    iteration = 5
    if flag_launch:
        for i in range(iteration):
            try:
                routs_data = routs(url=URL, delay=speed)
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
    time.sleep(0.3) # Задержка для более ровного вывода
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
                        print('Страница не догружена, всего:', no_load_page)
                        s_bar.update()
                    else:
                        stops_data.append(data)
                        s_bar.update()
            s_bar.close()
        except:
            print('Ошибка, данные об остановках не получены')
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
                            query_for_write = "INSERT INTO main_data (rout, direction, stop, time) VALUES (?, ?, ?, ?)"
                            cursor.execute(query_for_write, (rout, direction, stop, link))
            base.commit()
        except:
            print('Ошибка сохранения данных об остановках')
        else:
            print('OK')

    time.sleep(0.3)

    if deep_step_work < 3:
        flag_launch = False
        arrive_time_mass = []  # Массив для полученных данных

    print('Данные по остановкам получены, строк:', len(stops_data))
    print('Продолжить (yes/no)')
    user_answer = input()
    if user_answer == 'yes' or 'y':
        pass
    else:
        flag_launch = False

    # Получение времени отправления по остановкам

    if flag_launch:
        temp_mass = []        # Временный массив для данных для ссылок
        arrive_time_mass = []  # Массив для полученных данных

        for element_rout in stops_data:
            for name_rout, rout in element_rout.items():
                for direction, stops in rout.items():
                    for name_station, link_first in stops.items():
                        temp_mass.append(link_first)

        #temp_mass_1 = []
        #for i in range(2812):
            #temp_mass_1.append(temp_mass[i])


        arrive_time_statusbar = tqdm(total=len(temp_mass), colour='yellow',
                                     desc='Расписания по остановкам')  # создание статус бара
        # Многопоточная обработка ссылок, на выходе [{ссылка : время},{ссылка : время},...]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            arrive_time = {executor.submit(get_time_list, URL=link, wait_time=speed, iteration=10):
                               link for link in temp_mass}
            for future in concurrent.futures.as_completed(arrive_time):
                try:
                    data = future.result()
                except:
                    arrive_time_mass.append({'' : ''})
                    arrive_time_statusbar.update()
                else:
                    arrive_time_mass.append(data)
                    arrive_time_statusbar.update()

        arrive_time_statusbar.close()
    else:
        flag_launch = False
        print('Ошибка, данные о времени отправления не получены')

    """
    if flag_launch:
        # Сохранение в файл ? может это лишнее
        if reserve_file_copy:
            with open('temp_out.txt', 'w', encoding='utf-8') as temp_file:
                json.dump(arrive_time_mass, temp_file)
    """
    # Запись результатов в базу и подсчёт недогруженных страниц
    no_load_page_count = 0  # Недогруженные страницы
    if flag_launch:
        # Запись в базу
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

    if no_load_page_count > 0:
        print('Есть недогруженные страницы, количество:', no_load_page_count)
        # кусок кода для догрузки недостающих данных
    else:
        print('Все страницы загружены')


    # Проверка на "битые данные" по времени отправления
    if correct_data_test:
        query_to_data_from_base = "SELECT * FROM main_data"
        cursor.execute(query_to_data_from_base)
        data_mass = cursor.fetchall()
        incorrect_data_num = 0  # Счётчик битых строк
        statusbar = tqdm(total=len(data_mass), colour='yellow')
        problem_mass = []  # Массив для битых строк
        for line in data_mass:
            time_dikt = literal_eval(line[3])  # Магия преобразования строки в словарь
            rezult, out_error = correct_time_data(time_dikt)
            statusbar.update()
            if rezult:
                continue
            else:
                problem_mass.append((line[0], line[1], line[2], out_error))
                incorrect_data_num += 1
        statusbar.close()
        time.sleep(0.5)

        # Вывод информации по битым строкам
        # Добавить исправление не корректных строк
        if problem_mass:
            print('Битых строк', incorrect_data_num, 'Из', len(data_mass))
            print('Показать проблемные строки? (да, yes, y / нет, no, n)')  # Запрос к юзеру
            answer_from_user = str(input())
            # Показать проблемные строки
            if answer_from_user:
                print('Проблемные строки')
                for element in problem_mass:
                    print(element)
        else:
            print('Проблемных строк не обнаружено')

    else:
        print('Проверка корректности данных отменена')

    stop_func(base_object=base, cursor_object=cursor)
    print('Завершение работы')


if __name__ == '__main__':
    # Ссылки на транспорт
    URL_BUS = ''  # Автобусы
    URL_TROLLEYBUS = 'https://minsktrans.by/lookout_yard/Home/Index/minsk#/routes/trolleybus'  # Троллейбусы
    URL_TRAM = 'https://minsktrans.by/lookout_yard/Home/Index/minsk#/routes/tram'  # Трамваи

    BASE_TRAM = 'tram_data.db'  # База с данными о трамваях
    BASE_BUS = 'bus_data.db'    # База с данными о автобусах
    BASE_TROLLEYBUS = 'trolleybus_data.db'  # База с данными о троллейбусах

    # Запуск основной функции
    # main_get_data(URL_TRAM, BASE_TRAM, correct_data_test=False)
    main_get_data(URL_TROLLEYBUS, BASE_TROLLEYBUS, correct_data_test=False, max_workers=30, deep_step_work=3)
