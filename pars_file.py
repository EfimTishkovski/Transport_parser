import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import json


# Функция запуска парсинга
def launch(attempts=3):
    """
    Функция запуска, проверяет соединение с сайтом и базой.
    :param attempts : количество попыток проверки
    :return: driver, base, cursor : объект соединения с сайтом, объект базы, объект курсора
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
            base_object = sqlite3.connect('tram_data.db')  # Создание объекта базы
            cursor_object = base_object.cursor()  # Создание объекта курсора
        except:
            print('Ошибка инициализации: нет соединения с базой')
            return False
        # Если все соединения установлены, то возвращаем объекты
        return driver, base_object, cursor_object
    except:
        print('Ошибка инициализации')
        return False


# Получение маршрутов автобусов и ссылок на их расписания
def routs(web_browser, url, property=2):
    """
    Функция получения маршрутов
    :param web_browser: Объект вэбдрайвера
    :param url: ссылка на страницу с маршрутами
    :param property: Задержка после загрузки страницы
    :return: track_data словарь с данными, название и номер маршрута : ссылка
    """
    web_browser.get(url)
    time.sleep(property)
    data = web_browser.find_element(By.ID, 'routeList')
    item_data = data.find_elements(By.TAG_NAME, 'a')
    track_data = {}
    for element in item_data:
        track = element.find_element(By.TAG_NAME, 'h3')
        link = element.get_attribute('href')
        track_data[track.text] = link
    return track_data

# Функция получения остановок и ссылок на расписания по остановкам
def stops_transport_info(web_browser, data, property=2):
    """
    Функция получения названий остоновок
    :param web_browser: Объект вэбдрайвера
    :param data: Входные данные с маршрутами
    :param property: Задержка после загрузки страницы
    :return: out словарь с выходными данными
    """
    direct = {}   # Прямое направление маршрута
    reverse = {}  # Обратное
    out = []      # Список для выходных данных
    for key, val in data.items():
        url = val
        web_browser.get(url)
        time.sleep(property)
        data_from_list_stationA = web_browser.find_element(By.ID, 'tripA')
        name_stations_tripA = data_from_list_stationA.find_elements(By.TAG_NAME, 'a')
        data_from_list_stationB = web_browser.find_element(By.ID, 'tripB')
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
        station_data = {key : {'Прямое направление' : direct.copy(), 'Обратное направление' : reverse.copy()}}
        out.append(station_data)
    return out

# Функция записи данных в базу
def write_data(web_browser, base_object, cursor_object, data):
    """
    Функуия для записи данных в БД
    Таблица: Маршрут, Остановка, Направление, Расписание
    :param web_browser: Объект вэбдрайвера
    :param base_object: Объект БД
    :param cursor_object: Объект курсора в БД
    :param data: массив с данными для записи
    :return:
    """
    # Проверка базы на пустоту
    be_data_query = f'SELECT EXISTS (SELECT "Маршрут" FROM tram)'
    if cursor_object.execute(be_data_query):
        # Если база заполнена то обновление
        pass
    else:
        # Если база пуста, то первое заполнение
        pass

def get_time_list(web_browser, URL, wait_time=2):
    """
    Функция получения времени отправления по остановке
    :param web_browser: Объект браузера
    :param URL: ссылка на страницу
    :return: словарь с днями недели и временем отправления
    """
    # Дописать цикл извлечения ссылки из входного словаря
    web_browser.get(URL)  # Подгружаем страницу
    time.sleep(wait_time)
    week_days = {1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 4: 'Четверг',
                 5: 'Пятница', 6: 'Суббота', 7: 'Воскресенье'}
    temp_time_1 = {}
    out_data_mass = {}
    for day in range(1, 8):
        temp_time_1.clear()
        button_monday = web_browser.find_element(By.XPATH,
                                            f'/html/body/div[2]/div/div[2]/div[4]/div/div[3]/div[2]/div[1]/button[{day}]')
        button_monday.click()  # Щёлкает по кнопкам дней недели
        time.sleep(0.3)
        data_from_timelist = web_browser.find_element(By.ID, 'schedule')
        data_mass = data_from_timelist.text.split('\n')
        data_mass.pop(0)  # Убираем первый элемент "часы минуты" это лишнее
        # Сортировка данных
        # Проверка на "не стандартные данные"
        not_standart_data_flag = False
        for i in range(0, len(data_mass) - 1):
            if len(data_mass[i]) < len(data_mass[i + 1]):
                not_standart_data_flag = True
                break
        if not_standart_data_flag:
            # Обработка стандартного массива
            for i in range(0, len(data_mass) - 1, 2):
                temp_time_1[data_mass[i]] = tuple(data_mass[i + 1].split(' '))
                out_data_mass[week_days[day]] = temp_time_1.copy()
        else:
            # Обработка сложного, не стандартного массива
            out_data_mass[week_days[day]] = complex_mass(data_mass)

        # Возможно лишний кусок, дублируется выше
        for i in range(0, len(data_mass) - 1, 2):
            temp_time_1[data_mass[i]] = tuple(data_mass[i + 1].split(' '))
        out_data_mass[week_days[day]] = temp_time_1.copy()
    #print(out_data_mass)
    return out_data_mass

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

def stop_func(web_browser, base_object, cursor_object):
    """
    Функция завершения работы
    закрывает соединение с интернетом и БД
    :param web_browser: Объект вебдрайвера ()
    :param base_object: Объект БД
    :param cursor_object: Объект курсора в БД
    """
    web_browser.quit()
    cursor_object.close()
    base_object.close()
    print('Соединения закрыты')

def hours_digit_test(mass):
    """
    Функция проверки корректности значений часов в массиве
    :param mass: Массив с данными для анализа
    :return: True or False и сообщение об ошибке
    """
    for digit in mass:
        if digit != ' ' and 0 <= int(digit) <= 23:
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
        else: return False, 'Часы расположены не по порядку'
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
    tims_mass = []       # Массив с расписанием по дням недели
    # проверка 1 кол-во дней недели совпадет с кол-вом времени отправления по дням
    for week_days, tims in data_dikt.items():
        week_days_mass.append(week_days)
        tims_mass.append(tims)
    if len(tims_mass) != len(week_days_mass):
        return False, 'Пропущен день недели'

    # Проверка 2 каждому часу соответствует массив с минутами
    hours_mass = []  # Массив для часов
    minute_mass = [] # Массив для минут
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


if __name__ == '__main__':
    # Настройки
    speed = 2  # Задержка для загрузки страницы

    # Ссылки на транспорт
    URL_BUS = ''  # Автобусы
    URL_TROLLEYBUS = ''  # Троллейбусы
    URL_TRAM = 'https://minsktrans.by/lookout_yard/Home/Index/minsk#/routes/tram'  # Трамваи

    flag_launch = False  # Флаг запуска
    objects = launch()   # Запуск инициализации
    web_driwer, base, cursor = objects  # Получение объектов вэб драйвер, соединение с БД и курсор

    # Проверка инициализации
    if objects:
        print('Инициализация завершена успешно')
        flag_launch = True
    else:
        print('Инициализация не выполнена')

    # Запуск основной программы
    # Получение данных о маршрутах (номер - ссылка)
    iteration = 5
    if flag_launch:
        for i in range(iteration):
            try:
                tram_routs = routs(web_browser=web_driwer, url=URL_TRAM, property=speed)
                # Запись данных во временный файл, а оно надо?
                with open('temp_roads.txt', 'w') as routs_data_file:
                    json.dump(tram_routs, routs_data_file)
                #print('Маршруты получены и записаны в temp_roads.txt')
                # Запись данных в БД
                clear_routs_link_table_qwery = "DELETE FROM routs_link" # Очистка таблицы от предыдущих записей
                cursor.execute(clear_routs_link_table_qwery)
                base.commit()
                for rout, link in tram_routs.items():
                    qwery_for_write_routs_link = "INSERT INTO routs_link (rout, link) VALUES (?, ?)"
                    cursor.execute(qwery_for_write_routs_link, (rout, link))
                    base.commit()
                print('Маршруты получены и добавлены в базу')
                break
            except:
                print(f'Страница не догружена, попытка {i} из {iteration}')
        else:
            flag_launch = False
            print('Ошибка получения данных о маршрутах')

    # Получение данных об остановках
    # [{маршрут : {'Прямое направление : {'остановка : ссылка, ...'}, 'Обратное направление' : {'остановка : ссылка, ...'}}}, {маршрут1 : ...}]
    if flag_launch:
        for i in range(iteration):
            try:
                stops_data = stops_transport_info(web_browser=web_driwer, data=tram_routs, property=speed)
                # Сохранение в файл
                with open('temp_station_tram_data.txt', 'w') as tram_station_data_file:
                    json.dump(stops_data, tram_station_data_file)
                #print('Данные по остановкам получены и записаны в temp_station_tram_data.txt')
                # Запись данных в БД
                clear_routs_link_table_query = "DELETE FROM tram_main_data"  # Очистка таблицы от предыдущих записей
                cursor.execute(clear_routs_link_table_query)
                base.commit()
                for element in stops_data:
                    for rout, data in element.items():
                        for direction, stop_link in data.items():
                            for stop, link in stop_link.items():
                                query_for_write = "INSERT INTO tram_main_data (rout, direction, stop, time) VALUES (?, ?, ?, ?)"
                                cursor.execute(query_for_write, (rout, direction, stop, link))
                                base.commit()
                print('Данные по остановкам получены и добавлены в базу')
                break
            except:
                print(f'Страница не догружена, попытка {i} из {iteration}')
        else:
            flag_launch = False
            print('Ошибка получения данных по остановкам')

    # Получение времени отправления по остановкам
    if flag_launch:
        temp_mass = []  # Временный массив для данных для ссылок
        for element_rout in stops_data:
            for name_rout, rout in element_rout.items():
                for direction, stops in rout.items():
                    for name_station, link in stops.items():
                        # Дописать упрощённую вервию, убрать лишнее
                        #arrive_time = get_time_list(web_browser=web_driwer, URL=link, wait_time=speed)
                        temp_mass.append(link)

        # Получение времени на выходе [{ссылка : время},{ссылка : время},...]
        arrive_time_mass = []
        size = len(temp_mass)
        temp_mass = enumerate(temp_mass, start=1)
        for link in temp_mass:
            for i in range(10):
                try:
                    arrive_time = get_time_list(web_browser=web_driwer, URL=link[1], wait_time=speed)
                    arrive_time_mass.append({link[1]: arrive_time})
                    print(f'{link[0]} / {size}')
                    break
                except:
                    print('Страница не догружена', i)
            else:
                print('Страница так и не догрузилась')
                arrive_time_mass.append({link[1]: ''})

        # Сохранение в файл
        with open('temp_out.txt', 'w', encoding='utf-8') as temp_file:
            json.dump(arrive_time_mass, temp_file)
        #print('Данные по времени отправления получены и сохранены в temp_out.txt')

        # Запись в базу
        for line in arrive_time_mass:
            link = list(line.items())[0][0]
            arr_time = list(line.items())[0][1]
            query = "UPDATE tram_main_data SET time = ? WHERE time = ?"
            parametrs = (str(arr_time), str(link))
            cursor.execute(query, parametrs)
        base.commit()
        print('Данные по времени отправления получены и добавлены в базу')
    else:
        flag_launch = False
        print('Ошибка получения времени отправления')
    # Проверка на "битые данные" по времени отправления
    query_to_data_from_base = "SELECT * FROM tram_main_data"
    data_from_base = ''

    stop_func(web_browser=web_driwer, base_object=base, cursor_object=cursor)
    print('Завершение работы')
