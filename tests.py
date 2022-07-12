import json
import time
import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By

def func():
    file = open('temp_station_tram_data.txt', 'r')
    data = json.load(file)
    print(data)
    file.close()
    return data


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
                 5: 'пятница', 6: 'Суббота', 7: 'Воскресенье'}
    temp_time_1 = {}
    out_data_mass = {}
    for day in range(1, 8):
        temp_time_1.clear()
        button_monday = web_browser.find_element(By.XPATH,
                                            f'/html/body/div[2]/div/div[2]/div[4]/div/div[3]/div[2]/div[1]/button[{day}]')
        button_monday.click()  # Щёлкает по кнопкам дней недели
        time.sleep(0.1)
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
            print('Задействован не стандартный алгоритм')
            out_data_mass[week_days[day]] = complex_mass(data_mass)

    print(out_data_mass)
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

def write_base(mass, base, cursor, table):
    """
    Функция записи в базу
    :param mass: Массив данных для записи
    :param base: Объект базы
    :param base: Объект курсора
    :param table: Задействованная таблица базы
    :return:
    """
    pass

def hours_digit_test(mass):
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
        #print(mass[i], '', mass[i + 1])
        if mass[i] < mass[i + 1]:
            continue
        else: return False, 'Часы расположены не по порядку'
    else:
        out_flag = True
    return out_flag, ''

def correct_time_data(data_dikt):
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

    connection = sqlite3.connect('tram_data.db')
    cursor = connection.cursor()
    with open('temp_out.txt', 'r') as file:
        mass = json.load(file)

    arr_time = list(mass[0].items())[0][1]
    print(arr_time)
    flag = correct_time_data(arr_time)[0]
    print(flag)
    connection.commit()
    cursor.close()
    connection.close()

    """
    flag = minute_digit_test(['05', '06', '07', '08', '09', '10', '11', '12', '13', '14',
                              '15', '16', '17', '18', '19', '20', '21', '22', '23', '00'])
    print(flag)
    """