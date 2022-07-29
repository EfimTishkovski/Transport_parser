import concurrent.futures
import json
import time
import sqlite3

import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By

import ast
import tqdm
from concurrent.futures import ThreadPoolExecutor

import pars_file


def func():
    file = open('temp_station.txt', 'r')
    data = json.load(file)
    print(data)
    file.close()
    return data


def get_time_list_1(web_browser, URL, wait_time=2):
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


def hours_digit_test(mass):
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
        else: return False, 'Часы расположены не по порядку'
    else:
        out_flag = True
    return out_flag, ''

def correct_time_data(data_dikt):
    week_days_mass = []  # массив с днями недели
    tims_mass = []       # Массив с расписанием по дням недели

    days = ['Понедельник', 'Вторник', 'Среда', 'Четверг',
                'Пятница', 'Суббота', 'Воскресенье']


    # проверка 1 кол-во дней недели совпадет с кол-вом времени отправления по дням
    for week_days, tims in data_dikt.items():
        week_days_mass.append(week_days)
        tims_mass.append(tims)

    # Проверка на наличие всех дней недели
    diff = list(set(days).difference(set(week_days_mass)))
    if diff:
        return False, f'Нехватает дней {diff}'


    if len(tims_mass) != len(week_days_mass):
        return False, 'Пропущен день недели'


    # Проверка 2 каждому часу соответствует массив с минутами
    hours_mass = []  # Массив для часов
    minute_mass = [] # Массив для минут
    for line in tims_mass:
        hours_mass.clear()
        minute_mass.clear()

        if line == 'В этот день не ходит':
            continue

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

def get_time_list_inner_driver(URL, wait_time=3, iteration=8):
    """
    Функция получения времени отправления по остановке
    :param web_browser: Объект браузера
    :param URL: ссылка на страницу
    :return: словарь с днями недели и временем отправления
    """

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(argument='--headless')
    driver = webdriver.Chrome(options=chrome_options)

    driver.get(URL)  # Подгружаем страницу
    time.sleep(wait_time)
    week_days = {1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 4: 'Четверг',
                     5: 'Пятница', 6: 'Суббота', 7: 'Воскресенье'}
    temp_time_1 = {}
    out_data_mass = {}
    for day in range(1, 8):
        for i in range(iteration):
            try:
                temp_time_1.clear()
                button_monday = driver.find_element(By.XPATH,
                                                             f'/html/body/div[2]/div/div[2]/div[4]/div/div[3]/div[2]/div[1]/button[{day}]')
                button_monday.click()  # Щёлкает по кнопкам дней недели
                time.sleep(0.3)
                data_from_timelist = driver.find_element(By.ID, 'schedule')
                data_mass = data_from_timelist.text.split('\n')
                data_mass.pop(0)  # Убираем первый элемент "часы минуты" это лишнее
                # Сортировка данных

                # Обработка стандартного массива
                for i in range(0, len(data_mass) - 1, 2):
                    temp_time_1[data_mass[i]] = tuple(data_mass[i + 1].split(' '))
                    out_data_mass[week_days[day]] = temp_time_1.copy()

                break
            except Exception as error:
                print(error)
                continue
        else:
            driver.quit()
            return ''
    driver.quit()
    return out_data_mass

def half_week_rout(URL, wait_time=3, iteration=8):

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(argument='--headless')
    driver = webdriver.Chrome(options=chrome_options)

    week_days = {1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 4: 'Четверг',
                 5: 'Пятница', 6: 'Суббота', 7: 'Воскресенье'}

    temp_time = {}
    out_data_mass = {}

    for i in range(iteration):
        try:
            driver.get(URL)  # Подгружаем страницу
            time.sleep(wait_time)
            temp_time.clear()
            out_data_mass.clear()
            for day in range(1,8):
                # Проверка кнопки на активность
                button_info = driver.find_element(By.XPATH,
                f'/html/body/div[2]/div/div[2]/div[4]/div/div[3]/div[2]/div[1]/button[{day}]').get_attribute('class')
                if 'disabled' in button_info:
                    out_data_mass[week_days[day]] = 'В этот день не ходит'
                else:
                    button = driver.find_element(By.XPATH,
                                f'/html/body/div[2]/div/div[2]/div[4]/div/div[3]/div[2]/div[1]/button[{day}]')
                    button.click()     # Щёлкает по кнопкам дней недели
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
                return out_data_mass  # Успешная отработка цикла

        except Exception as error_mess:
            #print(error_mess)
            continue
    else:
        driver.quit()  # Закрытие драйвера если цикл отработал безуспешно
        return ''


if __name__ == '__main__':

    connection = sqlite3.connect('trolleybus_data.db')
    cursor = connection.cursor()

    rout = "4 ДС ''Одоевского'' - Бобруйская"
    direction = 'Прямое направление'
    stop = "ДС ''Одоевского''"
    values = rout, direction, stop
    search_query = f'SELECT link FROM main_data ' \
                   f'WHERE rout = ? and direction = ? and stop = ?;'

    cursor.execute(search_query, values)
    out = cursor.fetchall()
    print(out)

