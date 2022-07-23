# Обработка данных, исправление некорректных данных

import sqlite3
from ast import literal_eval

import pars_file


def hours_digit_test(mass):
    """
    Функция проверки корректности значений часов в массиве
    :param mass: Массив с данными для анализа
    :return: True or False и сообщение об ошибке
    """
    for digit in mass:
        # Проверка на "число"
        if digit.isdigit():
            pass
        else:
            return False, 'Нечисло'
        # Проверка на нахождение числа в рамках 0 - 23
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

def search_problem_data_func(name_base):
    """
    Функция поиска некорректных данных
    :param name_base:
    :return:
    """
    # Получение данных из базы
    connection = sqlite3.connect(name_base)
    cursor = connection.cursor()
    get_data_query = 'SELECT * FROM main_data'
    cursor.execute(get_data_query)
    data_mass = cursor.fetchall()

    # Проверка и поиск некорректных данных
    problem_line_mass = []
    incorrect_data_num = 0
    for line in data_mass:
        if line[3][0:5] != 'https':
            time_dikt = literal_eval(line[3])  # Магия преобразования строки в словарь
            rezult, out_error = correct_time_data(time_dikt)
            if rezult:
                continue
            else:
                problem_line_mass.append((line[0], line[1], line[2], out_error))
                print((line[0], line[1], line[2], out_error))
                incorrect_data_num += 1

    print(incorrect_data_num)
    return data_mass

# Функция исправления данных
def fix_func(data):
    pass


if __name__ == '__main__':
    #search_problem_data_func('trolleybus_data.db')
    data = pars_file.get_time_list('https://minsktrans.by/lookout_yard/Home/Index/minsk#/routes/trolleybus/22/stops/62413/1')
    print(data)
