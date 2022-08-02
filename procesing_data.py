# Обработка данных, исправление некорректных данных
# Некорректные данные встречаются в строках расписания по остановкам
# Не хватает часов, недопустимые символы и прочее, всё это исправляется здесь и записывается обратно в базу
import concurrent.futures
import sqlite3
import time
from ast import literal_eval
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

from add_load_data import half_week_rout


def hours_digit_test(mass):
    """
    Функция проверки корректности значений часов в массиве
    :param mass: Массив с данными для анализа
    :return: True or False и сообщение об ошибке
    """

    for digit in mass:
        # проверка на длину строки
        if len(digit) > 2:
            return False, f'Слишком много символов: {digit}'

        # Проверка на "число"
        if digit.isdigit():
            pass
        else:
            return False, f'Недопустимый символ: {digit}'

        # Проверка на нахождение числа в рамках 0 - 23
        if digit != ' ' and len(digit) <= 2 and 0 <= int(digit) <= 23:
            continue
        else:
            return False, 'Часы не в рамках 0 < 23'

    mass_int = list(map(int, mass))
    # Проверка на не убывание часов, что идут по порядку ?
    for i in range(len(mass_int) - 1):
        if mass_int[i] == 23:
            # Проверка на убывание после 23
            if mass_int[i] > mass_int[i + 1]:
                continue
            else:
                return False, 'Часы расположены не по порядку'

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
    minute_mass = []  # Массив для минут
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


def search_problem_data_func(data_massive):
    """
    Функция поиска некорректных данных
    :param data_massive: массив данных
    :return: true or false и массив с проблемными строками
    """
    problem_line_mass = []
    incorrect_data_num = 0
    try:
        # Проверка и поиск некорректных данных
        for line in data_massive:
            if line[3][0:5] != 'https':
                time_dikt = literal_eval(line[3])  # Магия преобразования строки в словарь
                rezult, out_error = correct_time_data(time_dikt)
                if rezult:
                    continue
                else:
                    problem_line_mass.append((line[0], line[1], line[2], line[4], out_error))
        if incorrect_data_num > 0:
            return True, problem_line_mass
        else:
            return True, problem_line_mass
    except Exception as error:
        print(error)
        return False, problem_line_mass


# Функция исправления данных
def fix_func(data, name_base):
    """
    Функция исправления некорректных данных
    :param data: Массив с входными данными
    :param name_base: База данных
    :return: Массив с обработанными данными
    """

    out = []
    array_of_grop_1 = []
    array_of_grop_2 = []
    array_of_grop_3 = []

    try:
        connection = sqlite3.connect(name_base)
        cursor = connection.cursor()
    except sqlite3.Error as base_connection_error:
        print(base_connection_error)
        return False, out

    s_bar = tqdm(total=len(data), desc='Сортировка', colour='GREEN')
    try:
        for line in data:
            # Сортировка
            error = line[3].split(':')[0]  # Текст ошибки, можно и без этой переменной, но так понятнее
            # Group 1
            if error in 'Недопустимый символ Не хватает дней Пропущен день недели':
                # Получение данных ошибочной строки и поиск ссылки
                rout = line[0]
                direction = line[1]
                stop = line[2]
                values = (rout, direction, stop)
                search_query = f'SELECT link FROM main_data ' \
                               f'WHERE rout = ? and direction = ? and stop = ?;'
                cursor.execute(search_query, values)
                link = cursor.fetchall()[0][0]
                array_of_grop_1.append(link)
                # Парсинг новых данных
                # data = half_week_rout(url=link, iteration=5)
                # out.append((rout, direction, stop, data))
                s_bar.update()

            # Group 2
            elif error in 'Слишком много символов':
                s_bar.update()

            # Group 3
            elif error in 'Часы не в рамках 0 < 23':
                rout = line[0]
                direction = line[1]
                stop = line[2]
                values = (rout, direction, stop)
                search_query = f'SELECT link FROM main_data ' \
                               f'WHERE rout = ? and direction = ? and stop = ?;'
                cursor.execute(search_query, values)
                link = cursor.fetchall()[0][0]
                array_of_grop_3.append(link)
                s_bar.update()
            else:
                s_bar.update()
        print('Сортировка завершена')
        s_bar.close()
        time.sleep(0.2)

        # Исправление
        array_of_grop_1.extend(array_of_grop_3)
        s_bar_get_new_data = tqdm(total=len(array_of_grop_1), desc='Повторное получение данных', colour='GREEN')
        with ThreadPoolExecutor(max_workers=20) as execuor:
            stops_info = {execuor.submit(half_week_rout, url=url, wait_time=3, iteration=5):
                              url for url in array_of_grop_1}
            for future in concurrent.futures.as_completed(stops_info):
                try:
                    data = future.result()
                except:
                    out.append({'': ''})
                    s_bar_get_new_data.update()
                else:
                    out.append(data)
                    s_bar_get_new_data.update()
            s_bar_get_new_data.close()

            # дописать внесение изменений в базу

            print('Исправление завершено')

    except Exception as processing_error:
        print(processing_error)
        s_bar.close()
        cursor.close()
        connection.close()
        return False, out

    else:
        cursor.close()
        connection.close()
        return True, out


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

    # Ошибки "Много символов" исправлять обработкой строки, добавить недостающий час или скопировать с другого дня
    # недели, где этой ошибки нет
    # Над исправлением ошибки "Часы не в рамках, подумать"


def re_pars(data_array, threead=10):
    """
    Функция первичного репарсинга
    :param data_array: Массив со ссылками для репарсинга
    :return: true or false и массив данных
    """

    out = []  # Массив выходных данных

    # Репарсинг
    s_bar_get_new_data = tqdm(total=len(data_array), desc='Повторное получение данных', colour='GREEN')
    with ThreadPoolExecutor(max_workers=threead) as execuor:
        stops_info = {execuor.submit(half_week_rout, url=url[3], wait_time=3, iteration=5):
                          url for url in data_array}
        for future in concurrent.futures.as_completed(stops_info):
            try:
                data = future.result()
            except:
                out.append({'': ''})
                s_bar_get_new_data.update()
            else:
                out.append(data)
                s_bar_get_new_data.update()
        s_bar_get_new_data.close()

    return True, out


# Главная функция объединяет работу всех остальных
def main_processing():
    pass


if __name__ == '__main__':

    # Запрос в базу
    name_base = 'trolleybus_data.db'
    connection = sqlite3.connect(name_base)
    cursor = connection.cursor()
    get_data_query = 'SELECT * FROM main_data'
    cursor.execute(get_data_query)
    data_mass = cursor.fetchall()

    # Поиск
    answer_search, mass = search_problem_data_func(data_mass)
    # массив кортежей ("Название маршрута", "направление", "остановка", "ошибка")
    print(f'Найдено {len(mass)} некорректных строк')

    # Первичный репарсинг
    repars_answer = False
    repars_mass = []
    if answer_search:
        """
        temp = []
        for i in range(100):
            a = 100 + i
            temp.append(mass[a])
        """
        repars_answer, repars_mass = re_pars(mass, threead=15)

    print('Репарсинг завершён, проверка новых данных')
    time.sleep(0.5)

    # Проверка данных после репарсинга
    true_data = []
    answer = False
    if repars_answer:
        for line in repars_mass:
            try:
                times = list(line.values())[0]
                answer, error_mess = correct_time_data(times)
            except:
                print(line, 'Ошибка')
                continue
            if answer:
                true_data.append(line)
        print('Исправленных строк:', len(true_data))

    # Формат промежуточного массива: [{ссылка : время, ссылка : время}]
    # Запись оного массива true_data в базу, сравнение кол-ва косячных строк
    for line in repars_mass:
        write_query = 'UPDATE main_data SET time = ? WHERE link = ?;'
        link = list(line.keys())[0]
        arr_times = list(line.values())[0]
        cursor.execute(write_query, (str(arr_times), str(link)))
    connection.commit()
    print('Изменения записаны')

    # Очистка массивов и освобождение памяти
    data_mass.clear()
    repars_mass.clear()
    true_data.clear()

    # Завершение первого этапа

    # Проверка на наличие битых строк
    #get_data_query = 'SELECT * FROM main_data'
    cursor.execute(get_data_query)
    data_mass = cursor.fetchall()

    time.sleep(0.5)
    print('Повторный поиск')

    # Повторный поиск
    answer_search, mass = search_problem_data_func(data_mass)
    # массив кортежей ("Название маршрута", "направление", "остановка", "ошибка")
    print(f'Найдено {len(mass)} некорректных строк')

    for line in mass:
        print(line)

    cursor.close()
    connection.close()
