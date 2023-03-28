'''
import csv
# пременная хронит адрес файла который нам необходимо открыть
name = 'posts.csv'
# чтение данных и csv файла
with open(name, "r", newline="") as file:
    # чтение из файла данных с помощью команды райдер
    data = csv.reader(file)
    # выведенм на экран данны, счттанные из файла
    for row in data:
        print(row[0])
'''
import numpy as np
import pandas as pd
import sqlite3

def text_search(text):
    con = sqlite3.connect('intern.db')
    cur = con.cursor()
    # limit - команда(коловое слово) указывет количество записей  в выборке. offseе - команда, указавающая сколько надо пропустить
    data_tuple = (text,)
    print(data_tuple)
    sql= 'create index if not exists id_filter4 on data_array1(id, texte) where texte = "интересно узнать ваше мнение" '
    cur.execute(sql)
    # подтверждаем транзакцию на измен ие базы
    con.commit()
    # закрываем объект  cursor
    cur.close()
    # закрываем соединение с базой данных
    con.close()
# функция удаления шндекса

def delete_index(name_index):
    con = sqlite3.connect('intern.db')
    cur = con.cursor()
    data_tuple = (name_index,)

    sql= "drop index "+ name_index
    cur.execute(sql)
    # подтверждаем транзакцию на измен ие базы
    con.commit()
    # закрываем объект  cursor
    cur.close()
    # закрываем соединение с базой данных
    con.close()

# функция добавлет в базу данных новую запись
def insert_data(text, rubrics, create_date):
    # подключаемся к базе данных
    con = sqlite3.connect('intern.db')
    cur = con.cursor()
    # создаем кортеж для безопасного добаления данных в базу
    data_corteg = (text, rubrics, create_date)
    # ввиде строковой переменной записываем SQL запрос на добаление данных в базу
    sql= 'insert into data_array1 (texte, rubrics, create_date) values (?, ?, ?)'
    # выполянем запроос на добаление данных
    cur.execute(sql, data_corteg)
    # подтверждаем транзакцию на измен ие базы
    con.commit()
    # закрывкм обект  cursor
    cur.close()
    # закрываем соединение с базой данных
    con.close()
#  функция удаляет из базы данных запись по указанному ID
def delete_data(id):
    # подключаемся к базе данных
    con = sqlite3.connect('intern.db')
    cur = con.cursor()
    # создаем кортеж для безопасного удаления данных из Базы
    data_corteg = (id,)
    # ввиде строковой переменной записываем SQL запрос на добаление данных в базу
    sql= 'delete from data_array1 where id = ?'
    # выполянем запроос на добаление данных
    cur.execute(sql, data_corteg)
    # подтверждаем транзакцию на измен ие базы
    con.commit()
    # закрывкм обект  cursor
    cur.close()
    # закрываем соединение с базой данных
    con.close()
    print("Указанные данные успешно удалены.")
# функция принимает на вход адрес файла с данными и помещает данные в базу
def data_in_DB(fail_name):
    print('считыввем данные из файла и записываем в DB...')
    # считываем данные из файла в датафрейм
    data = pd.read_csv(fail_name)
    # тестовый код для вывода нескольких переменных из датафрейма
    #  функция intertuples  созадает кортеж по каждой строуе с датафрейма(таблица)
    # создаем список кортежей для безопасного добаления данных в базу, для того чтобы одним запросом поместить все данные
    list_data = list(data.itertuples())
    # подключаемся к базе данных
    con = sqlite3.connect('intern.db')
    cur = con.cursor()
    # ввиде строковой переменной записываем SQL запрос на добаление данных в базу
    sql = 'insert into data_array1 (id, texte, create_date, rubrics)  values (?, ?, ?, ?)'
    # выполянем запроос на добаление данных
    cur.executemany(sql, list_data)
    # подтверждаем транзакцию на измен ие базы
    con.commit()
    # закрываем объект  cursor
    cur.close()
    # закрываем соединение с базой данных
    con.close()
   # print(list_data[0])
    """
    for row in data.itertuples():
        insert_data(row[1], row[2], row[3])
    """
    print('данные успешно дабавлоены')
def menu():
    while True:
        print("выберите действие :")
        print("0 - завершение работы ")
        print( "1 - в БД занесли данные из файла ")
        print("2 - удалить двнные по ID")
        print(('3 - поиск фрагмента текста из файла по введенному тексту' ))
        print('4 - удаление индексации')
        print()
        n =  int(input("Укажите ваш выбор: "))

        if n == 0:
            print("работа программы завершена. Досвидания!")
            break
        elif n == 1:
            fail_name = 'posts.csv'
            data_in_DB(fail_name)
        elif n == 2:
            id_1 = int(input('введите id  записи, которую нужно удалить:'))
            delete_data(id_1)
        elif n == 3:
            text = input("введите фрагмент тектста : ")
            text_search(text)
        elif n == 4:
            name_index = input("введите имя индека, которое хотите удалить. ")
            delete_index(name_index)
            print()

menu()





# считываем данные из файла в датафрейм

fail_name= 'posts.csv'



