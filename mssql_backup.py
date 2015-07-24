#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'wf'
import sys
import os

import pymssql


def __help():
    print """
        mssql_backup.py OPERATION DATABASE
        Скрипт резервного копирования базы данных с сервера MSSQL
        Для запуска необходимо два ключа.
        DATABASE - имя базы данных на сервере MSSQL
        OPERATION - вид операции: BACKUP, DELETE
    """


def config(section):
    """
    :param section:
    :return:
    во внешний файл не стал выносить, так как не много настроек
    [main]
    temp_path - временная папка, в которую сохраняем бэкап, из неё бакула заберёт архив
    awhere - та же папка, только относительно машины, с которой запускается скрипт
    [mssql]
    настройки подключения к серверу баз данных (хост, пользователь, пароль
    у пользователя должны быть права не ниже db_owner, что бы сделать сжатие логов
    """
    if section == 'main':
        return {'temp_path': '\\\\ip\\folder',
                'awhere': 'some_path'}
    elif section == 'mssql':
        return {'host': "",
                'user': '',
                'password': ""}


def dbconnect(adatabase):
    """
    Подключаеися к базе данных
    :param adatabase: имя базы
    :return: возвращаем подключение и строку подключения
    """
    args = config('mssql')
    args['database'] = adatabase
    try:
        conn = pymssql.connect(**args)
        cursor = conn.cursor()
        cursor.execute("commit tran")
        return conn, cursor
    except pymssql.Error, err:
        raise pymssql.Error, err


def backup(adatabase):
    """
    Резервное копирование базы
    :param adatabase: имя базы
    :return:
    """
    con, cur = dbconnect(adatabase)
    try:
        query = "BACKUP DATABASE %(db)s TO DISK = '%(path)s\\%(db)s.sql' WITH INIT, COMPRESSION" % {'db': adatabase, 'path': config('main')['temp_path']}
        cur.execute(query)
        # Создадим резервную копию логов и усечем их
    except pymssql.Error, err:
        raise pymssql.Error, err

    cur.execute("begin tran")
    con.close()


def backuplog(adatabase):
    """
    Резервное копирование логов базы и их усечение
    :param adatabase: имя базы
    :return:
    """
    con, cur = dbconnect(adatabase)
    # Получим режим восстановления
    cur.execute("""USE master; SELECT recovery_model_desc FROM sys.databases WHERE name = %s;""", adatabase)
    result = cur.fetchone()
    if result[0] != 'SIMPLE':
        # В режиме восстановления SIMPLE, резервное копирование логов недоступно,
        try:
            # Создадим резервную копию логов
            query = "BACKUP LOG %(db)s TO DISK = '%(path)s\\%(db)s_log.sql' WITH INIT, COMPRESSION" % {'db': adatabase, 'path': config('main')['temp_path']}
            cur.execute(query)
        except pymssql.Error, err:
            raise pymssql.Error, err

    # Получим имена файлов логов
    query = """USE %s; SELECT name FROM sys.database_files WHERE type = 1;""" % adatabase
    cur.execute(query)
    log_files = [log_file[0] for log_file in cur if len(log_file) > 0]
    # начнём усечение логов
    try:
        for log_file in log_files:
            query = "USE %(db)s; DBCC SHRINKFILE (%(log_file)s, 1024);" % {'db': adatabase, 'log_file': log_file}
            cur.execute(query)
    except pymssql.Error, err:
        raise pymssql.Error, err

    cur.execute("begin tran")
    con.close()


def __delete(adatabase):
    """
    Удаляем остатки от бэкапа
    :param adatabase:
    :return:
    """
    awhere = config('main')['awhere']
    os.remove("%(path)s/%(db)s.sql" % {'path': awhere, 'db': adatabase})
    os.remove("%(path)s/%(db)s_log.sql" % {'path': awhere, 'db': adatabase})


if __name__ == "__main__":
    # параметры подключения к базе mssql

    if sys.argv.__len__() != 3:
        __help()
        exit()

    # Получаем имя базы данных
    if sys.argv[1].lower() == "-h":
        __help()
        exit()

    # Получаем вид операции
    if sys.argv[1].lower() == 'backup':
        backuplog(sys.argv[2])
        backup(sys.argv[2])
    elif sys.argv[1].lower() == 'delete':
        __delete(sys.argv[1])
    else:
        __help()
