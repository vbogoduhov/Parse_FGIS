#!/usr/bin/python3
"""
Модуль для работы с локальной БД PostgreSQL.
В модуле два класса:
    WorkDb - класс для объекта БД postgreSQL,
    CardFgis - структура данных для карточки ФГИС для экземпляра СИ.
"""

# Блок импорта
import os.path
import psycopg2 as psql
import app_logger

logger = app_logger.get_logger(__name__, 'localdb_log.log')
output_data_metrology = app_logger.get_logger('Data_from_metrology', 'localdb_log.log')


class WorkDb:
    MODE_CHECK = {'title': 'tbtitle',
                  'modification': 'tbmodification',
                  'type_title': 'tbtype',
                  'type_number': 'tbtype',
                  'name_org': 'tborgmetrology',
                  'si_number': 'tbmetrology',
                  'type': 'tbtype',
                  'href': 'tbmetrology'}

    def __init__(self, database="fgis",
                 user="postgres",
                 password="postgres",
                 port="5432",
                 host="10.48.153.106"):
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.host = host
        try:
            self.connect = self.connect_db()
            self.cursor = self.cursor_db()
            if self.tables == [] or self.tables is None:
                self.__create_table()
        except:
            self.connect = None
            self.cursor = None
        self.dict_check = {'title': 'id_title',
                           'modification': 'id_mod',
                           'type_title': 'id_type',
                           'type_number': 'id_type',
                           'name_org': 'id_org',
                           'si_number': 'si_number',
                           'type': 'id_type'}
        self.namefile_sql_scripts = 'query_metrology.sql'
        self.lst_sql_scripts_metrology = self.__get_sql_scripts()

    def __get_sql_scripts(self):
        """
        Открываем файл с запросами к таблице tbmetrology,
        читаем и возвращем словарь с запросами
        :return: словарь с запросами
        """
        if os.path.isfile(self.namefile_sql_scripts):
            with open(self.namefile_sql_scripts, "r") as sql_file:
                querys = sql_file.read()
                lst_query = querys.split(sep=";")
            return lst_query
        else:
            logger.warning(f"Невозможно открыть файл {self.namefile_sql_scripts}, поскольку файл не существует")

    def __create_table(self):
        """"
        Метод для создания описания таблиц БД
        """
        lst_names_files = list(os.walk('scripts/create'))[0][2]
        for f in lst_names_files:
            name = os.path.join('scripts', 'create', f)
            with open(name, 'r') as script_file:
                sql_query = script_file.read()
                print(sql_query)
            try:
                logger.info("Попытка создания описания таблиц в БД, так как существующая БД пуста.")
                self.cursor.execute(sql_query)
                self.connect.commit()
                logger.info("Описание таблиц БД успешно создано")
            except Exception:
                logger.warning(f"Не удалось создать описание таблиц в БД. Ошибка {Exception.__str__()}")
                self.connect.rollback()

    def __write_data_on_db(self, table: str, dict_for_write: dict):
        """
        Метод для записи данных в БД

        :param dict_for_write: словарь для записи, где ключи - наименования столбцов,
                                значения - значения столбцов
        :return:
        """
        logger.info("Начало __write_data_on_db...")
        lst_name_column = list(dict_for_write.keys())
        lst_values = [dict_for_write[key] for key in lst_name_column]
        format_sql_query = f"""insert into {table} ({', '.join(map(str, lst_name_column))}) values ({", ".join(map(lambda x: f"'{x}'", lst_values))});"""
        logger.info(f"Попытка записи names_columns: {lst_name_column}\n values: {lst_values}")
        logger.info(f"Попытка выполнения запроса на запись: {format_sql_query}")
        try:
            self.cursor.execute(format_sql_query)
            self.connect.commit()
            logger.info("Запись удалась")
            return True
        except:
            self.connect.rollback()
            logger.warning("Запись не удалась\n")
            logger.warning(f"Не удалось выполнить запрос <{format_sql_query}>")
            return False

    def __write_data(self, sql_query: str):
        """
        Метод для записи данных в БД, по сути выполняет sql-запрос, заранее сформированный

        :param sql_query: строка запроса для выполнения
        :return: True or False
        """
        try:
            logger.info(f"Попытка записи данных по запросу: {sql_query}")
            self.cursor.execute(sql_query)
            self.connect.commit()
            return True
        except Exception as err:
            self.connect.rollback()
            logger.warning(f"Не удалось записать данные по запросу: {sql_query}. Ошибка: {err.__str__()}")
            return False

    def __update_record_metrology(self, dict_for_write: dict):
        """
        Метод для обновления записи по СИ в таблице tbmetrology

        :param dict_for_write: словарь с данными для записи
        :return:
        """
        pass

    def write_metrology(self, dict_for_write: dict):
        """
        Метод для записи данных в таблицу tbmetrology

        :param dict_for_write: словарь с данными для формирования запроса на запись
        :return:
        """
        logger.info(f"Начало работы функции write_metrology для данных {dict_for_write}")
        sql_query = f"call add_record_metrology({dict_for_write['mitnumber']}," \
                    f"                          {dict_for_write['modification']}," \
                    f"                          '{dict_for_write['si_number']}'," \
                    f"                          '{dict_for_write['valid_date']}'," \
                    f"                          '{dict_for_write['docnum']}'," \
                    f"                          {dict_for_write['mitype']}," \
                    f"                          {dict_for_write['title']}," \
                    f"                          {dict_for_write['org_title']}," \
                    f"                          '{dict_for_write['applicability']}'," \
                    f"                          '{dict_for_write['vri_id']}'," \
                    f"                          '{dict_for_write['verif_date']}'," \
                    f"                          '{dict_for_write['href']}'," \
                    f"                          '{dict_for_write['change_date']}'::date," \
                    f"                          {dict_for_write['change_flag']}," \
                    f"                          {dict_for_write['rows_number']})"
        logger.info(f"Сформированный для записи данных запрос: {sql_query}")
        try:
            if self.__write_data(sql_query):
                logger.info(f"Данные в таблицу tbmetrology успешно записаны")
            else:
                logger.warning(f"Не удалось записать данные по запросу: {sql_query}")
        except:
            logger.warning(f"Запись данных завершилась аварийно по запросу: {sql_query}")
        # self.__write_data_on_db("tbmetrology", dict_for_write)
        # logger.info(f"Окончание работы функции write_metrology для данных {dict_for_write}")

    def __get_data_from_db(self, sql_query: str):
        """
        Метод для получения данных из БД

        :param sql_query: sql-скрипт
        :return: словарь, где ключи - наименования столбцов,
                          значения - соответствующие значения стобцов
        """
        try:
            logger.info(f"Попытка получения данных из БД по запросу {sql_query}")
            self.cursor.execute(sql_query)
            rows = self.cursor.fetchall()
            logger.info(f"Данные по запросу <{sql_query}> успешно получены")
            return rows
        except Exception as err:
            logger.warning(f"Не удалось получить данные по запросу <{sql_query}>, ошибка: {err.__str__()}")

    @property
    def tables(self):
        """
        Список таблиц БД

        :return: список тиблиц в локальной БД
        """
        if self.cursor != None:
            try:
                logger.info(f"Получаем список таблиц из БД")
                self.cursor.execute("""SELECT table_name FROM information_schema.tables
                                        WHERE table_schema NOT IN ('information_schema','pg_catalog');""")
                rOut = self.cursor.fetchall()
                lst_tables = [item[0] for item in rOut]
                return lst_tables
            except:
                logger.warning("Не удалось получить список таблиц из БД")
        else:
            logger.info(f"Объект cursor() = None")

    @property
    def types(self):
        """
        Список типов СИ

        :return: список типов из таблицы tbtype
        """
        try:
            logger.info(f"Получаем список типов СИ из БД")
            self.cursor.execute("""select id_type, type_title from tbtype""")
            rOut = self.cursor.fetchall()
            lst_types = [[item[0], item[1]] for item in rOut]
            return lst_types
        except:
            logger.warning("Не удалось получить список типов СИ из БД.")

    @property
    def modifications(self):
        """
        Список модификаций типов СИ

        :return: список модификаций из тиблицы tbmodification
        """
        try:
            self.cursor.execute("""select id_mod, modification from tbmodification""")
            rOut = self.cursor.fetchall()
            lst_mod = [[item[0], item[1]] for item in rOut]
            return lst_mod
        except:
            logger.warning("Не удалось получить список модификаций из БД.")

    def connect_db(self):
        """
        Метод для установления соединения с БД

        :return: объект connect
        """
        try:
            connect = psql.connect(database=self.database,
                                   user=self.user,
                                   password=self.password,
                                   host=self.host,
                                   port=self.port)
            return connect
        except ConnectionError:
            logger.warning(f"Не удаётся установить соединение с БД")

    def cursor_db(self):
        """
        Метод для получения объекта cursor

        :return: объект cursor
        """
        if self.connect != None:
            cursor = self.connect.cursor()
            return cursor
        else:
            logger.warning(f"Не удалось получить объект cursor(), так как соединение с БД не было установлено")

    def check_value(self, value: tuple, mode: str = ""):
        """
        Метод для проверки значения value

        :param value: значение value для проверки, кортеж
        :param mode: режим проверки, то есть что проверять,
                     title - тип СИ, в таблице tbtitle,
                     modification - модификация типа СИ, в таблице tbmodification,
                     type_title - наименование типа СИ, в таблице tbtype,
                     type_number - номер типа СИ в базе ФГИС, в таблице tbtype,
                     name_org - наименование организации-поверителя, в таблице tborgmetrology
                     href - гиперссылку в таблице tbmetrology
        :return: True - если есть, False - если нет
        """
        if not self.__check_mode(mode):
            return False
        else:
            # Проверка значения, исходя из значения mode
            match mode:
                # Если mode == title
                case 'title':
                    id_title = self.get_id_title(value[0])
                    if id_title is None:
                        return False
                    else:
                        return True
                # Если mode == modification
                case 'modification':
                    id_mod = self.get_id_mod(value[0])
                    if id_mod is None:
                        return False
                    else:
                        return True
                # Если mode == type
                case 'type':
                    id_type = self.get_id_type(value[0], value[1])
                    if id_type is None:
                        return False
                    else:
                        return True
                # Если mode == name_org
                case 'name_org':
                    id_org = self.get_id_org(value[0])
                    if id_org is None:
                        return False
                    else:
                        return True
                # Если mode == href
                case 'href':
                    id_href = self.get_id_for_href(value[0])
                    if id_href is None:
                        return False
                    else:
                        return True

    def __format_check_query(self, value, table, column, filter="", mode="read"):
        """
        Метод для формирования строки запроса в БД, для проверки нахождения в БД
        значения value

        :param value: значение для проверки нахождения в БД
        :param table: имя таблицы БД для проверки
        :param column: наименоние поля для получения данных по нему
        :param filter: поле, в котором искать значение
        :return: строка запроса
        """
        if mode == "read":
            if type(filter) is list:
                query = f"""select {column} from {table} where {filter[0]} = '{value[0]}' and {filter[1]} = '{value[1]}';"""
            else:
                query = f"""select {column} from {table} where {filter} = '{value}';"""
            return query
        elif mode == "write":
            if table == "tbtype":
                query = f"insert into {table} ({column}) values ('{value[0]}', '{value[1]}');"
                return query
            else:
                query = f"insert into {table} ({column}) values ('{value}');"
                return query

    def get_id(self, value, mode=""):
        """
        Метод для получения id с БД по существующему значению value

        :param value: значение по поиску
        :param mode: столбец для фильтрации
        :return: id
        """
        if not self.__check_mode(mode):
            return None
        else:
            table = WorkDb.MODE_CHECK[mode]
            column = self.dict_check[mode]
            if mode == 'type':
                select_filter = ['type_title', 'type_number']
            else:
                select_filter = mode
            sql_query = self.__format_check_query(value, table, column, select_filter, mode="read")
            sql_executable_data = self.__get_data_from_db(sql_query)
            try:
                id = sql_executable_data[0][0]
                return id
            except IndexError:
                logger.warning(f"Получено пустое значение из БД по запросу <{sql_query}>")

    def set_id(self, value, mode=""):
        """
        Метод для записи значения value в БД

        :param value: значение для записи, кортеж
        :param mode: наименование столбца для записи
        :return: идентификатор для записи
        """
        if self.__check_mode(mode):
            table = WorkDb.MODE_CHECK[mode]
            match mode:
                case 'title':
                    if self.__add_title_record(value[0]):
                        id_record = self.get_id_title(value[0])
                        return id_record
                    else:
                        return None
                case 'modification':
                    if self.__add_modification_record(value[0]):
                        id_record = self.get_id_mod(value[0])
                        return id_record
                    else:
                        return None
                case 'type':
                    if self.__add_type_record(value[0], value[1]):
                        id_record = self.get_id_type(value[0], value[1])
                        return id_record
                    else:
                        return None
                case 'name_org':
                    if self.__add_orgmetrology_record(value[0]):
                        id_record = self.get_id_org(value[0])
                        return id_record
                    else:
                        return None

    def __check_mode(self, mode):
        """
        Метод для проверки mode

        :param mode: значение mode
        :return: True or False
        """
        if mode == "":
            logger.warning(f"Задано пустое значение mode. Проверка невозможна.")
            return False
        else:
            return True if mode in WorkDb.MODE_CHECK else False

    def __parse_dict_filter_for_card(self, dict_filter):
        """
        Парсим словарь и возвращаем данные

        :param dict_filter: словарь
        :return: данные по отдельности
        """
        return dict_filter['serial_si'], dict_filter['name_si'], dict_filter['verif_year'], dict_filter['type_si']

    def __format_get_metrology_query(self, serial_si, name_si, verif_year, type_si, mode="only_si"):
        """
        Форматируем и формируем строку запроса в БД,
        для получения данных по СИ

        :param serial_si: номер СИ
        :param name_si: наименование СИ
        :param verif_year: год поверки СИ
        :param type_si: тип СИ
        :param mode: режим формирования запроса: only_si - поиск только по номеру СИ,
                                                type_si - поиск в том числе и по типу СИ.
        :return: строка SQL-запроса
        """
        sql_query = None
        match mode:
            case 'only_si':
                sql_query = self.lst_sql_scripts_metrology[0].format(serial_si)
            case 'type_si':
                sql_query = self.lst_sql_scripts_metrology[1].format(serial_si, type_si, type_si)

        return sql_query

    def get_card_for_si(self, dict_filter):
        """
        Метод для получения данных из БД по текущему СИ

        :param dict_filter: словарь с данными
        :return: список карточек по найденным совпадениям
        """
        serial_si, name_si, verif_year, type_si = self.__parse_dict_filter_for_card(dict_filter)
        sql_query = self.__format_get_metrology_query(serial_si=serial_si,
                                                      name_si=name_si,
                                                      verif_year=verif_year,
                                                      type_si=type_si,
                                                      mode='type_si')
        data_from_metrology = self.__get_data_from_db(sql_query)
        output_data_metrology.info(f"Данные по СИ: {serial_si}, {type_si}, {verif_year}\n{data_from_metrology}")
        if len(data_from_metrology) == 1:
            current_card = CardFgis(data_from_metrology[0])
            return current_card
        elif len(data_from_metrology) == 0:
            logger.info(f"Для текущего СИ {serial_si} не найдено ни одного значения, уточните параметры поиска.")
        else:
            lst_card = [CardFgis(item) for item in data_from_metrology]
            # current_card = self.__parse_fgis_card(lst_card, dict_filter['type_si'], dict_filter['verif_date'])
            return lst_card

    def check_tbmetrology_value(self, value):
        """
        Метод для проверки значения в таблице tbmetrology

        :param value: словарь с данными
        :return: True или False
        """
        sql_query = (f"""select tt1.type_number, tmod.modification, tm.si_number, tm.valid_date, tm.docnum, tt1.type_title, tt.title, org.name_org, tm.applicability, tm.vri_id, tm.verif_date
                        from tbmetrology tm, tbtitle tt, tbtype tt1, tbmodification tmod, tborgmetrology org
                        where (tm.mitnumber = tt1.id_type and tt1.type_title = '{value['mitype']}' and tt1.type_number = '{value['mitnumber']}' and tm.mitype = tt1.id_type) and 
                        (tm.modification = tmod.id_mod and tmod.modification = '{value['modification']}') and 
                        (tm.title = tt.id_title and tt.title = '{value['title']}') and
                        (tm.org_title = org.id_org and org.name_org = '{value['org_title']}') and
                        tm.si_number like '{value['si_number']}' and tm.docnum = '{value['docnum']}';""")
        out_res = self.__get_data_from_db(sql_query)

        return False if out_res == [] else True

    def __parse_fgis_card(self, lst_cards, type_si, verif_date):
        """
        Метод для парсинга списка CardFgis

        :param lst_cards:
        :return: Объект CardFgis, или None - если не найдено однозначного
                соответствия.
        """
        date = verif_date.date()
        for card in lst_cards:
            current_type = card.mi_mitype
            current_verif_date = card.verification_date
            current_mod = card.mi_modification
            # dict_type_si = self.__parse_type_si(type_si)

    def get_change_date(self, si_inform: dict):
        """
        Проверка даты изменения информации в БД

        :return:
        """
        pass

    def change_inform(self, si_inform: dict):
        """
        Метод для получения информации об изменении из БД

        :param si_inform: словарь с информацией по СИ
        :return:
        """
        pass

    def __add_type_record(self, type_title: str, type_number: str):
        """
        Метод для добавления данных в таблицу типов tbtype

        :param type_title: Тип СИ
        :param type_number: регистрационный номер типа СИ во ФГИС
        :return: True or False
        """
        try:
            sql_query = f"call add_type('{type_title}', '{type_number}')"
            if self.__write_data(sql_query):
                return True
            else:
                return False
        except Exception as err:
            logger.warning(f"Не удалось добавить запись в таблицу tbtype. Ошибка: {err.__str__()}")
            return False

    def __add_modification_record(self, modification: str):
        """
        Метод для добавления данных в таблицу модификаций типов tbmodofication

        :param modification: модификация типа СИ
        :return: True or False
        """
        try:
            sql_query = f"call add_modification('{modification}')"
            if self.__write_data(sql_query):
                return True
            else:
                return False
        except Exception as err:
            logger.warning(f"Не удалось добавить запись в таблицу tbmodofication. Ошибка: {err.__str__()}")
            return False

    def __add_orgmetrology_record(self, organization: str):
        """
        Метод для добавления данных в таблицу организаций-поверителей tborgmetrology

        :param organization: организация-поверитель
        :return: True or False
        """
        try:
            sql_query = f"call add_org_metrology('{organization}')"
            if self.__write_data(sql_query):
                return True
            else:
                return False
        except Exception as err:
            logger.warning(f"Не удалось добавить запись в таблицу tborgmetrology. Ошибка: {err.__str__()}")
            return False

    def __add_title_record(self, title: str):
        """
        Метод для добавления данных в таблицу наименования видов СИ tbtitle

        :param title: наименование вида СИ
        :return: True or False
        """
        try:
            sql_query = f"call add_title('{title}')"
            if self.__write_data(sql_query):
                return True
            else:
                return False
        except Exception as err:
            logger.warning(f"Не удалось добавить запись в таблицу tbtitle. Ошибка: {err.__str__()}")
            return False

    def get_id_mod(self, modification: str):
        """
        Метод для получения идентификатора записи id_mod из таблицы
        tbmodification по модификации типа СИ (полное совпадение)

        :param modification: строка модификации
        :return: идентификатор id_mod: int
        """
        sql_query = f"select get_id_mod('{modification}')"
        try:
            return self.__get_data_from_db(sql_query)[0][0]
        except:
            logger.warning(f"Не удалось получить id_mod из БД по запросу: {sql_query}")

    def get_id_org(self, organization: str):
        """
        Метод для получения идентификатора записи id_org из таблицы
        tborgmetrology по наименованию организации-поверителя (полное совпадение)

        :param organization: строка организации-поверителя
        :return: идентификатор id_org: int
        """
        sql_query = f"select get_id_org('{organization}')"
        try:
            return self.__get_data_from_db(sql_query)[0][0]
        except:
            logger.warning(f"Не удалось получить идентификатор id_org из локальной БД по запросу: {sql_query}")

    def get_id_title(self, si_title: str):
        """
        Метод для получения идентификатора записи id_title из таблицы
        tbtitle по наименованию вида СИ (полное совпадение)

        :param si_title: строка вида СИ
        :return: идентификатор id_title
        """
        sql_query = f"select get_id_title('{si_title}')"
        try:
            return self.__get_data_from_db(sql_query)[0][0]
        except:
            logger.warning(f"Не удалось получить идентификатор id_title из локально БД по запросу: {sql_query}")

    def get_id_type(self, type_title: str, type_number: str):
        """
        Метод для получения идентификатора записи id_type из таблицы
        tbtype по типу СИ и регистрационному номеру СИ (полное совпадение)

        :param type_title: строка типа СИ
        :param type_number: строка регистрационного номера СИ
        :return: идентификатор id_type
        """
        sql_query = f"select get_id_type_full('{type_title}', '{type_number}')"
        try:
            return self.__get_data_from_db(sql_query)[0][0]
        except:
            logger.warning(f"Не удалост получить идентификатор id_type из локальной БД по запросу: {sql_query}")

    def get_id_for_href(self, href: str):
        """
        Метод для получения ижентификатора записи из таблицы tbmetrology,
        в которой найдена гиперссылка

        :param href: строка гиперссылки
        :return: идентификатор записи id_record
        """
        sql_query = f"select get_id_for_href('{href}')"
        try:
            return self.__get_data_from_db(sql_query)[0][0]
        except:
            logger.warning(f"Не удалось получить идентификатор записи для гиперссылки: {href}")

    def get_type_title(self, id_type: int):
        """
        Метод для получения типа СИ и регистрационного номера СИ из локальной БД
        по идентификатору

        :param id_type: идентификатор записи в БД
        :return: кортеж (type_title, type_number)
        """
        sql_query = f"select get_type_title({id_type})"
        try:
            return self.__get_data_from_db(sql_query)[0]
        except:
            logger.warning(f"Не удалось получить кортеж type_title по запросу: {sql_query}")

    def get_card_si(self, serial: str, type: str):
        """
        Метод для получения информации по СИ из локальной БД по серийному номеру
        при единственном совпадении

        :param serial: строка серийного номера
        :return: карточка СИ -> CardFgis, либо список из элементов CardFgis
        """
        sql_query = f"select get_card_array_on_type('{serial}', '{type}')"
        try:
            cards_fgis = self.create_lst_cardfgis(self.__get_data_from_db(sql_query)[0][0])
            return cards_fgis
        except:
            logger.warning(f"Не удалось получить данные CardFgis по серийному номеру: {serial}")

    def set_row(self, id_record: int, row_number: int):
        """
        Метод для записи номера строки файла Excel, которой соответствует
        запись в таблице tbmetrology

        :param id_record: идентификатор записи в таблице tbmetrology
        :param row_number: номер строки файла Excel
        :return:
        """
        pass

    def check_valid_href(self, href_value: str):
        """
        Метод для проверки валидности ссылки из файла Excel,
        если ссылка найдена в локальной БД, то она валидна

        :param href_value: строка гиперсссылки
        :return: True or False
        """
        return True if self.check_value((href_value,), 'href') else False

    def create_lst_cardfgis(self, source_data: list):
        """
        Метод для формирования списка объектов CardFgis

        :param source_data: список с исходными данными
        :return: список объектов CradFgis
        """
        # Список, в который будем добавлять объекты CardFgis
        result_lst = []
        for item in source_data:
            source_tuple = (item['id'],
                            item['type_number'],
                            item['modification'],
                            item['serial'],
                            item['valid_date'],
                            item['docnum'],
                            item['type_title'],
                            item['title'],
                            item['name_org'],
                            item['applicability'],
                            item['vri_id'],
                            item['verif_date'],
                            item['href'],
                            item['change_date'],
                            item['change_flag'],
                            item['rows_number'])
            result_lst.append(CardFgis(source_tuple))
        return result_lst


class CardFgis:
    """
    Класс, описывающий структуру данных - карточек ФГИС
    по средствам измерений. Содержит поля, как в карточке ФГИС:
        1. mi_mitnumber - регистрационный номер типа СИ в БД ФГИС;
        2. mi_modification - модификация типа СИ;
        3. mi_number - заводской номер СИ;
        4. valid_date - дата следующей поверки;
        5. result_docnum - серия и номер свидетельства о поверки СИ;
        6. mi_mitype - тип СИ;
        7. mi_mititle - наименование СИ;
        8. org_title - организация поверитель;
        9. applicability - флаг пригодности СИ, пригоден, если True, непригоден, если False;
        10. vri_id - уникалный идентификатор СИ в БД ФГИС;
        11. verification_date - дата последней поверки по данным ФГИС;
        12. href - ссылка на карточку СИ в БД ФГИС.
        13. change_date - дата изменения записи в БД.
        14. change_flag - флаг изменения данных в БД.
        15. number_rows - номер строки в файле Excel, которой соответствует запись.
    """

    def __init__(self, init_data: tuple):
        self.IND_PROP = {'id': 0,
                         'mi_mitnumber': 1,
                         'mi_modification': 2,
                         'mi_number': 3,
                         'valid_date': 4,
                         'result_docnum': 5,
                         'mi_mitype': 6,
                         'mi_mititle': 7,
                         'org_title': 8,
                         'applicability': 9,
                         'vri_id': 10,
                         'verification_date': 11,
                         'href': 12,
                         'change_date': 13,
                         'change_flag': 14,
                         'number_rows': 15}
        self.__card = init_data

    @property
    def id_record(self):
        """
        Идентификатор записи
        :return:
        """
        return self.__card[self.IND_PROP['id']]

    @property
    def mi_mitnumber(self):
        """
        Возвращаем регистрационный номер типа СИ
        :return: регистрационный номер типа СИ
        """
        return self.__card[self.IND_PROP['mi_mitnumber']]

    @property
    def mi_modification(self):
        """
        Модификация типа СИ
        :return: модификация типа СИ
        """
        return self.__card[self.IND_PROP['mi_modification']]

    @property
    def mi_number(self):
        """
        Заводской номер СИ
        :return: заводской номер СИ
        """
        return self.__card[self.IND_PROP['mi_number']]

    @property
    def valid_date(self):
        """
        Дата следующей поверки
        :return: дата следующей поверки
        """
        return self.__card[self.IND_PROP['valid_date']]

    @property
    def result_docnum(self):
        """
        Серия и номер свидетельства о поверке
        :return: номер свидетельства о поверке
        """
        return self.__card[self.IND_PROP['result_docnum']]

    @property
    def mi_mitype(self):
        """
        Тип СИ
        :return: тип СИ
        """
        return self.__card[self.IND_PROP['mi_mitype']]

    @property
    def mi_mititle(self):
        """
        Наименование СИ
        :return: наименование СИ
        """
        return self.__card[self.IND_PROP['mi_mititle']]

    @property
    def org_title(self):
        """
        Организация-поверитель
        :return: наименование организации-поверителя
        """
        return self.__card[self.IND_PROP['org_title']]

    @property
    def applicability(self):
        """
        Флаг пригодности к измерениям
        :return: False или True
        """
        return self.__card[self.IND_PROP['applicability']]

    @property
    def vri_id(self):
        """
        Уникальный идентификатор в БД ФГИС
        :return: идентификатор
        """
        return self.__card[self.IND_PROP['vri_id']]

    @property
    def verification_date(self):
        """
        Дата последней поверки
        :return: дата последней поверки из БД ФГИС
        """
        return self.__card[self.IND_PROP['verification_date']]

    @property
    def href(self):
        """
        Ссылка на карточку СИ
        :return: ссылка
        """
        return self.__card[self.IND_PROP['href']]

    @property
    def change_date(self):
        """
        Дата изменения записи
        :return: дата
        """
        return self.__card[self.IND_PROP['change_date']]

    @property
    def change_flag(self):
        """
        Флаг изменения
        :return:
        """
        return self.__card[self.IND_PROP['change_flag']]

    @property
    def number_rows(self):
        """
        Номер строки в файле Excel
        :return: номер строки
        """
        return self.__card[self.IND_PROP['number_rows']]

    def check_equals(self, other_card):
        """
        Метод для сравнения двух карточек, по ключевым параметрам

        :param other_card: объект CardFgis
        :return: True or False
        """
        if (self.mi_mitnumber == other_card.mi_mitnumber and
                self.mi_modification == other_card.mi_modification and
                self.result_docnum[:self.result_docnum.rfind('/')] == other_card.result_docnum[
                                                                      :other_card.result_docnum.rfind('/')] and
                self.mi_mitype == other_card.mi_mitype and
                self.mi_mititle == other_card.mi_mititle and
                self.org_title == other_card.org_title and
                self.verification_date == other_card.verification_date):
            return True
        else:
            return False


def main():
    db = WorkDb()
    tables = db.tables
    print(tables)


if __name__ == "__main__":
    main()
