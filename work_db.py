import os.path
import psycopg2 as psql
import app_logger

logger = app_logger.get_logger(__name__, 'workdb_log.log')
output_data_metrology = app_logger.get_logger('Data_from_metrology', 'workdb_log.log')

class WorkDb:
    MODE_CHECK = {'title': 'tbtitle',
                  'modification': 'tbmodification',
                  'type_title': 'tbtype',
                  'type_number': 'tbtype',
                  'name_org': 'tborgmetrology',
                  'si_number': 'tbmetrology',
                  'type': 'tbtype'}

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
            if self.tables == []:
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
        with open('scripts/create_table.sql', 'r') as script_file:
            sql_query = script_file.read()

        try:
            logger.info("Попытка создания описания таблиц в БД, так как существующая БД пуста.")
            self.cursor.execute(sql_query)
            self.connect.commit()
            logger.info("Описание таблиц БД успешно создано")
        except Exception:
            logger.warning(f"Не удалось создать описание таблиц в БД. Ошибка {Exception.__str__()}")
            self.connect.rollback()

    def __write_data_on_db(self, table, dict_for_write):
        """
        Метод для записи данных в БД

        :param dict_for_write: словарь для записи, где ключи - наименования столбцов,
                                значения - значения столбцов
        :return:
        """
        print("Начало __write_data_on_db...")
        lst_name_column = list(dict_for_write.keys())
        lst_values = [dict_for_write[key] for key in lst_name_column]
        format_sql_query = f"""insert into {table} ({', '.join(map(str, lst_name_column))}) values ({", ".join(map(lambda x: f"'{x}'", lst_values))});"""
        print(f"Попытка записи names_columns: {lst_name_column}\n values: {lst_values}")
        logger.info(f"Попытка выполнения запроса на запись: {format_sql_query}")
        try:
            self.cursor.execute(format_sql_query)
            self.connect.commit()
            print("Запись удалась")
            return True
        except:
            self.connect.rollback()
            print("Запись не удалась\n")
            logger.warning(f"Не удалось выполнить запрос <{format_sql_query}>")
            return False

    def write_metrology(self, dict_for_write):
        logger.info(f"Начало работы функции write_metrology для данных {dict_for_write}")
        self.__write_data_on_db("tbmetrology", dict_for_write)
        logger.info(f"Окончание работы функции write_metrology для данных {dict_for_write}")

    def __get_data_from_db(self, sql_query):
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
        except:
            logger.warning(f"Не удалось получить данные по запросу <{sql_query}>")

    @property
    def tables(self):
        """
        Список таблиц БД
        :return:
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
        :return:
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

    def check_value(self, value, mode=""):
        """
        Метод для проверки значения value

        :param value: значение value для проверки
        :param mode: режим проверки, то есть что проверять,
                     значения и соответствующие таблицы БД в словаре MODE_CHECK,
                     класса WorkDb:
                     title - тип СИ, в таблице tbtitle,
                     modification - модификация типа СИ, в таблице tbmodification,
                     type_title - наименование типа СИ, в таблице tbtype,
                     type_number - номер типа СИ в базе ФГИС, в таблице tbtype,
                     name_org - наименование организации-поверителя, в таблице tborgmetrology
        :return: True - если есть, False - если нет
        """
        if not self.__check_mode(mode):
            return False
        else:
            key = mode
            table = WorkDb.MODE_CHECK[key] if key in WorkDb.MODE_CHECK else None
            if table == None:
                logger.warning(f"Некорректно задан параметр mode = {mode}")
            column = self.dict_check[key]
            sql_query = self.__format_check_query(value, table, column, key, mode="read")
            execut_sql_query = self.__get_data_from_db(sql_query)
            if execut_sql_query == []:
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
        Метод для получения id_title с БД по существующему значению value
        :param value: значение по поиску
        :param mode: столбец для фильтрации
        :return: id_title
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
            id = sql_executable_data[0][0]

            return id

    def set_id(self, value, mode=""):
        """
        Метод для записи значения value в БД
        :param value: значение для записи
        :param mode: наименование столбца для записи
        :return:
        """
        if self.__check_mode(mode):
            table = WorkDb.MODE_CHECK[mode]
            if table == 'tbtype':
                column = 'type_title, type_number'
            else:
                column = mode

            sql_query = self.__format_check_query(value, table, column, mode="write")
            try:
                self.cursor.execute(sql_query)
                self.connect.commit()
                logger.info(f"Данные по запросу <{sql_query}> успешно записаны")
            except:
                logger.warning(f"Не удалось записать данные по запросу <{sql_query}>")

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
                        tm.si_number like '{value['si_number']}';""")
        out_res = self.__get_data_from_db(sql_query)

        return True if len(out_res) > 0 else False

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



    # def __parse_type_si(self, current_type_si):
    #     """
    #     Метод для парсинга строки типа СИ, для корректного поиска в БД
    #     :param current_type_si:
    #     :return: dictionary
    #     """
    #     SEP_CHAR = [" ", "-", "/"]
    #     # space_lst_types = current_type_si.split(sep=" ")
    #     # tire_lst_types = current_type_si.split(sep="-")
    #     # slesh_lst_types = current_type_si.split(sep="/")
    #
    #     dict_type_si = {sep: current_type_si.split(sep=sep) if len(current_type_si.split(sep=sep) > 1 else None) for sep in SEP_CHAR}
    #     return dict_type_si


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
    """

    def __init__(self, init_data: tuple):
        self.IND_PROP = {'mi_mitnumber': 1,
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
                    'href': 12}
        self.__card = init_data

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


def main():
    db = WorkDb()
    tables = db.tables
    print(tables)

if __name__ == "__main__":
    main()