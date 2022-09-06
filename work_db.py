import psycopg2 as psql
import app_logger

logger = app_logger.get_logger(__name__)

class WorkDb:
    MODE_CHECK = {'title': 'tbtitle',
                  'modification': 'tbmodification',
                  'type_title': 'tbtype',
                  'type_number': 'tbtype',
                  'name_org': 'tborgmetrology',
                  'si_number': 'tbmetrology'}

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
                           'si_number': 'si_number'}


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
        lst_name_column = list(dict_for_write.keys())
        lst_values = [dict_for_write[key] for key in lst_name_column]
        format_sql_query = f"""insert into {table} ({', '.join(map(str, lst_name_column))}) values ({", ".join(map(lambda x: f"'{x}'", lst_values))});"""
        logger.info(f"Попытка выполнения запроса на запись: {format_sql_query}")
        try:
            self.cursor.execute(format_sql_query)
            self.connect.commit()
            return True
        except:
            self.connect.rollback()
            logger.warning(f"Не удалось выполнить запрос <{format_sql_query}>")
            return False

    def write_metrology(self, dict_for_write):
        self.__write_data_on_db("tbmetrology", dict_for_write)

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

        :param title: значение value для проверки
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
            sql_query = self.__format_check_query(value, table, column, mode, mode="read")
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

    def __format_get_metrology_query(self, serial_si, name_si, verif_year, type_si):
        """
        Форматируем и формируем строку запроса в БД,
        для получения данных по СИ
        :param serial_si: номер СИ
        :param name_si: наименование СИ
        :param verif_year: год поверки СИ
        :param type_si: тип СИ
        :return: строка SQL-запроса
        """
        pass

    def get_card_for_si(self, dict_filter):
        """
        Метод для получения данных из БД по текущему СИ
        :param dict_filter: словарь с данными
        :return: список карточек по найденным совпадениям
        """
        serial_si, name_si, verif_year, type_si = self.__parse_dict_filter_for_card(dict_filter)


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
        8. org_title - органицазия поверитель;
        9. applicability - флаг пригодности СИ, пригоден, если True, непригоден, если False;
        10. vri_id - уникалный идентификатор СИ в БД ФГИС;
        11. verification_date - дата последней поверки по данным ФГИС;
        12. href - ссылка на карточку СИ в БД ФГИС.
    """

    def __init__(self, init_data: tuple):
        self.IND_PROP = {'mi_mitnumber': 0,
                    'mi_modification': 1,
                    'mi_number': 2,
                    'valid_date': 3,
                    'result_docnum': 4,
                    'mi_mitype': 5,
                    'mi_mititle': 6,
                    'org_title': 7,
                    'applicability': 8,
                    'vri_id': 9,
                    'verification_date': 10,
                    'href': 11}
        self.__card = init_data

    @property
    def mi_mitnumber(self):
        """
        Возвращаем регистрационный номер типа СИ
        :return: регистрационный номер типа СИ
        """
        return self.__card[0]

    @property
    def mi_modification(self):
        """
        Модификация типа СИ
        :return: модификация типа СИ
        """
        return self.__card[1]

    @property
    def mi_number(self):
        """
        Заводской номер СИ
        :return: заводской номер СИ
        """
        return self.__card[2]

    @property
    def valid_date(self):
        """
        Дата следующей поверки
        :return: дата следующей поверки
        """
        return self.__card[3]

    @property
    def result_docnum(self):
        """
        Серия и номер свидетельства о поверке
        :return: номер свидетельства о поверке
        """
        return self.__card[4]

    @property
    def mi_mitype(self):
        """
        Тип СИ
        :return: тип СИ
        """
        return self.__card[5]

    @property
    def mi_mititle(self):
        """
        Наименование СИ
        :return: наименование СИ
        """
        return self.__card[6]

    @property
    def org_title(self):
        """
        Организация-поверитель
        :return: наименование организации-поверителя
        """
        return self.__card[7]

    @property
    def applicability(self):
        """
        Флаг пригодности к измерениям
        :return: False или True
        """
        return self.__card[8]

    @property
    def vri_id(self):
        """
        Уникальный идентификатор в БД ФГИС
        :return: идентификатор
        """
        return self.__card[9]

    @property
    def verification_date(self):
        """
        Дата последней поверки
        :return: дата последней поверки из БД ФГИС
        """
        return self.__card[10]

    @property
    def href(self):
        """
        Ссылка на карточку СИ
        :return: ссылка
        """
        return self.__card[11]


def main():
    db = WorkDb()
    tables = db.tables
    print(tables)

if __name__ == "__main__":
    main()