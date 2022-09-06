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


def connect_db(database="fgis",
                 user="cnt",
                 password="cnt",
                 port="5432",
                 host="10.48.153.106"):
    """
    Подключение к БД. Возвращает объект connect и cursor.

    :param database: имя БД
    :param user: имя пользователя
    :param password: пароль
    :param port: порт для подключения
    :param host: адрес сервера

    :return: объект connect и объект cursor
    """
    try:
        conn = psql.connect(database=database,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
        cur = conn.cursor()
        return conn, cur
    except:
        raise ErrorConnectionDb

def create_table():
    pass



def main():
    db = WorkDb()
    tables = db.tables
    print(tables)

if __name__ == "__main__":
    main()