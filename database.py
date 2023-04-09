import teradatasql
from multiprocessing import Process, Queue


class Database:
    """Use teradatasql connect to TPCH database and apply show/create/drop index operations."""

    # Only primary and foreign keys
    tables = {
        "PARTTBL": ["P_PARTKEY"],
        "SUPPLIER": ["S_SUPPKEY", "S_NATIONKEY"],
        "PARTSUPP": ["PS_PARTKEY", "PS_SUPPKEY"],
        "CUSTOMER": ["C_CUSTKEY", "C_NATIONKEY"],
        "ORDERTBL": ["O_ORDERKEY", "O_CUSTKEY"],
        "LINEITEM": ["L_ORDERKEY", "L_LINENUMBER", "L_PARTKEY", "L_SUPPKEY"],
        "NATION": ["N_NATIONKEY", "N_REGIONKEY"],
        "REGION": ["R_REGIONKEY"],
    }

    def __init__(self):
        # Database connection config
        self.DB_CONFIG = {
            "user": "DBC",
            "password": "DBC",
            "host": "192.168.56.56",
            "database": "tpch",
        }

    """
        Action-related methods
    """

    def drop_index(self, column, table):
        """Drop column on table"""
        proc = Process(target=self.__drop_index, args=(column, table))
        proc.start()
        proc.join()

    def __drop_index(self, column, table):
        command = "DROP INDEX idx_%s ON %s;" % (column, table)
        try:
            self.conn = teradatasql.connect(**self.DB_CONFIG)
            self.cur = self.conn.cursor()
            self.cur.execute(command)
            self.conn.commit()
            self.cur.close()
            self.conn.close()
            print("Dropped index on (%s) %s" % (table, column))
        except teradatasql.OperationalError as ex:
            print("Didn't drop index on %s, error %s" % (column, ex))

    def create_index(self, column, table):
        """Create column on table"""
        proc = Process(target=self.__create_index, args=(column, table))
        proc.start()
        proc.join()

    def __create_index(self, column, table):
        command = "CREATE INDEX idx_%s (%s) ON %s;" % (column, column, table)
        try:
            self.conn = teradatasql.connect(**self.DB_CONFIG)
            self.cur = self.conn.cursor()
            self.cur.execute(command)
            self.conn.commit()
            self.cur.close()
            self.conn.close()
            print("Created index on (%s) %s" % (table, column))
        except teradatasql.OperationalError as ex:
            print("Didn't create index on %s, error %s" % (column, ex))

    """
        State-related methods
    """

    def get_table_columns(self, table):
        """Get column names of table(without the primary keys)"""
        result_queue = Queue()
        proc = Process(target=self.__get_table_columns, args=(table, result_queue))
        proc.start()
        ret = result_queue.get()
        proc.join()
        return ret

    def __get_table_columns(self, table, result_queue):
        self.conn = teradatasql.connect(**self.DB_CONFIG)
        self.cur = self.conn.cursor()
        self.cur.execute("help table %s;" % table)
        table_columns = list()
        for row in self.cur.fetchall():
            row = row[0].strip()
            if row not in self.tables[table]:
                table_columns.append(row)
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        result_queue.put(table_columns)
        return table_columns

    def get_table_indexed_columns(self, table):
        """Get indexed column names of table"""
        result_queue = Queue()
        proc = Process(
            target=self.__get_table_indexed_columns, args=(table, result_queue)
        )
        proc.start()
        ret = result_queue.get()
        proc.join()
        return ret

    def __get_table_indexed_columns(self, table, result_queue):
        self.conn = teradatasql.connect(**self.DB_CONFIG)
        self.cur = self.conn.cursor()
        self.cur.execute("help index %s;" % table)
        table_indexes = list()
        for index in self.cur.fetchall():
            lowered_index = index[2]
            table_indexes.append(lowered_index)
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        result_queue.put(table_indexes)
        return table_indexes

    def get_indexes_map(self):
        """Return a dictionary
        if column is indexed:
            map[table][column] = 1
        else:
            map[table][column] = 0
        """
        indexes_map = dict()
        for table in self.tables.keys():
            indexes_map[table] = dict()
            indexed_columns = self.get_table_indexed_columns(table)
            table_columns = self.get_table_columns(table)
            for column in table_columns:
                indexes_map[table][column] = 0
                for index in indexed_columns:
                    if column == index:
                        indexes_map[table][column] = 1

        return indexes_map

    def reset_indexes(self):
        """reset all the indexes"""
        proc = Process(target=self.__reset_indexes, args=())
        proc.start()
        proc.join()

    def __reset_indexes(self):
        # FETCH INDEX NAMES
        print("in database.py reset indexes")
        self.conn = teradatasql.connect(**self.DB_CONFIG)
        print("in database.py connection open")
        self.cur = self.conn.cursor()

        for table in self.tables.keys():
            self.cur.execute("help index %s;" % table)
            column_names = list()
            index_names = list()

            for index in self.cur.fetchall():
                column_names.append(index[2])
                index_names.append(index[5])

            for column, index in zip(column_names, index_names):
                print("in database.py colum : ", column)
                if index is not None and "idx_" in index:
                    print("in database.py DROP INDEX %s ON %s;" % (index, table))
                    self.cur.execute("DROP INDEX %s ON %s;" % (index, table))

            for column in self.tables[table]:
                if column in column_names:
                    continue
                print("in database.py CREATE INDEX %s ON %s;" % (column, table))
                self.cur.execute(
                    "CREATE INDEX idx_%s (%s) ON %s;" % (column, column, table)
                )

        self.conn.commit()
        self.cur.close()
        self.conn.close()
        print("in database.py connection closed")

    def analyze_tables(self):
        self.conn = teradatasql.connect(**self.DB_CONFIG)
        self.cur = self.conn.cursor()
        for table in self.tables.keys():
            try:
                self.cur.execute("help table %s;" % table)
                self.conn.commit()
                data = self.cur.fetchall()
                print(data)
            except teradatasql.OperationalError as er:
                print("Cannot find table %s" % table)
        self.cur.close()
        self.conn.close()
        print("Analyzed tables")


if __name__ == "__main__":
    db = Database()
    db.create_index("C_NAME", "CUSTOMER")
    print(db.get_table_indexed_columns("customer"))
    db.drop_index("C_NAME", "CUSTOMER")
    print(db.get_table_indexed_columns("customer"))
