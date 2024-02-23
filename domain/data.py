from mtools.base.domain import Domain


class DataDomain(Domain):

    def __init__(self, database, table):

        self.dbname = database
        self.table = table
