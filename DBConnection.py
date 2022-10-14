import pymysql


class DBConnection:
    __instance__ = None

    def __init__(self):
        if DBConnection.__instance__ is None:
            # replace the db_host with public IP for local and the private one for the Cloud
            con = pymysql.connect(host="34.136.243.20", user="root", password="rootuser", db="importfromjson")
            DBConnection.__instance__ = con
        else:
            raise Exception("You cannot create another SingletonGovt class")

    @staticmethod
    def get_db_connection():
        # print(DBConnection.__instance__)
        if not DBConnection.__instance__:
            DBConnection()
        return DBConnection.__instance__
