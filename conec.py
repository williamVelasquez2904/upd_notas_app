import pymysql

def get_connection():
    #servidor = "local"  # Cambia a "remoto" si es necesario
    servidor = "remoto"  # Cambia a "remoto" si es necesario
    if servidor == "local":
        return pymysql.connect(
            host="localhost",
            user="root",
            password="",
            database="wejadminmot_dev", 
            charset='utf8mb4', 
            use_unicode=True
            #,cursorclass=pymysql.cursors.DictCursor  
        )
    if servidor == "remoto":
        return pymysql.connect(
            host="ecotrago.com",
            user="wejsolut_wejadmin",
            password="Wejs2505",
            database="wejsolut_wejadminmot", 
            charset='utf8mb4', 
            use_unicode=True
            #,cursorclass=pymysql.cursors.DictCursor  
        )

def get_connection_local():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='bdumsl',
        charset='utf8mb4'
    )