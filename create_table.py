import time
import sqlite3
import os
import csv
import sys


def import_file(fle, data_path=None, db_path=None):
    if not db_path:
        db_path = os.path.join(os.curdir, 'resources', 'fedex.db')

    if not data_path:
        data_path = os.path.join(os.curdir, 'data', fle)

    db = sqlite3.connect(db_path)
    db.execute('DROP TABLE IF EXISTS tbl;')
    db.execute(('CREATE TABLE tbl(ADOJT CHAR(50), ADOST CHAR(2), '
                'ADPR INT(10), Record INT(8), Source CHAR(20));'))
    db.commit()

    with open(data_path) as tabfile:
        data = csv.reader(tabfile, delimiter='\t')
        for n, rec in enumerate(data, 1):
            sql = 'INSERT INTO tbl VALUES(?, ?, ?, ?, ?);'
            db.execute(sql, (rec[20], rec[21], rec[22], n, fle))
            db.execute(sql, (rec[27], rec[28], rec[29], n, fle))
            db.execute(sql, (rec[34], rec[35], rec[36], n, fle))
            db.execute(sql, (rec[41], rec[42], rec[43], n, fle))
            db.execute(sql, (rec[48], rec[49], rec[50], n, fle))

    db.commit()
    db.close()


def export_table(db_path=None, table_path=None):
    if not db_path:
        db_path = os.path.join(os.curdir, 'resources', 'fedex.db')

    if not table_path:
        table_path = os.path.join(os.curdir, 'resources', 'table.dat')

    db = sqlite3.connect(db_path)

    # with open(table_path, 'w', encoding='UTF-8', newline='\n') as t:
    with open(table_path, 'w') as t:
        table = csv.writer(t, delimiter='\t', quoting=csv.QUOTE_ALL)
        table.writerow(['ADOJT', 'ADOST', 'ADPR', 'recNo', 'Source'])

        qry = db.execute(("SELECT "
                          "TRIM(ADOJT) AS ADOJT, "
                          "TRIM(ADOST) AS ADOST, " 
                          "TRIM(sum(ADPR)) AS ADPR, " 
                          "TRIM(record) AS record, " 
                          "TRIM(source) AS source " 
                          "FROM tbl WHERE ADOJT <> '' "
                          "GROUP BY ADOJT, ADOST, record "
                          "ORDER BY CAST(record AS integer);")
                         )

        for result in qry:
            table.writerow(result)

    db.commit()
    db.close()


def main(fle, relative_path=True):
    if relative_path:
        """ 
        working with a relative file path name
        './resources' and './data' folders exist
        """
        import_file(fle)
        export_table()
    else:
        """
        working with full file path name
        './resources' and './data' folders do not exist
        """
        data_path = os.path.abspath(fle)
        db = os.path.join(os.path.dirname(data_path), 'fedex.db')
        filename = os.path.basename(data_path)
        table_path = os.path.join(os.path.dirname(db), 'table.dat')

        import_file(filename, data_path, db)
        export_table(db, table_path)


if __name__ == '__main__':
    # print(sys.version_info[0])
    try:
        main(sys.argv[1], True)
    except IndexError:
        f = input("Full file path to FedEx file: ")
        if os.path.exists(f):
            main(f, False)
        else:
            print("File name / path error")
            time.sleep(3)
