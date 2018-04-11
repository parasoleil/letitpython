import sys
import numpy as np
import pandas as pd
import psycopg2 as ps
import logging


class Trade:

    def __init__(self, filename):
        self.filename = filename

    '''Set up logger'''
    try:
        # connect to db
        logger = logging.getLogger('ConnectToDb')
        logger.setLevel(logging.DEBUG)

        hdlr = logging.FileHandler('C:/Users/tina/Documents/Python Scripts/tst.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)

        logger.addHandler(hdlr)
        logger.info('Start log')

    except Exception as elog:
        print("Error: " + str(elog))

    def connecttodatabase(logger):
        try:
            logger.info('Connect to db and return top 10 records')
            conn = ps.connect("dbname='someData' user='user' password='pass' host='localhost'")
            cur = conn.cursor()
            cur.execute("""SELECT * from test.tstfreight LIMIT 10""")
            rows = cur.fetchall()
            # print rows
            for row in rows:
                print("   ", row[0], row[1], row[2], row[3], row[4], row[6])

        except Exception as ex:
            print("Error: " + str(ex))
            print.error('Failed to connect to the database')
            logger.info('Unable to connect to db and return top 10 records ' + str(ex))

        finally:
            conn.close()

    def readfile(logger):
        logger.info('Reading csv from folder')
        try:
            # connect to input csv file
            filename = r'C:\Users\tina\Downloads\DS-018995_1_Data.csv'
            # with open(filename, 'rb') as csvfile:
            # filereader = csv.reader(csvfile, delimiter=',')
            # file = open(filename, 'rb')
            columnsandrows = []
            with open(filename) as f:
                for line in f:
                    inner_list = [elt.strip() for elt in line.replace('"', '').split(',')]
                    columnsandrows.append(inner_list)
            # rows = file.read().splitlines()

            arraycolumnsandrows = np.array(columnsandrows)
            arraycolumnsandrowspd = pd.DataFrame(arraycolumnsandrows[1:])

            logger.info('''Let's aggregate...''')
            print(arraycolumnsandrowspd[:3])
            # imports = pd.DataFrame({'Imports': arraycolumnsandrowspd.groupby()})

        except Exception as ec:
            print("Error: " + str(ec))
            logger.error('Failed to read the csv')


def main():
    Trade.readfile(Trade.logger)
    Trade.connecttodatabase(Trade.logger)


if __name__ == "__main__":
    main()
