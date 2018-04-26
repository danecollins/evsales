from pony.orm import db_session, select
from db import db_bind, SalesFigure


if __name__ == '__main__':

    with open('cars_sold.csv', 'w') as fp:
        db = db_bind()
        print('year,month,make,model,sold,source', file=fp)
        with db_session:
            for sf in select(s for s in SalesFigure):
                print('{},{},{},{},{},{}'.format(sf.year, sf.month, sf.make, sf.model,
                                                 sf.cars_sold, sf.source), file=fp)
