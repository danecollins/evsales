# standard python includes
import os
import sys
import datetime
import pytz

from pony import orm
from pony.orm import Required, select


db = orm.Database()


class SalesFigure(db.Entity):
    cars_sold = Required(int)
    year = Required(int)
    month = Required(int)
    make = Required(str)
    model = Required(str)
    source = Required(str)

    @classmethod
    def from_data(cls, year, month, make, model, sold, source):
        self = cls(cars_sold=sold,
                   year=year,
                   month=month,
                   make=make,
                   model=model,
                   source=source
                   )
        return self


def db_bind():
    global db
    filename = './cars_sold.sqlite'
    db.bind('sqlite', filename, create_db=True)
    db.generate_mapping(create_tables=True)
    return db


@orm.db_session
def update_sales_figure(year, month, make, model, sold, source, update):
    sf = select(s for s in SalesFigure if (s.make == make) and\
                                          (s.model == model) and\
                                          (s.year == year) and\
                                          (s.month == month) and\
                                          (s.source == source)).first()
    if sf and update:
        sf.cars_sold = sold
        added = False
        print('Updated record for', model, make, year, month)
    else:
        SalesFigure.from_data(year, month, make, model, sold, source)
        added = True
    orm.commit()
    return True, added
