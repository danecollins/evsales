from pony.orm import *
from db import db_bind, SalesFigure, update_sales_figure

# set_sql_debug(True)
db = db_bind()

with db_session:
    update_sales_figure(2018, 2, 'Tesla', 'Model S', 2002, 'InsideEv')
    update_sales_figure(2018, 2, 'Tesla', 'Model S', 2012, 'HybridCars')
