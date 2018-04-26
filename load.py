import argparse
import pandas as pd
import numpy as np
from pony.orm import db_session
import datetime
from db import db_bind, update_sales_figure


cars = {
    'BMW i3': ('BMW', 'i3'),
    'BMW i8': ('BMW', 'i8'),
    'Tesla X': ('Tesla', 'Model X'),
    'Tesla Model S': ('Tesla', 'Model S'),
    'Tesla Model X': ('Tesla', 'Model X'),
    'Tesla Model 3': ('Tesla', 'Model 3'),
    'Tesla S': ('Tesla', 'Model S'),
    'Tesla 3': ('Tesla', 'Model 3'),
    'Chevy Bolt': ('Chevrolet', 'Bolt'),
    'Chevy Spark': ('Chevrolet', 'Spark'),
    'Ford Focus Electric': ('Ford', 'Focus'),
    'Focus e': ('Ford', 'Focus'),
    'Nissan Leaf': ('Nissan', 'Leaf'),
    'Kia Soul EV': ('Kia', 'Soul'),
    'Volkswagen e-Golf': ('Volkswagen', 'e-Golf'),
    'VW e-Golf': ('Volkswagen', 'e-Golf'),
    'Fiat 500e': ('Fiat', '500e'),
    'Honda Clarity BEV': ('Honda', 'Clarity'),
    'Smart ED': ('Smart', 'ED'),
}

# Sources
# InsideEVs.com 2017: https://insideevs.com/ev-sales-rise-december-tesla-leading/
# EVobsession.com 2017: https://evobsession.com/2017-electric-car-sales-us-china-europe-month-month/



def get_source(filename):
    if 'EV_Obs' in filename:
        return 'http://evobsession.com'
    elif 'Hybrid' in filename:
        return 'http://www.hybridcars.com'
    elif 'InsideEV' in filename:
        return 'https://insideevs.com'
    else:
        return None


def get_arguments():
    parser = argparse.ArgumentParser(description='Load EV sales data spreadsheet into database')
    parser.add_argument('-m', '--mode',
                        help='how to load the data, add only adds new data, update adds and updates existing data',
                        choices=['add', 'update'], default='update')
    parser.add_argument('filename', nargs='*', help='Files to load')

    args = parser.parse_args()

    if args.mode == 'update':
        print('Will update existing records')
        update = True
    else:
        print('Will only add missing data')
        update = False

    files = args.filename
    print('Will load {} files'.format(len(files)))

    return update, files


def load_file(fn, update_existing):
    source = get_source(fn)
    if not source:
        return None
    else:
        new_cnt = 0
        upd_cnt = 0

    data = pd.read_excel(fn)
    for k, v in data.iterrows():
        try:
            (make, model) = cars[k]
        except:
            print('ERROR: Could not map car: {}'.format(k))
            raise KeyError('Unmapped car identifier "{}" is missing from cars dict'.format(k))

        for d, c in v.items():
            if isinstance(d, datetime.datetime) and c and (not np.isnan(c)):
                status, added = update_sales_figure(d.year, d.month,make, model, int(c), source, update_existing)
                if status:
                    if added:
                        new_cnt += 1
                    else:
                        upd_cnt += 1
    return new_cnt, upd_cnt


if __name__ == '__main__':
    update, files = get_arguments()

    db = db_bind()

    with db_session:
        for f in files:
            print('Loading file:', f)
            new_records, updated_records = load_file(f, update)
            print('    {} new records, {} updated records'.format(new_records, updated_records))
            print()
