from pony.orm import db_session, select
from db import db_bind, SalesFigure
from collections import defaultdict, Counter
# script to check database integrity

db = db_bind()


def ord(x):
    return x[0] + x[1]/100

def next_month(m):
    if m[1] == 12:
        return (m[0]+1, 1)
    else:
        return (m[0], m[1] + 1)

def check_months(months, header):
    error = False
    l = sorted(months.copy(), key=ord, reverse=True)
    start = l.pop()
    expected = next_month(start)
    month_to_check = l.pop()
    while month_to_check:
        if month_to_check != expected:
            if not error:
                print(header)
            error = True
            print('    expected {} but got {}'.format(expected, month_to_check))
        expected = next_month(month_to_check)
        if l:
            month_to_check = l.pop()
        else:
            month_to_check = None
    if error:
        print("   ", months)
    return error


# for each car, check number of sources
def check_num_sources():
    with db_session:
        cars = select((s.make, s.model) for s in SalesFigure)[:]  # distinct cars

    for make, model in cars:
        with db_session:
            # need to skip last month because data is released at different times
            data = select(s for s in SalesFigure if (s.make == make) and (s.model == model) and
                          not ((s.year == 2018) and (s.month == 3)))

            # organize by month/year
            d = defaultdict(list)
            for sf in data:
                d[(sf.year, sf.month)].append(sf)

        # first check whether each month has the same number of entries
        c = Counter()
        for x in d.values():
            c[len(x)] += 1

        if len(c) != 1:
            print('Number of sources varies for {} {}'.format(make, model))
            for k, v in c.items():
                print('     {} sources for {} months'.format(k, v))
            print()


def check_date_sequences():
    with db_session:
        cars = select((s.make, s.model, s.source) for s in SalesFigure)[:]  # distinct cars and sources
        for (ma, mo, so) in cars:
            dates = select((s.year, s.month) for s in SalesFigure if (s.make==ma) and (s.model==mo) and (s.source==so))[:]
            header = 'checking {} {} - {}'.format(ma, mo, so)
            check_months(dates, header)

if __name__ == '__main__':
    check_num_sources()
    check_date_sequences()