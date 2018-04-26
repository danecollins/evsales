from pony.orm import db_session, select
from db import db_bind, SalesFigure
from collections import defaultdict, Counter
# script to check database integrity

db = db_bind()


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


if __name__ == '__main__':

    check_num_sources()