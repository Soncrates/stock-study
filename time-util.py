def get_year_parameters(time=10) :
    import datetime as dt
    today = dt.datetime.today()
    d = dt.timedelta(days = ((time+1)*365))
    start_date = today -d
    return start_date.year, today.year, range(start_date.year,today.year)
def getLastDay(month,year) :
    import datetime
    ret = datetime.date (year, month+1, 1) - datetime.timedelta (days = 1)
    return ret.day
