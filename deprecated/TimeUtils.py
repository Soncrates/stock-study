class TimeUtil(object) :
    @staticmethod
    def get_year_parameters(time=10) :
      import datetime as dt
      today = dt.datetime.today()
      d = dt.timedelta(days = ((time+1)*365))
      start_date = today -d
      return start_date.year, today.year, range(start_date.year,today.year)
    @staticmethod
    def getLastDay(month,year) :
      import datetime
      ret = datetime.date (year, month+1, 1) - datetime.timedelta (days = 1)
      return ret.day

    class ExpireTimer() :
      def __init__(self,expire=1) :
        import datetime as dt
        self.expire = dt.datetime.today() + dt.timedelta(minutes = expire)
      def __call__(self) :
        import datetime as dt
        return self.expire > dt.datetime.today()
