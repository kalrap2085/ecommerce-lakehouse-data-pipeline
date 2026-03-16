from datetime import datetime as dt, timedelta
from pyspark.sql.functions import quarter

def generate_date(start_date : str = '2025-01-01', numYears : int = 1) -> list :
    _date_list = []
    
    _first_date = dt.strptime(start_date,'%Y%m%d')
    _date_list.append([
        int(dt.strftime(_first_date,'%Y%m%d')),
        dt.strftime(_first_date,'%Y-%m-%d'),
        dt.strftime(_first_date,'%Y'),
        (_first_date.month -1) //3 + 1,
        dt.strftime(_first_date,'%m'),
        dt.strftime(_first_date,'%d'),
        dt.strftime(_first_date,'%U'),
        dt.strftime(_first_date,'%w'),
        dt.strftime(_first_date,'%A'),
        dt.strftime(_first_date,'%B'),
        _first_date.weekday() >=5,
    ])
    
    _next_date = _first_date
    
    for _ in range(1, numYears*365):
        _next_date = _next_date + timedelta(days = 1)
        
        _date_list.append([
        int(dt.strftime(_next_date,'%Y%m%d')),
        dt.strftime(_next_date,'%Y-%m-%d'),
        dt.strftime(_next_date,'%Y'),
        (_next_date.month -1) //3 + 1,
        dt.strftime(_next_date,'%m'),
        dt.strftime(_next_date,'%d'),
        dt.strftime(_next_date,'%U'),
        dt.strftime(_next_date,'%w'),
        dt.strftime(_next_date,'%A'),
        dt.strftime(_next_date,'%B'),
        _next_date.weekday() >=5,
    ])
        
    return _date_list    
