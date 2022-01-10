import datetime
from datetime import datetime

date1 = "2022-01-07 14:53:53.029980"
date2 = "2022-01-08T15:57:23.616Z"
#print(datetime.strptime(date2, "%Y-%m-%dT%H:%M%S.%fZ"))
date2 = date2.replace("T", " ")
print(date2[:-1] + "000" + date2[-1])
