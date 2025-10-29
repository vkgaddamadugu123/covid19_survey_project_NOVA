# How to Run in Google Colab

## 1. Install dependencies:

`!pip -q install pandas sqlalchemy psycopg2-binary requests matplotlib pytest`


## 2. Setup the PostgreSQL connection

`import os
 os.environ\["PG\_URL"] = (
   "postgresql+psycopg2://neondb\_owner:npg\_2wdZpRaF0LmG@"
   "ep-patient-dawn-advbhv1g-pooler.c-2.us-east-1.aws.neon.tech/"
   "neondb?sslmode=require"
)`


## 3. Run the main ingestion

`!python Ingestion.py`


## 4. Verify the data in PostgreSQL table

```python

from sqlalchemy import create\_engine, text

import os

engine = create\_engine(os.environ\["PG\_URL"], pool\_pre\_ping=True)

with engine.connect() as conn:
   print(conn.execute(text("SELECT COUNT(\*) FROM covid\_case\_surveillance")).scalar())
   print(conn.execute(text("SELECT COUNT(DISTINCT res\_state) FROM covid\_case\_surveillance")).scalar())
'

### Preview the data
   ``` for row in conn.execute(text("SELECT \* FROM covid\_case\_surveillance LIMIT 5")):
       print(row)
   ``` 

## 5. Run the Transform and Visualization scripts

`!python Covid19_Transform.py`


## 6.Truncate the previously loaded data

```python
from sqlalchemy import create\_engine, text
engine = create\_engine(os.environ\["PG\_URL"], pool\_pre\_ping=True)

with engine.begin() as conn:
   conn.execute(text("TRUNCATE TABLE covid\_case\_surveillance"))
```
