FROM python:3.9.1
RUN pip install pandas
WORKDIR /app
COPY pipline.py pipline.py
ENTRYPOINT ["python", "pipline.py"]

pipline.py
import sys
import pandas as pd
print(sys.argv)
day = sys.argv[2]
print(f'Hello world! day = f{day}')