From Python Python:3.12.10
WORKDIR /app
COPY requirements.txt /app

RUN pip install -r requirements.txt
CMD["python","app.py","dev"]

