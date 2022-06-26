FROM python:3.9.13-slim

ADD ./server /server

WORKDIR /server
RUN python -m pip install --upgrade pip
RUN pip install pandas flask scikit-learn tabulate

EXPOSE 5000

CMD [ "python", "./main.py" ]