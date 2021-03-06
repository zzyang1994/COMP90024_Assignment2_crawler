FROM python:3.9.12-slim
RUN mkdir /usr/app
WORKDIR /usr/app
ENV PYTHONUNBUFFERED 1
COPY . .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 5000
CMD ["python", "crawler.py"]