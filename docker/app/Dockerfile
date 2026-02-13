FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir \
    matplotlib \
    seaborn \
    plotly \
    openpyxl \
    xlsxwriter \
    reportlab \
    faker \
    tqdm

RUN mkdir -p /app/data /app/reports /app/logs /app/config

COPY . /app

ENV PYTHONPATH="/app:${PYTHONPATH}"

CMD ["tail", "-f", "/dev/null"]