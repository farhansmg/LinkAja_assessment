FROM apache/airflow:2.2.5-python3.8

# update pip
RUN python3 -m pip install --upgrade pip

# install requirements
COPY ./requirements.txt /tmp/requirements.txt
RUN pip3 install --force -r /tmp/requirements.txt

RUN python3 -m pip install --upgrade pip
# RUN pip3 install --upgrade pip3
