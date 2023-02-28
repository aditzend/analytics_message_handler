# 
FROM python:3.10-slim

# 




WORKDIR /code

# 
COPY ./app/requirements.txt /code/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 
COPY ./app /code/app

# 
CMD ["python", "/code/app/main.py"]