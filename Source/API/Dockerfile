FROM python:3.12-slim

# RUN mkdir -p ~/.pip
# RUN printf "[global]\ntimeout=900\nno-cache-dir=true\ntrusted-host=pip.cache.alvand.ir\nindex-url=http://pip.cache.alvand.ir/simple\n" > ~/.pip/pip.conf

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]