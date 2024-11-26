FROM python:3.12-slim-bullseye

# Fix python encoding
ENV PYTHONUTF8='1'
ENV PYTHONIOENCODING='utf-8'

WORKDIR /app

# Install requirements
COPY requirements.in .
RUN pip install --no-cache-dir pip==24.3.1 setuptools==75.3.0 wheel==0.44.0
RUN pip install --no-cache-dir pip-tools==7.4.1
RUN pip-compile --strip-extras
RUN pip install --no-cache-dir -r requirements.txt --no-deps

# Copy source files after requirements to use its cached layer when source files are updated
COPY . .

EXPOSE 2127

ENTRYPOINT ["/app/entrypoint.sh"]
