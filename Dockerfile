FROM python:3.9

WORKDIR /app

COPY . .

# Install dependencies including gunicorn
RUN pip install --no-cache-dir -r requirements.txt && \
    python3 -m pip install gunicorn

# Expose the port for Gunicorn
EXPOSE 5000

# Run the app using Gunicorn with 4 workers
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "app:create_app()"]
