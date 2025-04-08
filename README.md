# Run to get the binaries
sudo apt-get update
sudo apt-get install -y libpq-dev
pip install psycopg2-binary
pip install -r requirements.txt

# Report Generation Service

This microservice generates CSV reports for an organization from a PostgreSQL database and encrypts them.

## Running with Docker

1. Build the image:
   ```sh
   docker build -t report-generation-service .
   ```

2. Run the container:
   ```sh
   docker run -p 5000:5000 report-generation-service
   ```

3. Access the report endpoint:
   ```sh
   curl -O http://localhost:5000/report/org/12345
   ```
4. To run 
   gunicorn -w 4 -b 0.0.0.0:5000 "app:create_app()" --timeout 900