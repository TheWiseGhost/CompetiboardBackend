# Use the official Python image from the Docker Hub
FROM python:3.9-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Set the working directory inside the container
WORKDIR /app
# Copy the requirements file into the container at /app
COPY requirements.txt /app/
# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
# Copy the entire application into the container at /app
COPY . /app/
# Ensure the database is created, migrations are applied, and static files are collected
RUN python manage.py migrate
# Expose the port that the app will run on (Render uses port 10000 by default)
EXPOSE 10000
# Run the Django application using Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:10000", "competiboard.wsgi:application"]