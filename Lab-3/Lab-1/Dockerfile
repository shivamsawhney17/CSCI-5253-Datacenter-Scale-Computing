
# Using Python runtime as a parent image
FROM python:3.11

# Installing required packages 
RUN pip install pandas
RUN pip install numpy
RUN pip install argparse

# Setting the working directory to /app
WORKDIR /app

# Copying the current directory contents into the container at /app
COPY . /app/

# Running pipeline.py when container launches
CMD ["python", "pipeline.py"]