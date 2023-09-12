# CSCI-5253-Datacenter-Scale-Computing

Lab 1 is a basic Docker Pipeline. The below objectives were achieved using Lab 01:

1. A Python script was created that:

    accepts 2 command line arguments, 
    reads a csv from the first argument
    does something to the data
    saves the results to the csv defined by the second argument

2. A Dockerfile was created that:

    pulls from Python Docker image
    copies the script above into the container
    runs the script above when the container starts.
