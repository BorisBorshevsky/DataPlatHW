# Find the IP address
try:
    # Get the container using Docker for python:
    container = docker.from_env().containers.get('cass_cluster')

    # Get the container's IP address
    cluster_ip_address = container.attrs['NetworkSettings']['IPAddress']
    print('IP:', cluster_ip_address)
except:
    print("Error in Docker setup!")