FROM python:3.8-slim-buster

# Tools to access the NVidia APT repository
RUN apt-get update && apt-get install --no-install-recommends -y \
    gnupg \
    ca-certificates

# Install tools for NVMe health metrics
RUN apt-get update && apt-get install --no-install-recommends -y \
    smartmontools

# Install NVidia GPU access tools
COPY build/nvidia-l4t-apt-source.list /etc/apt/sources.list.d/
COPY build/jetson-ota-public.asc /etc/apt/trusted.gpg.d/

# Disable the hardware compatibility check & install Nvidia tools
RUN mkdir -p /opt/nvidia/l4t-packages/ && \
    touch /opt/nvidia/l4t-packages/.nv-l4t-disable-boot-fw-update-in-preinstall && \
    apt-get update && apt-get install --no-install-recommends -y \
        nvidia-l4t-tools

# Add necessary python libraries
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY main.py .
ENTRYPOINT [ "python", "main.py" ]
