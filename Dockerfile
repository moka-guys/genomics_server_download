FROM python:2.7
RUN apt-get update && apt-get upgrade -y && apt-get -y install rsync sshpass
RUN pip install dxpy==0.287.0
RUN mkdir -p /mokaguys/apps/code /mokaguys/logfiles /mokaguys/dx_downloads
# add 
ADD ./ /mokaguys/apps/code
WORKDIR /mokaguys
CMD ["python","/mokaguys/apps/code/archer_script.py"]
