FROM python:2.7
RUN mkdir /code /sandbox
WORKDIR /code
RUN apt-get update && apt-get upgrade -y && apt-get -y install rsync
RUN pip install dxpy==0.287.0
# add the current working directory to the code folder
ADD . /code
#RUN chmod u+x ./resources/dx-toolkit/environment
#EXPOSE 22
#CMD [ "python","/code/archer_script.py" ]
#ENTRYPOINT ["/code/ampliconfilter/ampliconFilter.py"]
CMD ["bash"]
