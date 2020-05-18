# docker build -f fipa-fan-2019.dockerfile -t otrojota/geoportal:fipa-fan-2019-0.11 .
# docker push otrojota/geoportal:fipa-fan-2019-0.11
#
FROM otrojota/geoportal:gdal-nodejs
WORKDIR /opt/geoportal/geop-servimet
COPY . .
RUN apt-get update
RUN apt-get -y install git
RUN npm install 
EXPOSE 8190
CMD node index.js