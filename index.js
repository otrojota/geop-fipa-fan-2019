global.confPath = __dirname + "/config.json";
global.resDir = __dirname + "/resources";
const config = require("./lib/Config").getConfig();
const ProveedorCapasBCN = require("./lib/ProveedorCapasFIPAFAN2019");

const proveedorCapas = new ProveedorCapasBCN({
    puertoHTTP:config.webServer.http.port,
    directorioWeb:__dirname + "/www",
    directorioPublicacion:null
});
proveedorCapas.start();