global.confPath = __dirname + "/config.json";
global.resDir = __dirname + "/resources";
const config = require("./lib/Config").getConfig();
const ProveedorCapasFIPA = require("./lib/ProveedorCapasFIPAFAN2019");

const proveedorCapas = new ProveedorCapasFIPA({
    puertoHTTP:config.webServer.http.port,
    directorioWeb:__dirname + "/www",
    directorioPublicacion:null
});

if (process.argv.length == 4) {
    if (process.argv[2] == "-cmd") {
        let a = process.argv[3];
        proveedorCapas[a]();
    }
} else {
    proveedorCapas.start();
}