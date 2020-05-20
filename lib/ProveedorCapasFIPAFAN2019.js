const {ProveedorCapas, CapaObjetosConDatos} = require("geop-base-proveedor-capas");
const minz = require("./MinZClient");
const moment = require("moment-timezone");
const config = require("./Config").getConfig();
const turf = require("@turf/turf");

const infoMacrozonas = {
    "8":{nombre:"Tortel", provincia:"101"},
    "7":{nombre:"Chiloé Norte", provincia:"102"},
    "4":{nombre:"Chiloé Centro", provincia:"102"},
    "6":{nombre:"Chiloé Sur", provincia:"102"},
    "1":{nombre:"Aysén Noroeste", provincia:"112"},
    "3":{nombre:"Aysén Noreste", provincia:"112"},
    "2":{nombre:"Aysén Sur", provincia:"112"},
    "5":{nombre:"Palena", provincia:"104"},
    "10":{nombre:"Magallanes Norte", provincia:"124"},
    "9":{nombre:"Magallanes Sur", provincia:"124"}
}


class ProveedorCapasFIPAFAN2019 extends ProveedorCapas {
    constructor(opciones) {
        super("fipafan2019", opciones);
        this.addOrigen("subpesca", "Subsecretaría de Pesca y Acuicultura", "http://www.subpesca.cl/", "./img/subpesca.jpg");        
        this.addOrigen("fipa", "Fondo de Investigación Pesquera y de Acuicultura", "http://www.subpesca.cl/fipa/613/w3-channel.html", "./img/fipa.png");

        let capaMacrozonasSanitarias = new CapaObjetosConDatos("fipafan2019", "MacrozonasSanitarias", "Macrozonas Sanitarias", "subpesca", {
            temporal:false,
            datosDinamicos:false,
            menuEstaciones:false,
            dimensionMinZ:"fipafan2019.macrozona",
            geoJSON:true,
            estilos:(function(f) {
                return {stroke:"#ff0000", strokeWidth:1, fill:"#ff0000", opacity:0.4}
            }).toString()
        }, [], "img/macrozonas.svg");
        this.addCapa(capaMacrozonasSanitarias);   
        
        let capaEstaciones = new CapaObjetosConDatos("fipafan2019", "EstacionesMonitoreo", "FIPA - Estaciones de Monitoreo", "fipa", {
            temporal:false,
            datosDinamicos:false,
            menuEstaciones:true,
            dimensionMinZ:"fipafan2019.estacion",
            geoJSON:true,
            iconoEnMapa:"img/estacion-fipa.svg"
        }, [], "img/estaciones.svg");
        this.addCapa(capaEstaciones);   

        // cache
        this.macrozonas = this.getFeaturesMacrozonas();
        this.estaciones = this.getFeaturesEstaciones();
    }

    async resuelveConsulta(formato, args) {
        try {
            if (formato == "geoJSON") {
                return await this.generaGeoJSON(args);
            } else throw "Formato " + formato + " no soportado";
        } catch(error) {
            throw error;
        }
    }

    async generaGeoJSON(args) {
        try {           
            if (args.codigoVariable == "MacrozonasSanitarias") {
                return this.macrozonas;
            } else if (args.codigoVariable == "EstacionesMonitoreo") {
                return this.estaciones;            
            } else throw "Código de Capa '" + args.codigoVariable + "' no manejado";            
        } catch(error) {
            throw error;
        }
    }

    getFeaturesMacrozonas() {
        // ogr2ogr -t_srs EPSG:4326 macrozonas-sanitarias.geojson Macrozonas_Sanitarias.shp
        let path = global.resDir + "/macrozonas-sanitarias-fipa.geojson";
        let features = JSON.parse(require("fs").readFileSync(path));
        features.name = "SUB Pesca - Macrozonas Sanitarias";
        
        features.features.forEach(f => {
            let id = f.properties.REP_SUBPES;
            let info = infoMacrozonas[id];
            let nombre = info?info.nombre:f.properties.REP_SUBPES;
            f.properties.id = id;
            f.properties.nombre = nombre;
            f.properties._titulo = "Macrozona Sanitaria: " + nombre;
            f.properties._codigoDimension = this.pad(f.properties.REP_SUBPES, 2);
            f.properties.codigoRegion = this.pad(f.properties.REP_SUB_10, 2);            
            // Eliminar propiedades sin valor
            Object.keys(f.properties).forEach(k => {
                if (!f.properties[k]) delete f.properties[k];
            });
        });
        console.log("[Geoportal - FIPA-FAN-2019] Leidas " + features.features.length + " macrozoonas sanitarias a cache");
        return features;
    }
    getFeaturesEstaciones() {
        // docker run -it --mount type=bind,source=/Users/jota/Downloads/estaciones-monitoreo-marea-roja/zipfolder,target=/work osgeo/gdal bash
        // ogr2ogr -makevalid -t_srs EPSG:4326 estaciones.geojson Monitoreo_Marea_Roja.shp
        let path = global.resDir + "/estaciones-fipa.geojson";
        let features = JSON.parse(require("fs").readFileSync(path));
        features.name = "FIPA-FAN-2019 - Estaciones de Monitoreo";
        features.features.forEach(f => {
            f.properties.id = f.properties.REP_SUBPES;
            f.properties.nombre = f.properties.REP_SUBP_8;
            f.properties._titulo = "Estación: " + f.properties.REP_SUBP_8;
            f.properties.nombreSector = f.properties.REP_SUBP_9;
            f.properties.nombreInstitucion = f.properties.REP_SUB_10;
            f.properties._codigoDimension = f.properties.REP_SUBPES;
            // Eliminar propiedades sin valor
            Object.keys(f.properties).forEach(k => {
                if (!f.properties[k]) delete f.properties[k];
            });
        });
        console.log("[Geoportal - FIPA-FAN-2019] Leidas " + features.features.length + " estaciones a cache");
        return features;
    }

    // MinZ
    async comandoGET(cmd, req, res) {
        try {
            switch(cmd) {
                case "initMinZ":
                    await this.initMinZ(req, res);
                    break;
                case "preparaArchivos":
                    await this.preparaArchivos(req, res);
                    break;
                default: throw "Comando '" + cmd + "' no implementado";
            }
        } catch(error) {
            console.error(error);
            if (typeof error == "string") {
                res.send(error).status(401).end();
            } else {
                res.send("Error Interno").status(500).end();
            }
        }
    }

    pad(st, n) {
        let r = "" + st;
        while (r.length < n) r = "0" + r;
        return r;
    }

    request(method, url, data) {
        const http = (url.toLowerCase().startsWith('https://') ? require("https"):require("http")) ;
        //process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
        let postData, options = {method:method, headers:{}};
        if (this._token) options.headers.Authorization = "Bearer " + this._token;
        if (method == "POST") {
            postData = JSON.stringify(data || {});
            options.headers['Content-Type'] = 'application/json';
            options.headers['Content-Length'] = Buffer.byteLength(postData);            
        }
        return new Promise((resolve, reject) => {
            let req = http.request(url, options, res => {
                let chunks = [];
                res.on("data", chunk => chunks.push(chunk));
                res.on("end", _ => {
                    let body = Buffer.concat(chunks).toString();
                    if (res.statusCode != 200) reject(body);
                    else resolve(JSON.parse(body));
                });
            });
            req.on("error", err => reject(err));
            if (method == "POST") req.write(postData);
            req.end();
        }); 
    }

    async initMinZ(req, res) {
        try {
            // Crear dimensiones            
            await minz.addOrSaveDimension({code:"fipafan2019.macrozona", name:"Macrozona FIPA-FAN-2019", classifiers:[{fieldName:"provincia", name:"Provincia", dimensionCode:"bcn.provincia", defaultValue:"000"}]});
            for (let i=0; i<this.macrozonas.features.length; i++) {
                let macrozona = this.macrozonas.features[i];
                let id = macrozona.properties.id;
                let codigoMacrozona = this.pad(id, 2);
                let codigoProvincia = macrozona.properties.codigoProvincia;
                await minz.addOrUpdateRow("fipafan2019.macrozona", {code:codigoMacrozona, name:macrozona.properties.nombre, provincia:codigoProvincia});
            }
            await minz.addOrUpdateRow("fipafan2019.macrozona", {code:"00", name:"Sin Macrozona Asociada", provincia:"000"});

            // Crear instituciones
            await minz.addOrSaveDimension({code:"fipafan2019.institucion", name:"Institución FIPA-FAN-2019", classifiers:[]});
            let mapaI = {};
            for (let i=0; i < this.estaciones.features.length; i++) {
                let e = this.estaciones.features[i];
                let c = e.properties.nombreInstitucion;
                if (!c || c.trim() == "xx") mapaI["00"] = "No Indicada";
                else mapaI[c.trim().toLowerCase()] = c;
            }
            for (let i=0; i < Object.keys(mapaI).length; i++) {
                let codigo = Object.keys(mapaI)[i];
                let nombre = mapaI[codigo];
                await minz.addOrUpdateRow("fipafan2019.institucion", {code:codigo, name:nombre});
            }

            // Crear estaciones
            await minz.addOrSaveDimension({code:"fipafan2019.estacion", name:"Estaciones FIPA-FAN-2019", classifiers:[
                {fieldName:"macrozona", name:"Macrozona", dimensionCode:"fipafan2019.macrozona", defaultValue:"00"}, 
                {fieldName:"comuna", name:"Comuna", dimensionCode:"bcn.comuna", defaultValue:"00000"},
                {fieldName:"institucion", name:"Institución", dimensionCode:"fipafan2019.institucion", defaultValue:"S/I"}
            ]});

            for (let i=0; i < this.estaciones.features.length; i++) {
                let e = this.estaciones.features[i];
                let codigoEstacion = e.properties.id;
                let codigoMacrozona = e.properties.codigoMacrozona;
                let codigoComuna = e.properties.codigoComuna;
                let codigoInstitucion = e.properties.nombreInstitucion;
                if (!codigoInstitucion || codigoInstitucion.trim() == "xx") codigoInstitucion = "00";
                else codigoInstitucion = codigoInstitucion.trim().toLowerCase();
                await minz.addOrUpdateRow("fipafan2019.estacion", {code:codigoEstacion, name:e.properties.nombre, macrozona:codigoMacrozona, comuna:codigoComuna, institucion:codigoInstitucion});
            }

            // Crear Variables
            /*
            if (!await minz.existeVariable("ine.cosecha_acuicola")) {
                await minz.addVariable({
                    code:"ine.cosecha_acuicola",
                    name:"Cosecha Acuícola",
                    temporality:"1M",
                    classifiers:[{
                        fieldName:"region", name:"Región", dimensionCode:"bcn.region", defaultValue:"00"
                    }],
                    options:{
                        unit:"ton",
                        decimals:2
                    }
                })
            } 
            if (!await minz.existeVariable("ine.pesca_artesanal")) {
                await minz.addVariable({
                    code:"ine.pesca_artesanal",
                    name:"Pesca Artesanal",
                    temporality:"1M",
                    classifiers:[{
                        fieldName:"region", name:"Región", dimensionCode:"bcn.region", defaultValue:"00"
                    }],
                    options:{
                        unit:"ton",
                        decimals:2
                    }
                })
            }            
            */
            res.status(200).send("Ok").end();
        } catch(error) {
            throw error;
        }
    }

    async preparaArchivos(req, res) {
        try {
            const fs = require("fs");
            // ogr2ogr -t_srs EPSG:4326 macrozonas-sanitarias.geojson Macrozonas_Sanitarias.shp
            let path = global.resDir + "/macrozonas-sanitarias.geojson";
            let features = JSON.parse(fs.readFileSync(path));
            features.name = "SUB Pesca - Macrozonas Sanitarias";            
            features.features.forEach(f => {
                let id = f.properties.REP_SUBPES;
                let info = infoMacrozonas[id];
                let nombre = info?info.nombre:f.properties.REP_SUBPES;
                f.properties.id = id;
                f.properties.nombre = nombre;
                f.properties._titulo = "Macrozona Sanitaria: " + nombre;
                f.properties._codigoDimension = this.pad(f.properties.REP_SUBPES, 2);
                f.properties.codigoRegion = this.pad(f.properties.REP_SUB_10, 2);
                f.properties.codigoProvincia = info.provincia;
                
                // Eliminar propiedades sin valor
                Object.keys(f.properties).forEach(k => {
                    if (!f.properties[k]) delete f.properties[k];
                });
            });
            fs.writeFileSync(global.resDir + "/macrozonas-sanitarias-fipa.geojson", JSON.stringify(features));
            let macrozonas = features;
            
            // Obtener comunas para asociar estaciones
            let url = config.bcnUrl + "/consulta";
            let args = {
                formato:"geoJSON", 
                args:{codigoVariable:"Comunas"}
            }
            console.log("buscando comunas");
            let comunas = (await this.request("POST", url, args)).features;
            console.log("comunas", comunas.length);

            path = global.resDir + "/estaciones.geojson";
            features = JSON.parse(require("fs").readFileSync(path));
            features.name = "FIPA-FAN-2019 - Estaciones de Monitoreo";
            features.features.forEach(f => {
                f.properties.id = f.properties.REP_SUBPES;
                f.properties.nombre = f.properties.REP_SUBP_8;
                f.properties._titulo = "Estación: " + f.properties.REP_SUBP_8;
                f.properties.nombreSector = f.properties.REP_SUBP_9;
                f.properties.nombreInstitucion = f.properties.REP_SUB_10;
                f.properties._codigoDimension = f.properties.REP_SUBPES;

                // Buscar macrozona
                let lng = f.geometry.coordinates[0];
                let lat = f.geometry.coordinates[1];
                let p = turf.point([lng, lat]);
                let codigoMacrozona = "00";
                for (let j=0; j < macrozonas.features.length; j++) {
                    let macrozona = macrozonas.features[j];
                    let poly = turf.polygon(macrozona.geometry.coordinates);
                    if (turf.booleanPointInPolygon(p, poly)) {
                        codigoMacrozona = macrozona.properties._codigoDimension;
                        break;
                    }
                }
                f.properties.codigoMacrozona = codigoMacrozona;

                // Buscar Comuna
                let comunaMasCercana, distanciaMasCercana, nombreComunaMasCercana;
                for (let j=0; j<comunas.length; j++) {
                    let comuna = comunas[j];
                    // regiones con estaciones: 10, 11, 12
                    if (["10", "11", "12"].indexOf(comuna.properties.codigoRegion) >= 0) {                        
                        try {
                            let poly = comuna.geometry.type == "Polygon"?turf.multiPolygon([comuna.geometry.coordinates]):turf.multiPolygon(comuna.geometry.coordinates);
                            if (turf.booleanPointInPolygon(p, poly)) {
                                comunaMasCercana = comuna.properties._codigoDimension;
                                console.log(f.properties._codigoDimension + " => " + comuna.properties.nombre);
                                break;
                            }
                        } catch(error) {
                            console.warn(comuna.properties.nombre + ":" + error.toString());
                        }
                        
                    }
                }
                if (!comunaMasCercana) {
                    for (let j=0; j<comunas.length; j++) {
                        let comuna = comunas[j];
                        if (["10", "11", "12"].indexOf(comuna.properties.codigoRegion) >= 0) {
                            let poly = comuna.geometry.type == "Polygon"?turf.multiPolygon([comuna.geometry.coordinates]):turf.multiPolygon(comuna.geometry.coordinates);
                            try {
                                let vertices = turf.explode(poly)
                                let closestVertex = turf.nearest(p, vertices)
                                let distance = turf.distance(p, closestVertex);
                                //console.log(comuna.geometry.type, distance);
                                if (!comunaMasCercana || distance < distanciaMasCercana) {
                                    comunaMasCercana = comuna.properties._codigoDimension;
                                    distanciaMasCercana = distance;
                                    nombreComunaMasCercana = comuna.properties.nombre;
                                }
                            } catch(error) {
                                console.warn(comuna.geometry.type, error);
                            }
                        }
                    }
                    console.log(f.properties._codigoDimension + " => " + nombreComunaMasCercana);
                }
                f.properties.codigoComuna = comunaMasCercana;

                // Eliminar propiedades sin valor
                Object.keys(f.properties).forEach(k => {
                    if (!f.properties[k]) delete f.properties[k];
                });
            });
            fs.writeFileSync(global.resDir + "/estaciones-fipa.geojson", JSON.stringify(features));

            // Crear Variables
            /*
            if (!await minz.existeVariable("ine.cosecha_acuicola")) {
                await minz.addVariable({
                    code:"ine.cosecha_acuicola",
                    name:"Cosecha Acuícola",
                    temporality:"1M",
                    classifiers:[{
                        fieldName:"region", name:"Región", dimensionCode:"bcn.region", defaultValue:"00"
                    }],
                    options:{
                        unit:"ton",
                        decimals:2
                    }
                })
            } 
            if (!await minz.existeVariable("ine.pesca_artesanal")) {
                await minz.addVariable({
                    code:"ine.pesca_artesanal",
                    name:"Pesca Artesanal",
                    temporality:"1M",
                    classifiers:[{
                        fieldName:"region", name:"Región", dimensionCode:"bcn.region", defaultValue:"00"
                    }],
                    options:{
                        unit:"ton",
                        decimals:2
                    }
                })
            }            
            */
            res.status(200).send("Ok").end();
        } catch(error) {
            throw error;
        }
    }

    async importaINE(req, res) {
        try {
            let regiones = await minz.findRows("bcn.region");

            console.log("\n\nCosecha Acuicola por Region");
            console.log("  => Eliminando periodo anterior");
            let startTime = moment.tz("2010-01-01", config.timeZone);
            let endTime = moment.tz("2100-01-01", config.timeZone);
            await minz.deletePeriod("ine.cosecha_acuicola", startTime.valueOf(), endTime.valueOf(), true);
            await minz.deletePeriod("ine.pesca_artesanal", startTime.valueOf(), endTime.valueOf(), true);
            for (let i=0; i<regiones.length; i++) {
                let region = regiones[i];
                if (region.code != "00") {
                    let cosechaAcuicola = await ine.getCosechaAcuicola(region.code);
                    console.log("Cosecha Acuicola:", region.name, cosechaAcuicola);
                    for (let j=0; j<cosechaAcuicola.length; j++) {
                        let row = cosechaAcuicola[j];
                        let time = moment.tz(row.ano + "-" + this.pad(row.mes,2) + "-01", config.timeZone);
                        await minz.postData("ine.cosecha_acuicola", time.valueOf(), row.valor, {region:row.codigoRegion});
                    }
                    let pescaArtesanal = await ine.getPescaArtesanal(region.code);
                    console.log("Pesca Artesanal:", region.name, pescaArtesanal);
                    for (let j=0; j<pescaArtesanal.length; j++) {
                        let row = pescaArtesanal[j];
                        let time = moment.tz(row.ano + "-" + this.pad(row.mes,2) + "-01", config.timeZone);
                        await minz.postData("ine.pesca_artesanal", time.valueOf(), row.valor, {region:row.codigoRegion});
                    }
                }
            }
            res.status(200).send("Ok").end();
        } catch(error) {
            console.error(error);
            throw error;
        }
    }
}

module.exports = ProveedorCapasFIPAFAN2019;