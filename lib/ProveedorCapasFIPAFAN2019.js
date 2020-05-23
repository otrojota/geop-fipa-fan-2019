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
        this.addOrigen("fipafan2019", "FIPA - FAN 2019", "http://www.subpesca.cl/fipa/613/w3-channel.html", "./img/fipa.png");

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
        
        let capaEstaciones = new CapaObjetosConDatos("fipafan2019", "EstacionesMonitoreo", "FIPA - Estaciones de Monitoreo", "fipafan2019", {
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
                case "importaOxigeno":
                    await this.importaOxigeno(req, res);
                    break;
                case "importaClorofila":
                    await this.importaClorofila(req, res);
                case "importaSalinidad":
                    await this.importaSalinidad(req, res);
                    break;
                case "importaViento":
                    await this.importaViento(req, res);
                    break;
                case "importaTemperaturaAmbiental":
                    await this.importaTemperaturaAmbiental(req, res);
                    break;
                case "importaToxinaVDM":
                    await this.importaToxinaVDM(req, res);
                    break;
                case "importaAbundanciaRelativa":
                    await this.importaAbundanciaRelativa(req, res);
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

            // Profundidades
            let prof = [0,5,10,20,30,40,50,75,100];
            await minz.addOrSaveDimension({code:"fipafan2019.profundidad", name:"Profundidad FIPA-FAN-2019", classifiers:[]});
            for (let i=0; i<prof.length; i++) {
                let p = prof[i];
                await minz.addOrUpdateRow("fipafan2019.profundidad", {code:"" + p, name:p + " [m]"});
            }

            // Programas
            let prog = [["0", "Programa No Indicado"], ["1", "Programa 1"], ["2", "Programa 2"], ["3", "Programa 3"]];
            await minz.addOrSaveDimension({code:"fipafan2019.programa", name:"Programa FIPA-FAN-2019", classifiers:[]});
            for (let i=0; i<prog.length; i++) {
                let p = prog[i];
                await minz.addOrUpdateRow("fipafan2019.programa", {code:p[0], name:p[1]});
            }

            // Dirección Viento
            let dirs = ["CALMA", "S", "N", "E", "O", "SE", "SO", "NE", "NO"];
            await minz.addOrSaveDimension({code:"fipafan2019.direccion_viento", name:"Dirección Viento FIPA-FAN-2019", classifiers:[]});
            for (let i=0; i<dirs.length; i++) {
                let d = dirs[i];
                await minz.addOrUpdateRow("fipafan2019.direccion_viento", {code:d, name:d});
            }

            // Recurso VDM
            let recursos = [["ALMEJA", "Almeja"], ["CHOLGA", "Cholga"], ["CHORITO", "CHORITO"], ["CHORITO MALTÓN", "Chorito Maltón"], ["SM", "Sin Muestra"]];
            await minz.addOrSaveDimension({code:"fipafan2019.recurso_vdm", name:"Recurso VDM FIPA-FAN-2019", classifiers:[]});
            for (let i=0; i<recursos.length; i++) {
                let r = recursos[i];
                await minz.addOrUpdateRow("fipafan2019.recurso_vdm", {code:r[0], name:r[1]});
            }

            // Especie Microalga
            let especies = ["ARALEXCATE", "ARALEXOSTE", "ARALEXTAMA", "ARDINOACUM", "ARDINOACUT", "ARPROTRETI", "ARPSEUAUST", "ARPSEUPSEU"];
            await minz.addOrSaveDimension({code:"fipafan2019.especie_microalga", name:"Especia Microalga FIPA-FAN-2019", classifiers:[]});
            for (let i=0; i<especies.length; i++) {
                let e = especies[i];
                await minz.addOrUpdateRow("fipafan2019.especie_microalga", {code:e, name:e});
            }

            // Crear Variables            
            await minz.addOrSaveVariable({
                code:"fipafan2019.oxigeno",
                name:"Oxígeno Disuelto",
                temporality:"1d",
                classifiers:[{
                    fieldName:"estacion", name:"Estación", dimensionCode:"fipafan2019.estacion", defaultValue:"00"
                }, {
                    fieldName:"profundidad", name:"Profundidad", dimensionCode:"fipafan2019.profundidad", defaultValue:"0"
                }, {
                    fieldName:"programa", name:"Programa", dimensionCode:"fipafan2019.programa", defaultValue:"0"
                }],
                options:{
                    unit:"mg/ml",
                    decimals:2,
                    defQuery:{
                        accum:"avg", temporality:"1M", filters:[
                            {path:"profundidad", value:"10"}
                        ]
                    }
                }
            });
            await minz.addOrSaveVariable({
                code:"fipafan2019.clorofila",
                name:"Clorofila",
                temporality:"1d",
                classifiers:[{
                    fieldName:"estacion", name:"Estación", dimensionCode:"fipafan2019.estacion", defaultValue:"00"
                }, {
                    fieldName:"profundidad", name:"Profundidad", dimensionCode:"fipafan2019.profundidad", defaultValue:"0"
                }, {
                    fieldName:"programa", name:"Programa", dimensionCode:"fipafan2019.programa", defaultValue:"0"
                }],
                options:{
                    unit:"??",
                    decimals:2,
                    defQuery:{
                        accum:"avg", temporality:"1M", filters:[
                            {path:"profundidad", value:"10"}
                        ]
                    }
                }
            })
            await minz.addOrSaveVariable({
                code:"fipafan2019.salinidad",
                name:"Salinidad",
                temporality:"1d",
                classifiers:[{
                    fieldName:"estacion", name:"Estación", dimensionCode:"fipafan2019.estacion", defaultValue:"00"
                }, {
                    fieldName:"profundidad", name:"Profundidad", dimensionCode:"fipafan2019.profundidad", defaultValue:"0"
                }, {
                    fieldName:"programa", name:"Programa", dimensionCode:"fipafan2019.programa", defaultValue:"0"
                }],
                options:{
                    unit:"??",
                    decimals:2,
                    defQuery:{
                        accum:"avg", temporality:"1M", filters:[
                            {path:"profundidad", value:"0"}
                        ]
                    }
                }
            })
            await minz.addOrSaveVariable({
                code:"fipafan2019.velocidad_viento",
                name:"Velocidad del Viento",
                temporality:"1d",
                classifiers:[{
                    fieldName:"estacion", name:"Estación", dimensionCode:"fipafan2019.estacion", defaultValue:"00"
                }, {
                    fieldName:"direccion", name:"Dirección", dimensionCode:"fipafan2019.direccion_viento", defaultValue:"CALMA"
                }, {
                    fieldName:"programa", name:"Programa", dimensionCode:"fipafan2019.programa", defaultValue:"0"
                }],
                options:{
                    unit:"m/s",
                    decimals:2,
                    defQuery:{
                        accum:"avg", temporality:"1M"
                    }
                }
            })
            await minz.addOrSaveVariable({
                code:"fipafan2019.temperatura_ambiental",
                name:"Temperatura Ambiental",
                temporality:"1d",
                classifiers:[{
                    fieldName:"estacion", name:"Estación", dimensionCode:"fipafan2019.estacion", defaultValue:"00"
                }, {
                    fieldName:"programa", name:"Programa", dimensionCode:"fipafan2019.programa", defaultValue:"0"
                }],
                options:{
                    unit:"ºC",
                    decimals:2,
                    defQuery:{
                        accum:"avg", temporality:"1M"
                    }
                }
            })
            await minz.addOrSaveVariable({
                code:"fipafan2019.toxina_vdm",
                name:"Toxina VDM",
                temporality:"1d",
                classifiers:[{
                    fieldName:"estacion", name:"Estación", dimensionCode:"fipafan2019.estacion", defaultValue:"00"
                }, {
                    fieldName:"recurso", name:"Recurso", dimensionCode:"fipafan2019.recurso_vdm", defaultValue:"SM"
                }, {
                    fieldName:"programa", name:"Programa", dimensionCode:"fipafan2019.programa", defaultValue:"0"
                }],
                options:{
                    unit:"VDM",
                    decimals:2,
                    defQuery:{
                        accum:"avg", temporality:"1M"
                    }
                }
            })
            await minz.addOrSaveVariable({
                code:"fipafan2019.abundancia_relativa",
                name:"Abundancia Relativa",
                temporality:"1d",
                classifiers:[{
                    fieldName:"estacion", name:"Estación", dimensionCode:"fipafan2019.estacion", defaultValue:"00"
                }, {
                    fieldName:"especie", name:"Especie", dimensionCode:"fipafan2019.especie_microalga", defaultValue:"00"
                }, {
                    fieldName:"programa", name:"Programa", dimensionCode:"fipafan2019.programa", defaultValue:"0"
                }],
                options:{
                    unit:"rel",
                    decimals:2,
                    defQuery:{
                        accum:"avg", temporality:"1M"
                    }
                }
            })
            
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
            res.status(200).send("Ok").end();
        } catch(error) {
            throw error;
        }
    }

    async importaOxigeno(req, res) {
        const fs = require("fs");
        const readline = require("readline");
        try {
            console.log("\n\nImportando Oxigeno");
            console.log("  => Eliminando periodo anterior");
            let startTime = moment.tz("2006-01-01", config.timeZone);
            let endTime = moment.tz("2015-12-31", config.timeZone);
            await minz.deletePeriod("fipafan2019.oxigeno", startTime.valueOf(), endTime.valueOf(), true);
            let path = global.resDir + "/OX_2006_2015.csv";
            console.log("abriendo archivo");
            let lineas = await fs.readFileSync(path).toString().split("\n");
            console.log("lineas:" + lineas.length);
            const prof = [0,5,10,20,30,40,50,75,100];
            for (let k=0; k < lineas.length; k++) {
                let line = lineas[k];
                if (!k) continue;
                let fields = line.split(",");
                if (fields.length != 16) {
                    console.warn(`Linea ${k+1} contiene ${fields.length} campos`);
                    continue;
                }
                if (!(k % 100)) console.log("linea:" + k);
                let codigoEstacion = fields[1];
                let programa = fields[2];
                let fMuestreo = fields[4];
                let time = moment.tz(fMuestreo, "YYYY/MM/DD", config.timeZone);
                if (!time._isValid) {
                    console.warn("Linea " + (k+1) + ": Fecha inválida: " + fMuestreo);
                    continue;
                }
                for (let i=0; i<prof.length; i++) {
                    let profundidad = "" + prof[i];
                    let v = fields[7 + i];
                    if (v) {
                        v = parseFloat(v);
                        if (!isNaN(v)) {
                            await minz.postData("fipafan2019.oxigeno", time.valueOf(), v, {
                                estacion:codigoEstacion, profundidad:profundidad, programa:programa
                            });
                        }
                    }
                }
            }            
            res.status(200).send("Ok").end();
        } catch(error) {
            console.error(error);
            throw error;
        }
    }

    async importaClorofila(req, res) {
        const fs = require("fs");
        try {
            console.log("\n\nImportando Clorofila");
            console.log("  => Eliminando periodo anterior");
            let startTime = moment.tz("2006-01-01", config.timeZone);
            let endTime = moment.tz("2015-12-31", config.timeZone);
            await minz.deletePeriod("fipafan2019.clorofila", startTime.valueOf(), endTime.valueOf(), true);
            let path = global.resDir + "/CLOROFILA_2006_2015.csv";
            console.log("abriendo archivo");
            let lineas = await fs.readFileSync(path).toString().split("\n");
            console.log("lineas:" + lineas.length);
            const prof = [0,5,10,20,30,40,50,75,100];
            for (let k=0; k < lineas.length; k++) {
                let line = lineas[k];
                if (!k) continue;
                let fields = line.split(",");
                if (fields.length < 7) {
                    console.warn(`Linea ${k+1} contiene ${fields.length} campos`);
                    continue;
                }
                if (!(k % 100)) console.log("linea:" + k);
                let codigoEstacion = fields[1];
                let programa = fields[2];
                let fMuestreo = fields[4];
                let time = moment.tz(fMuestreo, "YYYY/MM/DD", config.timeZone);
                if (!time._isValid) {
                    console.warn("Linea " + (k+1) + ": Fecha inválida: " + fMuestreo);
                    continue;
                }
                for (let i=0; i<prof.length; i++) {
                    let profundidad = "" + prof[i];
                    let v = fields[7 + i];
                    if (v) {
                        v = parseFloat(v);
                        if (!isNaN(v)) {
                            await minz.postData("fipafan2019.clorofila", time.valueOf(), v, {
                                estacion:codigoEstacion, profundidad:profundidad, programa:programa
                            });
                        }
                    }
                }
            }            
            res.status(200).send("Ok").end();
        } catch(error) {
            console.error(error);
            throw error;
        }
    }

    async importaSalinidad(req, res) {
        const fs = require("fs");
        try {
            console.log("\n\nImportando Salinidad");
            console.log("  => Eliminando periodo anterior");
            let startTime = moment.tz("2006-01-01", config.timeZone);
            let endTime = moment.tz("2015-12-31", config.timeZone);
            await minz.deletePeriod("fipafan2019.salinidad", startTime.valueOf(), endTime.valueOf(), true);
            let path = global.resDir + "/SALINIDAD_2006_2015.csv";
            console.log("abriendo archivo");
            let lineas = await fs.readFileSync(path).toString().split("\n");
            console.log("lineas:" + lineas.length);
            const prof = [0,5,10,20,30,40,50,75,100];
            for (let k=0; k < lineas.length; k++) {
                let line = lineas[k];
                if (!k) continue;
                let fields = line.split(",");
                if (fields.length < 7) {
                    console.warn(`Linea ${k+1} contiene ${fields.length} campos`);
                    continue;
                }
                if (!(k % 100)) console.log("linea:" + k);
                let codigoEstacion = fields[1];
                let programa = fields[2];
                let fMuestreo = fields[4];
                let time = moment.tz(fMuestreo, "YYYY/MM/DD", config.timeZone);
                if (!time._isValid) {
                    console.warn("Linea " + (k+1) + ": Fecha inválida: " + fMuestreo);
                    continue;
                }
                for (let i=0; i<prof.length; i++) {
                    let profundidad = "" + prof[i];
                    let v = fields[7 + i];
                    if (v) {
                        v = parseFloat(v);
                        if (!isNaN(v)) {
                            await minz.postData("fipafan2019.salinidad", time.valueOf(), v, {
                                estacion:codigoEstacion, profundidad:profundidad, programa:programa
                            });
                        }
                    }
                }
            }            
            res.status(200).send("Ok").end();
        } catch(error) {
            console.error(error);
            throw error;
        }
    }

    async importaViento(req, res) {
        const fs = require("fs");
        try {
            console.log("\n\nImportando Viento");
            console.log("  => Eliminando periodo anterior");
            let startTime = moment.tz("2006-01-01", config.timeZone);
            let endTime = moment.tz("2015-12-31", config.timeZone);
            await minz.deletePeriod("fipafan2019.velocidad_viento", startTime.valueOf(), endTime.valueOf(), true);
            let path = global.resDir + "/VIENTO_2006_2014-15.csv";
            console.log("abriendo archivo");
            let lineas = await fs.readFileSync(path).toString().split("\n");
            console.log("lineas:" + lineas.length);
            for (let k=0; k < lineas.length; k++) {
                let line = lineas[k];
                if (!k) continue;
                let fields = line.split(",");
                if (fields.length < 9) {
                    console.warn(`Linea ${k+1} contiene ${fields.length} campos`);
                    continue;
                }
                if (!(k % 100)) console.log("linea:" + k);
                let codigoEstacion = fields[1];
                let programa = fields[2];
                let fMuestreo = fields[4];
                let time = moment.tz(fMuestreo, "YYYY/MM/DD", config.timeZone);
                if (!time._isValid) {
                    console.warn("Linea " + (k+1) + ": Fecha inválida: " + fMuestreo);
                    continue;
                }
                let direccion = fields[7];
                if (!direccion) {
                    console.warn("Linea " + (k+1) + ": Sin Dirección. Se descarta");
                    continue;
                }
                let v = fields[8];
                if (v !== undefined && v.trim().length) {
                    v = parseFloat(v);
                    if (!isNaN(v)) {
                        await minz.postData("fipafan2019.velocidad_viento", time.valueOf(), v, {
                            estacion:codigoEstacion, programa:programa, direccion:direccion.trim().toUpperCase()
                        });
                    }
                }
            }            
            res.status(200).send("Ok").end();
        } catch(error) {
            console.error(error);
            throw error;
        }
    }

    async importaTemperaturaAmbiental(req, res) {
        const fs = require("fs");
        try {
            console.log("\n\nImportando Temperatura Ambiental");
            console.log("  => Eliminando periodo anterior");
            let startTime = moment.tz("2006-01-01", config.timeZone);
            let endTime = moment.tz("2015-12-31", config.timeZone);
            await minz.deletePeriod("fipafan2019.temperatura_ambiental", startTime.valueOf(), endTime.valueOf(), true);
            let path = global.resDir + "/TEMP_AMB_2006_2015.csv";
            console.log("abriendo archivo");
            let lineas = await fs.readFileSync(path).toString().split("\n");
            console.log("lineas:" + lineas.length);
            for (let k=0; k < lineas.length; k++) {
                let line = lineas[k];
                if (!k) continue;
                let fields = line.split(",");
                if (fields.length < 8) {
                    console.warn(`Linea ${k+1} contiene ${fields.length} campos`);
                    continue;
                }
                if (!(k % 100)) console.log("linea:" + k);
                let codigoEstacion = fields[1];
                let programa = fields[2];
                let fMuestreo = fields[4];
                let time = moment.tz(fMuestreo, "YYYY/MM/DD", config.timeZone);
                if (!time._isValid) {
                    console.warn("Linea " + (k+1) + ": Fecha inválida: " + fMuestreo);
                    continue;
                }
                let v = fields[7];
                if (v !== undefined && v.trim().length) {
                    v = parseFloat(v);
                    if (!isNaN(v)) {
                        await minz.postData("fipafan2019.temperatura_ambiental", time.valueOf(), v, {
                            estacion:codigoEstacion, programa:programa
                        });
                    }
                }
            }            
            res.status(200).send("Ok").end();
        } catch(error) {
            console.error(error);
            throw error;
        }
    }

    async importaToxinaVDM(req, res) {
        const fs = require("fs");
        try {
            console.log("\n\nImportando Toxina VDM");
            console.log("  => Eliminando periodo anterior");
            let startTime = moment.tz("2006-01-01", config.timeZone);
            let endTime = moment.tz("2015-12-31", config.timeZone);
            await minz.deletePeriod("fipafan2019.toxina_vdm", startTime.valueOf(), endTime.valueOf(), true);
            let path = global.resDir + "/TOXINA_VDM_2006-2014.csv";
            console.log("abriendo archivo");
            let lineas = await fs.readFileSync(path).toString().split("\n");
            console.log("lineas:" + lineas.length);
            for (let k=0; k < lineas.length; k++) {
                let line = lineas[k];
                if (!k) continue;
                let fields = line.split(",");
                if (fields.length < 10) {
                    console.warn(`Linea ${k+1} contiene ${fields.length} campos`);
                    continue;
                }
                if (!(k % 100)) console.log("linea:" + k);
                let codigoEstacion = fields[1];
                let programa = fields[2];
                let fMuestreo = fields[4];
                let time = moment.tz(fMuestreo, "YYYY/MM/DD", config.timeZone);
                if (!time._isValid) {
                    console.warn("Linea " + (k+1) + ": Fecha inválida: " + fMuestreo);
                    continue;
                }
                let recurso = fields[7].trim().toUpperCase();
                if (!recurso || recurso == "SIN MUESTRA" || recurso == "SIN RECURSO" || recurso.startsWith("#")) {
                    console.warn("Linea " + (k+1) + ": Recurso Inválido: " + recurso);
                    continue;
                }
                let vdm = fields[8].trim();
                if (vdm == "+" || vdm == "-") {
                    await minz.postData("fipafan2019.toxina_vdm", time.valueOf(), (vdm == "+"?1:0), {
                        estacion:codigoEstacion, recurso:recurso, programa:programa
                    });
                }
            }            
            res.status(200).send("Ok").end();
        } catch(error) {
            console.error(error);
            throw error;
        }
    }

    async importaAbundanciaRelativa(req, res) {
        const fs = require("fs");
        try {
            console.log("\n\nImportando Toxina VDM");
            console.log("  => Eliminando periodo anterior");
            let startTime = moment.tz("2006-01-01", config.timeZone);
            let endTime = moment.tz("2015-12-31", config.timeZone);
            await minz.deletePeriod("fipafan2019.abundancia_relativa", startTime.valueOf(), endTime.valueOf(), true);
            let path = global.resDir + "/ABUNDANCIA_RELAT.csv";
            console.log("abriendo archivo");
            let lineas = await fs.readFileSync(path).toString().split("\n");
            console.log("lineas:" + lineas.length);
            for (let k=0; k < lineas.length; k++) {
                let line = lineas[k];
                if (!k) continue;
                let fields = line.split(",");
                if (fields.length < 15) {
                    console.warn(`Linea ${k+1} contiene ${fields.length} campos`);
                    continue;
                }
                if (!(k % 100)) console.log("linea:" + k);
                let codigoEstacion = fields[1];
                let programa = fields[2];
                let fMuestreo = fields[4];
                let time = moment.tz(fMuestreo, "YYYY/MM/DD", config.timeZone);
                if (!time._isValid) {
                    console.warn("Linea " + (k+1) + ": Fecha inválida: " + fMuestreo);
                    continue;
                }
                let especies = ["ARALEXCATE", "ARALEXOSTE", "ARALEXTAMA", "ARDINOACUM", "ARDINOACUT", "ARPROTRETI", "ARPSEUAUST", "ARPSEUPSEU"];
                for (let i=0; i<especies.length; i++) {
                    let v = fields[7 + i];
                    if (v !== undefined && v.trim().length) {
                        v = parseFloat(v);
                        if (!isNaN(v)) {
                            await minz.postData("fipafan2019.abundancia_relativa", time.valueOf(), v, {
                                estacion:codigoEstacion, programa:programa, especie:especies[i]
                            });
                        }
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