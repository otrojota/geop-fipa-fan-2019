const {ProveedorCapas, CapaVectorial} = require("geop-base-proveedor-capas");
const minz = require("./MinZClient");
const moment = require("moment-timezone");
const config = require("./Config").getConfig();

class ProveedorCapasFIPAFAN2019 extends ProveedorCapas {
    constructor(opciones) {
        super("fipafan2019", opciones);
        this.addOrigen("subpesca", "Subsecretaría de Pesca y Acuicultura", "http://www.subpesca.cl/", "./img/subpesca.jpg");        
        this.addOrigen("fipa", "Fondo de Investigación Pesquera y de Acuicultura", "http://www.subpesca.cl/fipa/613/w3-channel.html", "./img/fipa.png");

        let capaMacrozonasSanitarias = new CapaVectorial("fipafan2019", "MacrozonasSanitarias", "Macrozonas Sanitarias", "subpesca", {
            temporal:false,
            dimensionMinZ:"fipafan2019.macrozona",
            tipoObjetos:"poligono",
            formatos:{
                geoJSON:true,
            },
            estilos:function(f) {
                return {color:"#000000", weight:1, fillColor:"#ff0505", fillOpacity:1}
            }
        }, [], "img/macrozonas.svg");
        this.addCapa(capaMacrozonasSanitarias);   
        
        let capaEstaciones = new CapaVectorial("fipafan2019", "EstacionesMonitoreo", "Estaciones de Monitoreo", "fipa", {
            temporal:false,
            dimensionMinZ:"fipafan2019.estacion",
            tipoObjetos:"punto",
            formatos:{
                geoJSON:true,
            },
            estilos:function(f) {
                return {color:"#000000", weight:1, fillColor:"#ff0505", fillOpacity:1}
            }
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
        let path = global.resDir + "/macrozonas-sanitarias.geojson";
        let features = JSON.parse(require("fs").readFileSync(path));
        features.name = "SUB Pesca - Macrozonas Sanitarias";
        features.features.forEach(f => {
            f.properties.id = f.properties.REP_SUBPES;
            f.properties.nombre = f.properties.REP_SUBPES;
            f.properties._titulo = "Macrozona Sanitaria: " + f.properties.REP_SUBPES;
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
        let path = global.resDir + "/estaciones.geojson";
        let features = JSON.parse(require("fs").readFileSync(path));
        features.name = "FIPA-FAN-2019 - Estaciones de Monitoreo";
        features.features.forEach(f => {
            f.properties.id = f.properties.REP_SUBPES;
            f.properties.nombre = f.properties.REP_SUBP_8;
            f.properties._titulo = "Estación: " + f.properties.REP_SUBP_8;
            f.properties.nombreSector = f.properties.REP_SUBP_9;
            f.properties.nombreInstitucion = f.properties.REP_SUB_10;
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
    async initMinZ(req, res) {
        try {
            // Crear dimensiones
            if (!(await minz.existeDimension("bcn.region"))) {
                await minz.addOrSaveDimension({code:"bcn.region", name:"Región"});
            }
            if (!(await minz.existeDimension("bcn.provincia"))) {
                await minz.addOrSaveDimension({code:"bcn.provincia", name:"Provincia", classifiers:[{fieldName:"region", name:"Región", dimensionCode:"bcn.region", defaultValue:"00"}]});
            }
            if (!(await minz.existeDimension("bcn.comuna"))) {
                await minz.addOrSaveDimension({code:"bcn.comuna", name:"Comuna", classifiers:[{fieldName:"provincia", name:"Provincia", dimensionCode:"bcn.provincia", defaultValue:"000"}]});
            }
            for (let i=0; i<this.regiones.features.length; i++) {
                let region = this.regiones.features[i];
                let cod = this.pad(region.properties.codregion, 2);                
                await minz.addOrUpdateRow("bcn.region", {code:cod, name:region.properties.Region});
            }
            for (let i=0; i<this.provincias.features.length; i++) {
                let provincia = this.provincias.features[i];
                console.log("Provincia: " + provincia.properties.Provincia);
                let cod = this.pad(provincia.properties.cod_prov, 3);                
                let codregion = this.pad(provincia.properties.codregion, 2);                
                await minz.addOrUpdateRow("bcn.provincia", {code:cod, name:provincia.properties.Provincia, region:codregion});
            }
            for (let i=0; i<this.comunas.features.length; i++) {
                let comuna = this.comunas.features[i];
                let cod = this.pad(comuna.properties.cod_comuna, 5);
                let provincia = this.provincias.features.find(p => p.properties.Provincia == comuna.properties.Provincia);
                if (provincia) {
                    let codprovincia = this.pad(provincia.properties.cod_prov, 3);                
                    await minz.addOrUpdateRow("bcn.comuna", {code:cod, name:comuna.properties.Comuna, provincia:codprovincia});
                } else {
                    console.warn("No se encontró la Provincia '" + comuna.properties.Provincia + "' referenciada desde la comuna", comuna);
                }
            }

            // Crear Variables
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