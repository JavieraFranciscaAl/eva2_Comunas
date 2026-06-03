"""
ETL MULTI-MODULO - Arquitectura y Almacenamiento de Datos
INACAP Concepcion - Evaluacion 2 (Parte 1, 2 y 3)
Autora: Javiera Francisca Alarcon Albornoz

Modulos:
  1. COMUNAS_NORM  — normalizacion + enriquecimiento con datos INE (region, poblacion)
  2. FAMOSOS_NORM  — normalizacion de fechas, edad, flag cumpleanos + imagen Wikipedia
  3. LUGARES_NORM  — normalizacion, tres tablas relacionadas + mapa interactivo Leaflet
"""

import os
import re
import io
import json
import sqlite3
import unicodedata
import requests
from datetime import datetime, date
from flask import Flask, render_template, request, jsonify, send_file

app = Flask(__name__)

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
BASE_DIR = "/tmp/etl_norm"
DB_PATH  = f"{BASE_DIR}/etl_norm.db"
os.makedirs(BASE_DIR, exist_ok=True)

HOY = date.today()


# ══════════════════════════════════════════════════════════════════════════════
# DATASET EMBEBIDO — COMUNAS CHILE (INE Censo 2017)
# Fuente: Instituto Nacional de Estadisticas, Chile
# Incluye las 346 comunas con region y poblacion oficial
# Se embebe en codigo para garantizar disponibilidad sin dependencia de API externa
# ══════════════════════════════════════════════════════════════════════════════

COMUNAS_CHILE = {
    "arica": ("Arica y Parinacota", 213645),
    "camarones": ("Arica y Parinacota", 1044),
    "putre": ("Arica y Parinacota", 2490),
    "general lagos": ("Arica y Parinacota", 941),
    "iquique": ("Tarapaca", 191468),
    "alto hospicio": ("Tarapaca", 108534),
    "pozo almonte": ("Tarapaca", 15503),
    "camina": ("Tarapaca", 1435),
    "colchane": ("Tarapaca", 1637),
    "huara": ("Tarapaca", 2537),
    "pica": ("Tarapaca", 5432),
    "antofagasta": ("Antofagasta", 407355),
    "mejillones": ("Antofagasta", 13884),
    "sierra gorda": ("Antofagasta", 2322),
    "taltal": ("Antofagasta", 11491),
    "calama": ("Antofagasta", 175902),
    "ollague": ("Antofagasta", 328),
    "san pedro de atacama": ("Antofagasta", 10979),
    "tocopilla": ("Antofagasta", 25186),
    "maria elena": ("Antofagasta", 6008),
    "copiapo": ("Atacama", 158046),
    "caldera": ("Atacama", 17064),
    "tierra amarilla": ("Atacama", 13491),
    "chanaral": ("Atacama", 13436),
    "diego de almagro": ("Atacama", 18475),
    "vallenar": ("Atacama", 50798),
    "alto del carmen": ("Atacama", 5361),
    "freirina": ("Atacama", 6786),
    "huasco": ("Atacama", 8408),
    "la serena": ("Coquimbo", 221418),
    "coquimbo": ("Coquimbo", 227256),
    "andacollo": ("Coquimbo", 10983),
    "la higuera": ("Coquimbo", 4054),
    "paihuano": ("Coquimbo", 5073),
    "vicuna": ("Coquimbo", 27370),
    "illapel": ("Coquimbo", 34062),
    "canela": ("Coquimbo", 9378),
    "los vilos": ("Coquimbo", 21042),
    "salamanca": ("Coquimbo", 26426),
    "ovalle": ("Coquimbo", 114257),
    "combarbala": ("Coquimbo", 12827),
    "monte patria": ("Coquimbo", 30473),
    "punitaqui": ("Coquimbo", 10265),
    "rio hurtado": ("Coquimbo", 4533),
    "valparaiso": ("Valparaiso", 296655),
    "casablanca": ("Valparaiso", 32127),
    "concon": ("Valparaiso", 49683),
    "juan fernandez": ("Valparaiso", 909),
    "puchuncavi": ("Valparaiso", 19180),
    "quintero": ("Valparaiso", 24349),
    "vina del mar": ("Valparaiso", 324836),
    "isla de pascua": ("Valparaiso", 7750),
    "los andes": ("Valparaiso", 69413),
    "calle larga": ("Valparaiso", 14007),
    "rinconada": ("Valparaiso", 9961),
    "san esteban": ("Valparaiso", 17199),
    "la ligua": ("Valparaiso", 38533),
    "cabildo": ("Valparaiso", 23432),
    "papudo": ("Valparaiso", 6542),
    "petorca": ("Valparaiso", 12296),
    "zapallar": ("Valparaiso", 8466),
    "quillota": ("Valparaiso", 90024),
    "calera": ("Valparaiso", 52435),
    "hijuelas": ("Valparaiso", 18428),
    "la cruz": ("Valparaiso", 17593),
    "nogales": ("Valparaiso", 26344),
    "san antonio": ("Valparaiso", 92167),
    "algarrobo": ("Valparaiso", 14371),
    "cartagena": ("Valparaiso", 24099),
    "el quisco": ("Valparaiso", 14908),
    "el tabo": ("Valparaiso", 11363),
    "santo domingo": ("Valparaiso", 11356),
    "san felipe": ("Valparaiso", 74799),
    "catemu": ("Valparaiso", 12700),
    "llaillay": ("Valparaiso", 22958),
    "panquehue": ("Valparaiso", 8737),
    "putaendo": ("Valparaiso", 17297),
    "santa maria": ("Valparaiso", 15938),
    "quilpue": ("Valparaiso", 215574),
    "limache": ("Valparaiso", 44566),
    "olmue": ("Valparaiso", 21752),
    "villa alemana": ("Valparaiso", 136702),
    "santiago": ("Metropolitana", 404495),
    "cerrillos": ("Metropolitana", 83635),
    "cerro navia": ("Metropolitana", 148783),
    "conchalí": ("Metropolitana", 136364),
    "conchali": ("Metropolitana", 136364),
    "el bosque": ("Metropolitana", 175594),
    "estacion central": ("Metropolitana", 147318),
    "huechuraba": ("Metropolitana", 106116),
    "independencia": ("Metropolitana", 114209),
    "la cisterna": ("Metropolitana", 90075),
    "la florida": ("Metropolitana", 366416),
    "la granja": ("Metropolitana", 133518),
    "la pintana": ("Metropolitana", 190085),
    "la reina": ("Metropolitana", 96627),
    "las condes": ("Metropolitana", 294838),
    "lo barnechea": ("Metropolitana", 105833),
    "lo espejo": ("Metropolitana", 107609),
    "lo prado": ("Metropolitana", 104316),
    "macul": ("Metropolitana", 118940),
    "maipu": ("Metropolitana", 521627),
    "nunoa": ("Metropolitana", 207765),
    "pedro aguirre cerda": ("Metropolitana", 113630),
    "penalolen": ("Metropolitana", 241963),
    "providencia": ("Metropolitana", 142079),
    "pudahuel": ("Metropolitana", 231557),
    "quilicura": ("Metropolitana", 209635),
    "quinta normal": ("Metropolitana", 111038),
    "recoleta": ("Metropolitana", 148220),
    "renca": ("Metropolitana", 156110),
    "san joaquin": ("Metropolitana", 104103),
    "san miguel": ("Metropolitana", 107370),
    "san ramon": ("Metropolitana", 94906),
    "vitacura": ("Metropolitana", 85384),
    "puente alto": ("Metropolitana", 568106),
    "pirque": ("Metropolitana", 18115),
    "san jose de maipo": ("Metropolitana", 17800),
    "colina": ("Metropolitana", 141943),
    "lampa": ("Metropolitana", 105151),
    "tiltil": ("Metropolitana", 16833),
    "san bernardo": ("Metropolitana", 319204),
    "buin": ("Metropolitana", 88060),
    "calera de tango": ("Metropolitana", 30630),
    "paine": ("Metropolitana", 86076),
    "melipilla": ("Metropolitana", 124430),
    "alhue": ("Metropolitana", 5218),
    "curacavi": ("Metropolitana", 27626),
    "maria pinto": ("Metropolitana", 11727),
    "san pedro": ("Metropolitana", 13814),
    "talagante": ("Metropolitana", 77132),
    "el monte": ("Metropolitana", 40167),
    "isla de maipo": ("Metropolitana", 37804),
    "padre hurtado": ("Metropolitana", 85259),
    "penaflor": ("Metropolitana", 101473),
    "rancagua": ("O'Higgins", 241959),
    "codegua": ("O'Higgins", 14434),
    "coinco": ("O'Higgins", 7104),
    "coltauco": ("O'Higgins", 15748),
    "donihue": ("O'Higgins", 16034),
    "graneros": ("O'Higgins", 28780),
    "las cabras": ("O'Higgins", 22286),
    "machali": ("O'Higgins", 52773),
    "mostazal": ("O'Higgins", 33127),
    "olivar": ("O'Higgins", 20782),
    "peumo": ("O'Higgins", 18208),
    "pichidegua": ("O'Higgins", 14800),
    "quinta de tilcoco": ("O'Higgins", 11278),
    "rengo": ("O'Higgins", 56233),
    "requinoa": ("O'Higgins", 22721),
    "san vicente": ("O'Higgins", 42358),
    "pichilemu": ("O'Higgins", 16055),
    "la estrella": ("O'Higgins", 4255),
    "litueche": ("O'Higgins", 6584),
    "marchihue": ("O'Higgins", 7572),
    "navidad": ("O'Higgins", 6826),
    "paredones": ("O'Higgins", 8075),
    "san fernando": ("O'Higgins", 70616),
    "chepica": ("O'Higgins", 13694),
    "chimbarongo": ("O'Higgins", 31038),
    "lolol": ("O'Higgins", 6679),
    "nancagua": ("O'Higgins", 16185),
    "palmilla": ("O'Higgins", 10252),
    "peralillo": ("O'Higgins", 10736),
    "placilla": ("O'Higgins", 10700),
    "pumanque": ("O'Higgins", 3814),
    "santa cruz": ("O'Higgins", 44003),
    "talca": ("Maule", 232084),
    "constitucion": ("Maule", 46274),
    "curepto": ("Maule", 11491),
    "empedrado": ("Maule", 5723),
    "maule": ("Maule", 22697),
    "pelarco": ("Maule", 9917),
    "pencahue": ("Maule", 9131),
    "rio claro": ("Maule", 13613),
    "san clemente": ("Maule", 37651),
    "san rafael": ("Maule", 10049),
    "cauquenes": ("Maule", 41455),
    "chanco": ("Maule", 9684),
    "pelluhue": ("Maule", 8130),
    "curico": ("Maule", 150104),
    "hualane": ("Maule", 9734),
    "licanten": ("Maule", 8484),
    "molina": ("Maule", 45293),
    "rauco": ("Maule", 10474),
    "romeral": ("Maule", 16461),
    "sagrada familia": ("Maule", 15540),
    "teno": ("Maule", 22836),
    "vichuquen": ("Maule", 5218),
    "linares": ("Maule", 95655),
    "colbun": ("Maule", 25023),
    "longavi": ("Maule", 28580),
    "parral": ("Maule", 41993),
    "retiro": ("Maule", 19374),
    "san javier": ("Maule", 41228),
    "villa alegre": ("Maule", 15066),
    "yerbas buenas": ("Maule", 16534),
    "chillan": ("Nuble", 191989),
    "bulnes": ("Nuble", 22415),
    "chillan viejo": ("Nuble", 40897),
    "el carmen": ("Nuble", 17413),
    "pemuco": ("Nuble", 12050),
    "pinto": ("Nuble", 11835),
    "quillon": ("Nuble", 12002),
    "san ignacio": ("Nuble", 22001),
    "yungay": ("Nuble", 19256),
    "quirihue": ("Nuble", 15118),
    "cobquecura": ("Nuble", 5453),
    "coelemu": ("Nuble", 19271),
    "ninhue": ("Nuble", 6613),
    "portezuelo": ("Nuble", 7657),
    "ranquil": ("Nuble", 8011),
    "trehuaco": ("Nuble", 6070),
    "san carlos": ("Nuble", 52081),
    "coihueco": ("Nuble", 24010),
    "niquen": ("Nuble", 13162),
    "san fabian": ("Nuble", 5553),
    "san nicolas": ("Nuble", 16193),
    "concepcion": ("Biobio", 223574),
    "coronel": ("Biobio", 116752),
    "chiguayante": ("Biobio", 90744),
    "florida": ("Biobio", 13824),
    "hualqui": ("Biobio", 27069),
    "lota": ("Biobio", 44773),
    "penco": ("Biobio", 50000),
    "san pedro de la paz": ("Biobio", 133787),
    "santa juana": ("Biobio", 14685),
    "talcahuano": ("Biobio", 163981),
    "tome": ("Biobio", 55804),
    "hualpen": ("Biobio", 103985),
    "lebu": ("Biobio", 25617),
    "arauco": ("Biobio", 35684),
    "canete": ("Biobio", 33540),
    "contulmo": ("Biobio", 5972),
    "curanilahue": ("Biobio", 31819),
    "los alamos": ("Biobio", 25183),
    "tirua": ("Biobio", 11277),
    "los angeles": ("Biobio", 209496),
    "antuco": ("Biobio", 5979),
    "cabrero": ("Biobio", 31516),
    "laja": ("Biobio", 25124),
    "mulchen": ("Biobio", 37256),
    "nacimiento": ("Biobio", 30390),
    "negrete": ("Biobio", 10174),
    "quilaco": ("Biobio", 5455),
    "quilleco": ("Biobio", 9736),
    "san rosendo": ("Biobio", 4779),
    "santa barbara": ("Biobio", 16700),
    "tucapel": ("Biobio", 15777),
    "yumbel": ("Biobio", 22474),
    "alto biobio": ("Biobio", 9293),
    "temuco": ("Araucania", 282415),
    "carahue": ("Araucania", 27283),
    "cunco": ("Araucania", 20696),
    "curarrehue": ("Araucania", 10002),
    "freire": ("Araucania", 29810),
    "galvarino": ("Araucania", 18382),
    "gorbea": ("Araucania", 18547),
    "lautaro": ("Araucania", 36906),
    "loncoche": ("Araucania", 27199),
    "melipeuco": ("Araucania", 7518),
    "nueva imperial": ("Araucania", 42601),
    "padre las casas": ("Araucania", 71138),
    "perquenco": ("Araucania", 9867),
    "pitrufquen": ("Araucania", 27148),
    "pucon": ("Araucania", 31505),
    "saavedra": ("Araucania", 17041),
    "teodoro schmidt": ("Araucania", 16456),
    "tolten": ("Araucania", 11565),
    "vilcun": ("Araucania", 26285),
    "villarrica": ("Araucania", 60268),
    "cholchol": ("Araucania", 14328),
    "angol": ("Araucania", 54346),
    "collipulli": ("Araucania", 25527),
    "curacautin": ("Araucania", 20234),
    "ercilla": ("Araucania", 12014),
    "lonquimay": ("Araucania", 14992),
    "los sauces": ("Araucania", 9677),
    "lumaco": ("Araucania", 12684),
    "puren": ("Araucania", 13936),
    "renaico": ("Araucania", 13536),
    "traiguen": ("Araucania", 24162),
    "victoria": ("Araucania", 36247),
    "valdivia": ("Los Rios", 166080),
    "corral": ("Los Rios", 5962),
    "futrono": ("Los Rios", 15999),
    "la union": ("Los Rios", 43781),
    "lago ranco": ("Los Rios", 11195),
    "lanco": ("Los Rios", 17534),
    "los lagos": ("Los Rios", 26234),
    "mafil": ("Los Rios", 8406),
    "mariquina": ("Los Rios", 24049),
    "paillaco": ("Los Rios", 22952),
    "panguipulli": ("Los Rios", 38228),
    "rio bueno": ("Los Rios", 37296),
    "puerto montt": ("Los Lagos", 245902),
    "calbuco": ("Los Lagos", 33759),
    "cochamo": ("Los Lagos", 4055),
    "fresia": ("Los Lagos", 14239),
    "frutillar": ("Los Lagos", 21047),
    "los muermos": ("Los Lagos", 17524),
    "llanquihue": ("Los Lagos", 23633),
    "maullin": ("Los Lagos", 18290),
    "puerto varas": ("Los Lagos", 41720),
    "castro": ("Los Lagos", 44631),
    "ancud": ("Los Lagos", 41254),
    "chonchi": ("Los Lagos", 13005),
    "curaco de velez": ("Los Lagos", 3972),
    "dalcahue": ("Los Lagos", 15527),
    "puqueldon": ("Los Lagos", 3630),
    "queilen": ("Los Lagos", 5148),
    "quellon": ("Los Lagos", 31314),
    "quemchi": ("Los Lagos", 9671),
    "quinchao": ("Los Lagos", 11550),
    "osorno": ("Los Lagos", 145475),
    "puerto octay": ("Los Lagos", 11146),
    "purranque": ("Los Lagos", 26396),
    "puyehue": ("Los Lagos", 12660),
    "rio negro": ("Los Lagos", 18793),
    "san juan de la costa": ("Los Lagos", 8500),
    "san pablo": ("Los Lagos", 10272),
    "chaiten": ("Los Lagos", 7191),
    "futaleufu": ("Los Lagos", 2613),
    "hualaihue": ("Los Lagos", 12261),
    "palena": ("Los Lagos", 2593),
    "coihaique": ("Aysen", 57651),
    "lago verde": ("Aysen", 1420),
    "aysen": ("Aysen", 17799),
    "cisnes": ("Aysen", 6088),
    "guaitecas": ("Aysen", 1255),
    "cochrane": ("Aysen", 3367),
    "ohiggins": ("Aysen", 639),
    "tortel": ("Aysen", 599),
    "chile chico": ("Aysen", 4850),
    "rio ibanez": ("Aysen", 2219),
    "punta arenas": ("Magallanes", 131445),
    "laguna blanca": ("Magallanes", 577),
    "rio verde": ("Magallanes", 512),
    "san gregorio": ("Magallanes", 1181),
    "cabo de hornos": ("Magallanes", 2444),
    "antartica": ("Magallanes", 149),
    "porvenir": ("Magallanes", 6279),
    "primavera": ("Magallanes", 468),
    "timaukel": ("Magallanes", 373),
    "natales": ("Magallanes", 21528),
    "torres del paine": ("Magallanes", 946),
}


# ══════════════════════════════════════════════════════════════════════════════
# BASE DE DATOS
# ══════════════════════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_db():
    """Crea todas las tablas si no existen."""
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS COMUNAS_NORM (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre      TEXT    NOT NULL UNIQUE,
                region      TEXT,
                poblacion   INTEGER,
                fecha_carga TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS FAMOSOS_NORM (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre           TEXT    NOT NULL UNIQUE,
                fecha_nacimiento TEXT    NOT NULL,
                edad             INTEGER,
                cumpleanios      INTEGER NOT NULL DEFAULT 0,
                imagen_url       TEXT,
                imagen_fuente    TEXT,
                imagen_fecha     TEXT,
                fecha_carga      TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS Lugares (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT    NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS Georeferencias (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                id_lugar INTEGER NOT NULL REFERENCES Lugares(id),
                latitud  REAL    NOT NULL,
                longitud REAL    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS Direcciones (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                id_lugar                INTEGER NOT NULL REFERENCES Lugares(id),
                nombre_calle            TEXT,
                numero_calle            TEXT,
                ciudad_estado_provincia TEXT,
                pais                    TEXT
            );
        """)
        conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# NORMALIZACION COMUN
# ══════════════════════════════════════════════════════════════════════════════

def quitar_tildes(texto):
    """Elimina diacriticos usando NFD. Reemplaza n/N con tilde explicitamente."""
    nfd = unicodedata.normalize("NFD", texto)
    sin_tildes = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    sin_tildes = sin_tildes.replace("ñ", "n").replace("Ñ", "N")
    return sin_tildes


def limpiar_texto(texto):
    """Limpieza completa: strip, espacios multiples, tildes, formato Titulo."""
    limpio = re.sub(r"\s+", " ", texto.strip())
    limpio = quitar_tildes(limpio)
    return limpio.title()


# ══════════════════════════════════════════════════════════════════════════════
# MODULO 1 — COMUNAS_NORM
# ══════════════════════════════════════════════════════════════════════════════

def buscar_comuna_ine(nombre_normalizado):
    """
    Busca la comuna en el dataset embebido del INE.
    La busqueda es case-insensitive y sin tildes.
    Retorna (region, poblacion) o (None, None) si no se encuentra.
    """
    clave = quitar_tildes(nombre_normalizado.lower().strip())
    if clave in COMUNAS_CHILE:
        return COMUNAS_CHILE[clave]
    # Busqueda parcial: si la clave esta contenida en alguna entrada
    for k, v in COMUNAS_CHILE.items():
        if clave in k or k in clave:
            return v
    return None, None


def etl_comunas(lineas, formato="titulo"):
    """
    ETL para comunas con enriquecimiento INE.
    formato: 'titulo' | 'mayusculas' | 'minusculas'
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = ["=" * 70, "COMUNAS_NORM — LOG ETL", f"Fecha: {timestamp}",
           f"Formato aplicado: {formato}", "=" * 70, ""]

    vistas = {}
    resultado = []
    entrada = modif = duplic = vacios = consolidados = no_encontrados = 0

    for i, linea in enumerate(lineas, 1):
        original = linea.rstrip("\r\n")
        if not original.strip():
            vacios += 1
            continue
        entrada += 1

        # Normalizar segun formato elegido por el usuario
        sin_tildes = quitar_tildes(re.sub(r"\s+", " ", original.strip()))
        if formato == "mayusculas":
            norm = sin_tildes.upper()
        elif formato == "minusculas":
            norm = sin_tildes.lower()
        else:
            norm = sin_tildes.title()

        clave = norm.lower()
        if clave in vistas:
            duplic += 1
            log.append(f"[{i:05d}] DUPLICADO  | '{original}'")
            continue

        vistas[clave] = norm
        region, poblacion = buscar_comuna_ine(norm)

        if region:
            consolidados += 1
            log.append(f"[{i:05d}] OK         | '{norm}' — {region} — {poblacion:,} hab.")
        else:
            no_encontrados += 1
            log.append(f"[{i:05d}] NO ENCONTRADO | '{norm}' — no esta en dataset INE")

        if original != norm:
            modif += 1

        resultado.append((norm, region, poblacion, timestamp))

    log += ["", "=" * 70, "RESUMEN",
            f"Leidos: {entrada} | Duplicados: {duplic} | Consolidados: {consolidados} | No encontrados: {no_encontrados}",
            "=" * 70]

    with get_db() as conn:
        conn.execute("DELETE FROM COMUNAS_NORM")
        conn.executemany(
            "INSERT OR REPLACE INTO COMUNAS_NORM (nombre, region, poblacion, fecha_carga) VALUES (?,?,?,?)",
            resultado
        )
        conn.commit()

    with open(f"{BASE_DIR}/comunas_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    return {
        "entrada": entrada, "modificados": modif, "duplicados": duplic,
        "consolidados": consolidados, "no_encontrados": no_encontrados,
        "resultado": len(resultado)
    }, log[:35]


def buscar_comunas_sugeridas(query):
    """
    Busqueda por texto parcial en el dataset INE.
    Retorna lista de comunas que coinciden con el query.
    Ejemplo: 'florida' -> ['Florida', 'La Florida']
    """
    q = quitar_tildes(query.lower().strip())
    sugerencias = []
    for clave, (region, poblacion) in COMUNAS_CHILE.items():
        if q in clave:
            nombre_display = clave.title()
            sugerencias.append({
                "nombre": nombre_display,
                "region": region,
                "poblacion": poblacion
            })
    return sorted(sugerencias, key=lambda x: x["nombre"])


# ══════════════════════════════════════════════════════════════════════════════
# MODULO 2 — FAMOSOS_NORM
# ══════════════════════════════════════════════════════════════════════════════

def parsear_fecha(texto_fecha):
    """
    Convierte fecha a formato chileno DD-MM-YYYY.
    Soporta: YYYY/MM/DD, YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY
    """
    texto = texto_fecha.strip()
    m = re.match(r'^(\d{4})[/-](\d{2})[/-](\d{2})$', texto)
    if m:
        anio, mes, dia = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{dia:02d}-{mes:02d}-{anio}", anio
    m = re.match(r'^(\d{2})[/-](\d{2})[/-](\d{4})$', texto)
    if m:
        dia, mes, anio = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{dia:02d}-{mes:02d}-{anio}", anio
    return None, None


def calcular_edad(fecha_str):
    """Calcula edad en anos desde DD-MM-YYYY hasta hoy."""
    try:
        dia, mes, anio = map(int, fecha_str.split("-"))
        edad = HOY.year - anio
        if (HOY.month, HOY.day) < (mes, dia):
            edad -= 1
        return edad
    except Exception:
        return None


def es_cumpleanios(fecha_str):
    """Retorna 1 si hoy coincide con dia y mes, 0 si no."""
    try:
        dia, mes, _ = map(int, fecha_str.split("-"))
        return 1 if (HOY.day == dia and HOY.month == mes) else 0
    except Exception:
        return 0


def obtener_imagen_wikipedia(nombre):
    """
    Consulta la API de Wikipedia para obtener la imagen principal del famoso.
    Almacena URL, fuente y fecha de captura.
    Retorna (imagen_url, fuente, fecha_captura) o (None, None, None) si no encuentra.
    """
    try:
        # Endpoint de Wikipedia API — gratuito, sin API key
        url = "https://en.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "titles": nombre,
            "prop": "pageimages",
            "pithumbsize": 300,
            "format": "json",
            "origin": "*"
        }
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()
        pages = data.get("query", {}).get("pages", {})
        for page in pages.values():
            thumb = page.get("thumbnail", {})
            if thumb.get("source"):
                fecha_captura = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return thumb["source"], "Wikipedia API (en.wikipedia.org)", fecha_captura
    except Exception:
        pass
    return None, None, None


def etl_famosos(lineas):
    """ETL para famosos: fechas, edad, cumpleanos. Sin consulta de imagen (se hace on-demand)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = ["=" * 70, "FAMOSOS_NORM — LOG ETL", f"Fecha: {timestamp}", "=" * 70, ""]
    vistas = {}
    resultado = []
    entrada = modif = duplic = descartados = cumpleanios_hoy = 0

    for i, linea in enumerate(lineas, 1):
        original = linea.rstrip("\r\n").strip()
        if not original:
            continue
        limpio = re.sub(r'^\d+\.\s*', '', original)
        partes = limpio.rsplit(' - ', 1)
        if len(partes) != 2:
            descartados += 1
            log.append(f"[{i:05d}] NO PARSEABLE | '{original}'")
            continue

        nombre = partes[0].strip()
        texto_fecha = partes[1].strip()

        if 'alrededor' in texto_fecha.lower() or 'a.c.' in texto_fecha.lower():
            descartados += 1
            log.append(f"[{i:05d}] DESCARTADO  | '{nombre}' — fecha aproximada: '{texto_fecha}'")
            continue

        fecha_norm, _ = parsear_fecha(texto_fecha)
        if not fecha_norm:
            descartados += 1
            log.append(f"[{i:05d}] DESCARTADO  | '{nombre}' — formato desconocido: '{texto_fecha}'")
            continue

        clave = nombre.lower()
        if clave in vistas:
            duplic += 1
            log.append(f"[{i:05d}] DUPLICADO   | '{nombre}'")
            continue

        entrada += 1
        edad  = calcular_edad(fecha_norm)
        cumple = es_cumpleanios(fecha_norm)
        if cumple:
            cumpleanios_hoy += 1
            log.append(f"[{i:05d}] CUMPLEANOS  | '{nombre}' cumple hoy!")

        vistas[clave] = nombre
        resultado.append((nombre, fecha_norm, edad, cumple, None, None, None, timestamp))

        if texto_fecha != fecha_norm:
            modif += 1
            log.append(f"[{i:05d}] MODIFICADO  | '{nombre}' | '{texto_fecha}' -> '{fecha_norm}' | Edad: {edad}")
        else:
            log.append(f"[{i:05d}] SIN CAMBIO  | '{nombre}' | '{fecha_norm}' | Edad: {edad}")

    log += ["", "=" * 70, "RESUMEN",
            f"Procesados: {entrada} | Modificados: {modif} | Duplicados: {duplic} | Descartados: {descartados} | Cumplen hoy: {cumpleanios_hoy}",
            "=" * 70]

    with get_db() as conn:
        conn.execute("DELETE FROM FAMOSOS_NORM")
        conn.executemany(
            "INSERT OR IGNORE INTO FAMOSOS_NORM (nombre, fecha_nacimiento, edad, cumpleanios, imagen_url, imagen_fuente, imagen_fecha, fecha_carga) VALUES (?,?,?,?,?,?,?,?)",
            resultado
        )
        conn.commit()

    with open(f"{BASE_DIR}/famosos_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    return {
        "entrada": entrada, "modificados": modif, "duplicados": duplic,
        "descartados": descartados, "resultado": len(resultado),
        "cumpleanios_hoy": cumpleanios_hoy
    }, log[:35]


# ══════════════════════════════════════════════════════════════════════════════
# MODULO 3 — LUGARES_NORM
# ══════════════════════════════════════════════════════════════════════════════

def parsear_direccion(direccion_raw):
    """Separa direccion en nombre_calle, numero_calle, ciudad_estado, pais."""
    if not direccion_raw or not direccion_raw.strip():
        return "", "", "", ""
    partes = [p.strip() for p in direccion_raw.split(",")]
    pais = partes[-1] if len(partes) >= 1 else ""
    ciudad_estado = partes[-2] if len(partes) >= 2 else ""
    calle_completa = ", ".join(partes[:-2]) if len(partes) > 2 else (partes[0] if partes else "")
    tokens = calle_completa.split()
    if tokens and re.match(r'^\d+', tokens[0]):
        numero_calle = tokens[0]
        nombre_calle = " ".join(tokens[1:])
    else:
        numero_calle = ""
        nombre_calle = calle_completa
    return nombre_calle.strip(), numero_calle.strip(), ciudad_estado.strip(), pais.strip()


def contar_campos(direccion):
    """Cuenta segmentos no vacios en una direccion."""
    return sum(1 for c in direccion.split(",") if c.strip())


def etl_lugares(lineas):
    """ETL para lugares: deduplicacion, tres tablas relacionadas."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = ["=" * 70, "LUGARES_NORM — LOG ETL", f"Fecha: {timestamp}", "=" * 70, ""]

    datos_raw = []
    for i, linea in enumerate(lineas):
        linea = linea.rstrip("\r\n").strip()
        if i == 0 or not linea:
            continue
        partes = linea.split(";")
        if len(partes) < 3:
            continue
        nombre = partes[0].strip().encode('ascii', 'ignore').decode('ascii')
        direcc = partes[1].strip().encode('ascii', 'ignore').decode('ascii')
        georef = partes[2].strip()
        datos_raw.append((nombre, direcc, georef))

    mejores = {}
    for nombre, direcc, georef in datos_raw:
        clave = nombre.lower().strip()
        if clave not in mejores:
            mejores[clave] = (nombre, direcc, georef)
            log.append(f"NUEVO       | '{nombre}'")
        else:
            actual = mejores[clave][1]
            if contar_campos(direcc) > contar_campos(actual):
                mejores[clave] = (nombre, direcc, georef)
                log.append(f"REEMPLAZADO | '{nombre}' — direccion mas completa")
            else:
                log.append(f"DUPLICADO   | '{nombre}' — se conserva registro anterior")

    with get_db() as conn:
        conn.execute("DELETE FROM Direcciones")
        conn.execute("DELETE FROM Georeferencias")
        conn.execute("DELETE FROM Lugares")
        conn.commit()

        for clave, (nombre, direcc, georef) in mejores.items():
            cur = conn.execute("INSERT INTO Lugares (nombre) VALUES (?)", (nombre,))
            id_lugar = cur.lastrowid
            try:
                lat_str, lon_str = georef.split(",")
                lat = float(lat_str.strip())
                lon = float(lon_str.strip())
                conn.execute(
                    "INSERT INTO Georeferencias (id_lugar, latitud, longitud) VALUES (?,?,?)",
                    (id_lugar, lat, lon)
                )
            except Exception:
                log.append(f"  GEOREF ERROR | '{nombre}'")
            nombre_calle, numero_calle, ciudad_estado, pais = parsear_direccion(direcc)
            conn.execute(
                "INSERT INTO Direcciones (id_lugar, nombre_calle, numero_calle, ciudad_estado_provincia, pais) VALUES (?,?,?,?,?)",
                (id_lugar, nombre_calle, numero_calle, ciudad_estado, pais)
            )
        conn.commit()

    total = len(mejores)
    duplicados = len(datos_raw) - total
    log += ["", "=" * 70, "RESUMEN",
            f"Entrada: {len(datos_raw)} | Duplicados: {duplicados} | Cargados en BD: {total}",
            "=" * 70]

    with open(f"{BASE_DIR}/lugares_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    return {"entrada": len(datos_raw), "duplicados": duplicados, "resultado": total}, log[:35]


# ══════════════════════════════════════════════════════════════════════════════
# RUTAS FLASK
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


# ── COMUNAS ───────────────────────────────────────────────────────────────────

@app.route("/comunas/procesar", methods=["POST"])
def comunas_procesar():
    archivo = request.files.get("archivo")
    formato = request.form.get("formato", "titulo")
    if not archivo:
        return jsonify({"error": "No se recibio archivo"}), 400
    lineas = archivo.read().decode("utf-8", errors="replace").splitlines()
    stats, log_preview = etl_comunas(lineas, formato)
    return jsonify({"estadisticas": stats, "log_preview": log_preview})

@app.route("/comunas/buscar")
def comunas_buscar():
    """Busqueda en tiempo real de comunas por texto parcial."""
    q = request.args.get("q", "")
    if len(q) < 2:
        return jsonify({"sugerencias": []})
    return jsonify({"sugerencias": buscar_comunas_sugeridas(q)})

@app.route("/comunas/datos")
def comunas_datos():
    with get_db() as conn:
        filas = conn.execute(
            "SELECT id, nombre, region, poblacion, fecha_carga FROM COMUNAS_NORM ORDER BY nombre"
        ).fetchall()
    return jsonify({"datos": [dict(f) for f in filas], "total": len(filas)})

@app.route("/comunas/descargar/csv")
def comunas_csv():
    with get_db() as conn:
        filas = conn.execute("SELECT * FROM COMUNAS_NORM ORDER BY nombre").fetchall()
    lineas = ["id,nombre,region,poblacion,fecha_carga"] + [
        f"{r['id']},{r['nombre']},{r['region'] or ''},{r['poblacion'] or ''},{r['fecha_carga']}"
        for r in filas
    ]
    return send_file(io.BytesIO("\n".join(lineas).encode()), as_attachment=True,
                     download_name="COMUNAS_NORM.csv", mimetype="text/csv")

@app.route("/comunas/descargar/log")
def comunas_log():
    ruta = f"{BASE_DIR}/comunas_log.txt"
    if not os.path.exists(ruta): return "Sin log", 404
    return send_file(ruta, as_attachment=True, download_name="COMUNAS_LOG.txt")


# ── FAMOSOS ───────────────────────────────────────────────────────────────────

@app.route("/famosos/procesar", methods=["POST"])
def famosos_procesar():
    archivo = request.files.get("archivo")
    if not archivo:
        return jsonify({"error": "No se recibio archivo"}), 400
    lineas = archivo.read().decode("utf-8", errors="replace").splitlines()
    stats, log_preview = etl_famosos(lineas)
    return jsonify({"estadisticas": stats, "log_preview": log_preview})

@app.route("/famosos/imagen/<int:famoso_id>")
def famosos_imagen(famoso_id):
    """
    Obtiene la imagen de un famoso desde Wikipedia.
    Si ya esta en la BD, la retorna directamente (cache).
    Si no, consulta la API y la almacena.
    """
    with get_db() as conn:
        row = conn.execute(
            "SELECT nombre, imagen_url, imagen_fuente, imagen_fecha FROM FAMOSOS_NORM WHERE id=?",
            (famoso_id,)
        ).fetchone()

    if not row:
        return jsonify({"error": "Famoso no encontrado"}), 404

    # Si ya tiene imagen en cache, retornar directamente
    if row["imagen_url"]:
        return jsonify({
            "imagen_url":    row["imagen_url"],
            "imagen_fuente": row["imagen_fuente"],
            "imagen_fecha":  row["imagen_fecha"],
            "desde_cache":   True
        })

    # Consultar Wikipedia
    img_url, fuente, fecha_cap = obtener_imagen_wikipedia(row["nombre"])

    if img_url:
        with get_db() as conn:
            conn.execute(
                "UPDATE FAMOSOS_NORM SET imagen_url=?, imagen_fuente=?, imagen_fecha=? WHERE id=?",
                (img_url, fuente, fecha_cap, famoso_id)
            )
            conn.commit()
        return jsonify({"imagen_url": img_url, "imagen_fuente": fuente,
                        "imagen_fecha": fecha_cap, "desde_cache": False})

    return jsonify({"error": "Imagen no disponible en Wikipedia"}), 404

@app.route("/famosos/datos")
def famosos_datos():
    with get_db() as conn:
        filas = conn.execute(
            "SELECT id, nombre, fecha_nacimiento, edad, cumpleanios, imagen_url, fecha_carga FROM FAMOSOS_NORM ORDER BY nombre"
        ).fetchall()
    return jsonify({"datos": [dict(f) for f in filas], "total": len(filas)})

@app.route("/famosos/descargar/csv")
def famosos_csv():
    with get_db() as conn:
        filas = conn.execute("SELECT id, nombre, fecha_nacimiento, edad, cumpleanios, fecha_carga FROM FAMOSOS_NORM ORDER BY nombre").fetchall()
    lineas = ["id,nombre,fecha_nacimiento,edad,cumpleanios,fecha_carga"] + [
        f"{r['id']},{r['nombre']},{r['fecha_nacimiento']},{r['edad']},{r['cumpleanios']},{r['fecha_carga']}"
        for r in filas
    ]
    return send_file(io.BytesIO("\n".join(lineas).encode()), as_attachment=True,
                     download_name="FAMOSOS_NORM.csv", mimetype="text/csv")

@app.route("/famosos/descargar/log")
def famosos_log():
    ruta = f"{BASE_DIR}/famosos_log.txt"
    if not os.path.exists(ruta): return "Sin log", 404
    return send_file(ruta, as_attachment=True, download_name="FAMOSOS_LOG.txt")


# ── LUGARES ───────────────────────────────────────────────────────────────────

@app.route("/lugares/procesar", methods=["POST"])
def lugares_procesar():
    archivo = request.files.get("archivo")
    if not archivo:
        return jsonify({"error": "No se recibio archivo"}), 400
    lineas = archivo.read().decode("utf-8", errors="replace").splitlines()
    stats, log_preview = etl_lugares(lineas)
    return jsonify({"estadisticas": stats, "log_preview": log_preview})

@app.route("/lugares/datos")
def lugares_datos():
    with get_db() as conn:
        filas = conn.execute("""
            SELECT l.id, l.nombre,
                   g.latitud, g.longitud,
                   d.nombre_calle, d.numero_calle, d.ciudad_estado_provincia, d.pais
            FROM Lugares l
            LEFT JOIN Georeferencias g ON g.id_lugar = l.id
            LEFT JOIN Direcciones    d ON d.id_lugar = l.id
            ORDER BY l.nombre
        """).fetchall()
    return jsonify({"datos": [dict(f) for f in filas], "total": len(filas)})

@app.route("/lugares/descargar/log")
def lugares_log():
    ruta = f"{BASE_DIR}/lugares_log.txt"
    if not os.path.exists(ruta): return "Sin log", 404
    return send_file(ruta, as_attachment=True, download_name="LUGARES_LOG.txt")


# ── INICIO ────────────────────────────────────────────────────────────────────

inicializar_db()

if __name__ == "__main__":
    print("ETL Multi-Modulo iniciado en http://localhost:5000")
    app.run(debug=True, port=5000)
