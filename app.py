"""
ETL MULTI-MODULO - Arquitectura y Almacenamiento de Datos
INACAP Concepcion - Evaluacion 2 (Parte 1 y Parte 2)
Autora: Javiera Francisca Alarcon Albornoz

Modulos:
  1. COMUNAS_NORM  — normalizacion de comunas chilenas
  2. FAMOSOS_NORM  — normalizacion de fechas, calculo de edad y flag cumpleanos
  3. LUGARES_NORM  — normalizacion y separacion en tres tablas relacionadas
"""

import os
import re
import io
import sqlite3
import unicodedata
from datetime import datetime, date
from flask import Flask, render_template, request, jsonify, send_file

app = Flask(__name__)

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
BASE_DIR = "/tmp/etl_norm"
DB_PATH  = f"{BASE_DIR}/etl_norm.db"
os.makedirs(BASE_DIR, exist_ok=True)

HOY = date.today()


# ══════════════════════════════════════════════════════════════════════════════
# BASE DE DATOS
# ══════════════════════════════════════════════════════════════════════════════

def get_db():
    """Abre conexion SQLite con acceso por nombre de columna."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_db():
    """
    Crea todas las tablas si no existen.
    Tablas:
      COMUNAS_NORM   — id, nombre, fecha_carga
      FAMOSOS_NORM   — id, nombre, fecha_nacimiento, edad, cumpleanios, fecha_carga
      Lugares        — id, nombre
      Georeferencias — id, id_lugar, latitud, longitud
      Direcciones    — id, id_lugar, nombre_calle, numero_calle, ciudad_estado_provincia, pais
    """
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS COMUNAS_NORM (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre      TEXT    NOT NULL UNIQUE,
                fecha_carga TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS FAMOSOS_NORM (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre           TEXT    NOT NULL UNIQUE,
                fecha_nacimiento TEXT    NOT NULL,
                edad             INTEGER,
                cumpleanios      INTEGER NOT NULL DEFAULT 0,
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
# MODULO 1 — COMUNAS_NORM
# ══════════════════════════════════════════════════════════════════════════════

def quitar_tildes(texto):
    """Elimina diacriticos usando descomposicion Unicode NFD y reemplaza n con tilde."""
    nfd = unicodedata.normalize("NFD", texto)
    sin_tildes = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    sin_tildes = sin_tildes.replace("ñ", "n").replace("Ñ", "N")
    return sin_tildes


def limpiar_comuna(texto):
    """Limpia una comuna: elimina espacios extra, tildes y aplica formato Titulo."""
    limpio = re.sub(r"\s+", " ", texto.strip())
    limpio = quitar_tildes(limpio)
    return limpio.title()


def etl_comunas(lineas):
    """
    ETL completo para comunas:
      - Limpia y normaliza cada registro
      - Deduplica (case-insensitive post-normalizacion)
      - Registra cada accion en el log
      - Carga resultado en tabla COMUNAS_NORM
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = ["=" * 70, "COMUNAS_NORM — LOG ETL", f"Fecha: {timestamp}", "=" * 70, ""]
    vistas = {}
    resultado = []
    entrada = modif = duplic = vacios = 0

    for i, linea in enumerate(lineas, 1):
        original = linea.rstrip("\r\n")
        if not original.strip():
            vacios += 1
            log.append(f"[{i:05d}] VACIA      | omitida")
            continue
        entrada += 1
        norm = limpiar_comuna(original)
        clave = norm.lower()
        if clave in vistas:
            duplic += 1
            log.append(f"[{i:05d}] DUPLICADO  | '{original}' ya existe como '{vistas[clave]}'")
            continue
        vistas[clave] = norm
        resultado.append(norm)
        if original != norm:
            modif += 1
            log.append(f"[{i:05d}] MODIFICADO | '{original}' -> '{norm}'")
        else:
            log.append(f"[{i:05d}] SIN CAMBIO | '{original}'")

    log += ["", "=" * 70, "RESUMEN",
            f"Entrada: {entrada} | Modificados: {modif} | Duplicados: {duplic} | Resultado: {len(resultado)}",
            "=" * 70]

    with get_db() as conn:
        conn.execute("DELETE FROM COMUNAS_NORM")
        conn.executemany(
            "INSERT OR IGNORE INTO COMUNAS_NORM (nombre, fecha_carga) VALUES (?,?)",
            [(n, timestamp) for n in resultado]
        )
        conn.commit()

    with open(f"{BASE_DIR}/comunas_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    return {"entrada": entrada, "modificados": modif, "duplicados": duplic, "resultado": len(resultado)}, log[:30]


# ══════════════════════════════════════════════════════════════════════════════
# MODULO 2 — FAMOSOS_NORM
# ══════════════════════════════════════════════════════════════════════════════

def parsear_fecha(texto_fecha):
    """
    Convierte una fecha a formato chileno DD-MM-YYYY.
    Soporta: YYYY/MM/DD, YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY.
    Retorna (fecha_normalizada, anio) o (None, None) si no es parseable.
    """
    texto = texto_fecha.strip()
    # YYYY/MM/DD o YYYY-MM-DD
    m = re.match(r'^(\d{4})[/-](\d{2})[/-](\d{2})$', texto)
    if m:
        anio, mes, dia = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{dia:02d}-{mes:02d}-{anio}", anio
    # DD/MM/YYYY o DD-MM-YYYY
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
    """Retorna 1 si hoy coincide con dia y mes de la fecha, 0 si no."""
    try:
        dia, mes, _ = map(int, fecha_str.split("-"))
        return 1 if (HOY.day == dia and HOY.month == mes) else 0
    except Exception:
        return 0


def etl_famosos(lineas):
    """
    ETL para famosos:
      - Elimina numero de linea al inicio
      - Parsea y unifica fecha a DD-MM-YYYY
      - Descarta registros con fechas aproximadas o a.C.
      - Calcula edad y flag de cumpleanos
      - Deduplica por nombre
      - Carga en tabla FAMOSOS_NORM
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = ["=" * 70, "FAMOSOS_NORM — LOG ETL", f"Fecha: {timestamp}", "=" * 70, ""]
    vistas = {}
    resultado = []
    entrada = modif = duplic = descartados = 0

    for i, linea in enumerate(lineas, 1):
        original = linea.rstrip("\r\n").strip()
        if not original:
            continue

        # Eliminar numero al inicio: "1. ", "42. ", etc.
        limpio = re.sub(r'^\d+\.\s*', '', original)

        # Separar nombre y fecha por ultimo ' - '
        partes = limpio.rsplit(' - ', 1)
        if len(partes) != 2:
            descartados += 1
            log.append(f"[{i:05d}] NO PARSEABLE | '{original}'")
            continue

        nombre = partes[0].strip()
        texto_fecha = partes[1].strip()

        # Descartar fechas aproximadas o a.C.
        if 'alrededor' in texto_fecha.lower() or 'a.c.' in texto_fecha.lower() or 'a.C.' in texto_fecha:
            descartados += 1
            log.append(f"[{i:05d}] DESCARTADO  | '{nombre}' — fecha no procesable: '{texto_fecha}'")
            continue

        # Parsear fecha
        fecha_norm, anio = parsear_fecha(texto_fecha)
        if not fecha_norm:
            descartados += 1
            log.append(f"[{i:05d}] DESCARTADO  | '{nombre}' — formato desconocido: '{texto_fecha}'")
            continue

        # Deduplicar por nombre
        clave = nombre.lower()
        if clave in vistas:
            duplic += 1
            log.append(f"[{i:05d}] DUPLICADO   | '{nombre}' omitido")
            continue

        entrada += 1
        edad = calcular_edad(fecha_norm)
        cumple = es_cumpleanios(fecha_norm)
        vistas[clave] = nombre
        resultado.append((nombre, fecha_norm, edad, cumple, timestamp))

        if texto_fecha != fecha_norm:
            modif += 1
            log.append(f"[{i:05d}] MODIFICADO  | '{nombre}' | '{texto_fecha}' -> '{fecha_norm}' | Edad: {edad} | Cumple: {cumple}")
        else:
            log.append(f"[{i:05d}] SIN CAMBIO  | '{nombre}' | '{fecha_norm}' | Edad: {edad} | Cumple: {cumple}")

    log += ["", "=" * 70, "RESUMEN",
            f"Procesados: {entrada} | Modificados: {modif} | Duplicados: {duplic} | Descartados: {descartados}",
            "=" * 70]

    with get_db() as conn:
        conn.execute("DELETE FROM FAMOSOS_NORM")
        conn.executemany(
            "INSERT OR IGNORE INTO FAMOSOS_NORM (nombre, fecha_nacimiento, edad, cumpleanios, fecha_carga) VALUES (?,?,?,?,?)",
            resultado
        )
        conn.commit()

    with open(f"{BASE_DIR}/famosos_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    return {"entrada": entrada, "modificados": modif, "duplicados": duplic,
            "descartados": descartados, "resultado": len(resultado)}, log[:35]


# ══════════════════════════════════════════════════════════════════════════════
# MODULO 3 — LUGARES_NORM
# ══════════════════════════════════════════════════════════════════════════════

def parsear_direccion(direccion_raw):
    """
    Separa una direccion en componentes:
      nombre_calle, numero_calle, ciudad_estado_provincia, pais
    Estrategia:
      - El pais es el ultimo segmento separado por coma
      - La ciudad/estado/provincia es el penultimo
      - El numero de calle es el primer token si es numerico
    """
    if not direccion_raw or not direccion_raw.strip():
        return "", "", "", ""
    partes = [p.strip() for p in direccion_raw.split(",")]
    pais          = partes[-1] if len(partes) >= 1 else ""
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
    """Cuenta segmentos no vacios en una direccion (para elegir la mas completa)."""
    return sum(1 for c in direccion.split(",") if c.strip())


def etl_lugares(lineas):
    """
    ETL para lugares:
      - Parsea CSV separado por ;
      - Limpia caracteres corruptos (encoding)
      - Deduplica por nombre conservando la direccion mas completa
      - Separa georeferencia en latitud y longitud
      - Carga en tres tablas: Lugares, Georeferencias, Direcciones
    """
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

    # Deduplicar conservando la direccion mas completa
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
                log.append(f"REEMPLAZADO | '{nombre}' — direccion mas completa encontrada")
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
                log.append(f"  GEOREF ERROR | '{nombre}' no parseable: '{georef}'")

            nombre_calle, numero_calle, ciudad_estado, pais = parsear_direccion(direcc)
            conn.execute(
                "INSERT INTO Direcciones (id_lugar, nombre_calle, numero_calle, ciudad_estado_provincia, pais) VALUES (?,?,?,?,?)",
                (id_lugar, nombre_calle, numero_calle, ciudad_estado, pais)
            )

        conn.commit()

    total = len(mejores)
    duplicados = len(datos_raw) - total
    log += ["", "=" * 70, "RESUMEN",
            f"Entrada: {len(datos_raw)} | Duplicados eliminados: {duplicados} | Cargados en BD: {total}",
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

# Modulo 1
@app.route("/comunas/procesar", methods=["POST"])
def comunas_procesar():
    archivo = request.files.get("archivo")
    if not archivo:
        return jsonify({"error": "No se recibio archivo"}), 400
    lineas = archivo.read().decode("utf-8", errors="replace").splitlines()
    stats, log_preview = etl_comunas(lineas)
    return jsonify({"estadisticas": stats, "log_preview": log_preview})

@app.route("/comunas/datos")
def comunas_datos():
    with get_db() as conn:
        filas = conn.execute("SELECT id, nombre, fecha_carga FROM COMUNAS_NORM ORDER BY nombre").fetchall()
    return jsonify({"datos": [dict(f) for f in filas], "total": len(filas)})

@app.route("/comunas/descargar/csv")
def comunas_csv():
    with get_db() as conn:
        filas = conn.execute("SELECT id, nombre, fecha_carga FROM COMUNAS_NORM ORDER BY nombre").fetchall()
    lineas = ["id,nombre,fecha_carga"] + [f"{r['id']},{r['nombre']},{r['fecha_carga']}" for r in filas]
    return send_file(io.BytesIO("\n".join(lineas).encode()), as_attachment=True,
                     download_name="COMUNAS_NORM.csv", mimetype="text/csv")

@app.route("/comunas/descargar/log")
def comunas_log():
    ruta = f"{BASE_DIR}/comunas_log.txt"
    if not os.path.exists(ruta): return "Sin log", 404
    return send_file(ruta, as_attachment=True, download_name="COMUNAS_LOG.txt")

# Modulo 2
@app.route("/famosos/procesar", methods=["POST"])
def famosos_procesar():
    archivo = request.files.get("archivo")
    if not archivo:
        return jsonify({"error": "No se recibio archivo"}), 400
    lineas = archivo.read().decode("utf-8", errors="replace").splitlines()
    stats, log_preview = etl_famosos(lineas)
    return jsonify({"estadisticas": stats, "log_preview": log_preview})

@app.route("/famosos/datos")
def famosos_datos():
    with get_db() as conn:
        filas = conn.execute(
            "SELECT id, nombre, fecha_nacimiento, edad, cumpleanios, fecha_carga FROM FAMOSOS_NORM ORDER BY nombre"
        ).fetchall()
    return jsonify({"datos": [dict(f) for f in filas], "total": len(filas)})

@app.route("/famosos/descargar/csv")
def famosos_csv():
    with get_db() as conn:
        filas = conn.execute("SELECT * FROM FAMOSOS_NORM ORDER BY nombre").fetchall()
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

# Modulo 3
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
