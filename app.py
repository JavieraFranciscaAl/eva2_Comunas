"""
COMUNAS_NORM - Aplicacion ETL de Normalizacion de Datos
Arquitectura y Almacenamiento de Datos - INACAP Concepcion
Autora: Javiera Francisca Alarcon Albornoz

Pipeline completo:
  1. Carga del archivo CSV/TXT desde la interfaz web
  2. Normalizacion (unificar formato, quitar tildes, eliminar duplicados)
  3. Carga de registros limpios en base de datos SQLite (tabla COMUNAS_NORM)
  4. Exposicion de los datos desde la BD via endpoint REST
  5. Log de cambios descargable
"""

import os
import re
import io
import sqlite3
import unicodedata
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file

app = Flask(__name__)

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
BASE_DIR = "/tmp/comunas_norm"
DB_PATH  = f"{BASE_DIR}/comunas_norm.db"
LOG_PATH = f"{BASE_DIR}/COMUNAS_NORM_LOG.txt"

os.makedirs(BASE_DIR, exist_ok=True)


# ── BASE DE DATOS ─────────────────────────────────────────────────────────────

def get_db():
    """
    Abre conexion a SQLite. row_factory permite leer columnas por nombre.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_db():
    """
    Crea la tabla COMUNAS_NORM si no existe.
    Esquema:
        id          INTEGER  clave primaria autoincremental
        nombre      TEXT     nombre normalizado (restriccion UNIQUE)
        fecha_carga TEXT     timestamp del proceso ETL
    """
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS COMUNAS_NORM (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre      TEXT    NOT NULL UNIQUE,
                fecha_carga TEXT    NOT NULL
            )
        """)
        conn.commit()


def vaciar_tabla():
    """
    Borra todos los registros antes de cada nueva carga.
    Permite reprocesar distintos datasets sin acumular datos de sesiones anteriores.
    """
    with get_db() as conn:
        conn.execute("DELETE FROM COMUNAS_NORM")
        conn.commit()


def insertar_comunas(comunas, timestamp):
    """
    Inserta la lista de comunas normalizadas en COMUNAS_NORM.
    INSERT OR IGNORE descarta silenciosamente cualquier duplicado residual.
    """
    with get_db() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO COMUNAS_NORM (nombre, fecha_carga) VALUES (?, ?)",
            [(nombre, timestamp) for nombre in comunas]
        )
        conn.commit()


def leer_comunas_bd():
    """
    Retorna todos los registros de COMUNAS_NORM ordenados alfabeticamente.
    """
    with get_db() as conn:
        filas = conn.execute(
            "SELECT id, nombre, fecha_carga FROM COMUNAS_NORM ORDER BY nombre ASC"
        ).fetchall()
    return [dict(f) for f in filas]


# ── NORMALIZACION ─────────────────────────────────────────────────────────────

def quitar_tildes(texto):
    """
    Elimina diacriticos (tildes) usando descomposicion Unicode NFD.
    Reemplaza explicitamente n/N con tilde (que NFD no elimina sola).
    Ejemplos: 'Concepcion' <- 'Concepción' | 'Niquen' <- 'Ñiquén'
    """
    nfd = unicodedata.normalize("NFD", texto)
    sin_tildes = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    sin_tildes = sin_tildes.replace("n\u0303", "n").replace("N\u0303", "N")
    sin_tildes = sin_tildes.replace("ñ", "n").replace("Ñ", "N")
    return sin_tildes


def limpiar_texto(texto):
    """
    Pipeline de limpieza por registro:
      1. strip + colapso de espacios multiples
      2. Eliminacion de tildes y caracteres especiales
      3. Conversion a formato Titulo
    Ejemplos:
      '  los angeles  ' → 'Los Angeles'
      'CONCEPCIÓN'      → 'Concepcion'
      'chillán viejo'   → 'Chillan Viejo'
    """
    limpio = re.sub(r"\s+", " ", texto.strip())
    limpio = quitar_tildes(limpio)
    limpio = limpio.title()
    return limpio


def normalizar_dataset(lineas):
    """
    ETL principal. Procesa todas las lineas del archivo y retorna:
      resultado    : lista de comunas limpias y unicas
      log_entries  : lineas del log de auditoria
      estadisticas : contadores del proceso
      timestamp    : marca de tiempo del proceso
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_entries = []
    log_entries.append("=" * 70)
    log_entries.append("COMUNAS_NORM — LOG DE NORMALIZACION ETL")
    log_entries.append(f"Fecha y hora : {timestamp}")
    log_entries.append(f"Motor BD     : SQLite  |  Tabla: COMUNAS_NORM")
    log_entries.append("=" * 70)
    log_entries.append("")

    comunas_vistas = {}
    resultado = []
    total_entrada = total_modif = total_duplic = total_vacios = 0

    for i, linea in enumerate(lineas, start=1):
        original = linea.rstrip("\r\n")

        if not original.strip():
            total_vacios += 1
            log_entries.append(f"[{i:05d}] VACIA      | omitida")
            continue

        total_entrada += 1
        normalizado = limpiar_texto(original)
        clave = normalizado.lower()

        if clave in comunas_vistas:
            total_duplic += 1
            log_entries.append(
                f"[{i:05d}] DUPLICADO  | '{original}'"
                f" → ya existe como '{comunas_vistas[clave]}' — eliminado"
            )
            continue

        comunas_vistas[clave] = normalizado
        resultado.append(normalizado)

        if original != normalizado:
            total_modif += 1
            log_entries.append(f"[{i:05d}] MODIFICADO | '{original}' → '{normalizado}'")
        else:
            log_entries.append(f"[{i:05d}] SIN CAMBIO | '{original}'")

    log_entries.append("")
    log_entries.append("=" * 70)
    log_entries.append("RESUMEN")
    log_entries.append("=" * 70)
    log_entries.append(f"Registros de entrada      : {total_entrada}")
    log_entries.append(f"Registros modificados     : {total_modif}")
    log_entries.append(f"Duplicados eliminados     : {total_duplic}")
    log_entries.append(f"Lineas vacias omitidas    : {total_vacios}")
    log_entries.append(f"Registros cargados en BD  : {len(resultado)}")
    log_entries.append("=" * 70)

    estadisticas = {
        "entrada":     total_entrada,
        "modificados": total_modif,
        "duplicados":  total_duplic,
        "vacios":      total_vacios,
        "resultado":   len(resultado),
        "timestamp":   timestamp,
    }

    return resultado, log_entries, estadisticas, timestamp


# ── RUTAS FLASK ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/procesar", methods=["POST"])
def procesar():
    """
    Recibe el archivo, ejecuta el ETL y carga los datos en SQLite.
    Flujo: leer archivo → normalizar → vaciar tabla → insertar → guardar log → responder JSON
    """
    if "archivo" not in request.files:
        return jsonify({"error": "No se recibio ningun archivo."}), 400

    archivo = request.files["archivo"]
    if archivo.filename == "":
        return jsonify({"error": "Nombre de archivo vacio."}), 400

    try:
        contenido = archivo.read().decode("utf-8", errors="replace")
    except Exception as e:
        return jsonify({"error": f"Error al leer el archivo: {str(e)}"}), 500

    lineas = contenido.splitlines()
    resultado, log_entries, estadisticas, timestamp = normalizar_dataset(lineas)

    # Cargar en SQLite
    vaciar_tabla()
    insertar_comunas(resultado, timestamp)

    # Guardar log
    with open(LOG_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(log_entries))

    return jsonify({
        "estadisticas": estadisticas,
        "log_preview":  log_entries[:25],
    })


@app.route("/comunas")
def comunas():
    """
    Lee los registros desde SQLite y los retorna como JSON.
    La interfaz consume este endpoint para mostrar la tabla completa.
    """
    datos = leer_comunas_bd()
    return jsonify({"comunas": datos, "total": len(datos)})


@app.route("/descargar/csv")
def descargar_csv():
    """
    Exporta los datos DESDE la base de datos (no desde el archivo original)
    como CSV descargable. Demuestra que la fuente es SQLite.
    """
    datos = leer_comunas_bd()
    if not datos:
        return "Sin datos. Procese un dataset primero.", 404
    lineas = ["id,nombre,fecha_carga"] + [
        f"{f['id']},{f['nombre']},{f['fecha_carga']}" for f in datos
    ]
    return send_file(
        io.BytesIO("\n".join(lineas).encode("utf-8")),
        as_attachment=True,
        download_name="COMUNAS_NORM.csv",
        mimetype="text/csv"
    )


@app.route("/descargar/log")
def descargar_log():
    if not os.path.exists(LOG_PATH):
        return "Log no disponible. Procese un dataset primero.", 404
    return send_file(LOG_PATH, as_attachment=True, download_name="COMUNAS_NORM_LOG.txt")


# ── INICIO ────────────────────────────────────────────────────────────────────

inicializar_db()

if __name__ == "__main__":
    print("COMUNAS_NORM iniciado en http://localhost:5000")
    app.run(debug=True, port=5000)
