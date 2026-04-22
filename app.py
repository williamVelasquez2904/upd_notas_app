# Fecha de actualización: 28 marzo 2026
# Mejoras:
# - Fix definitivo documento (sin .0)
# - dtype controlado en Excel
# - clean() robusto
# - Logs optimizados
# - Código más limpio y modular


import streamlit as st
import pandas as pd
import pymysql
from conec import get_connection
import sys
import logging
from datetime import datetime


# ==========================
# STREAMLIT LOGIN
# ==========================
def login_form():
    st.title("Login")
    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")
    login_btn = st.button("Iniciar sesión")
    if login_btn:
        # Cambia estos valores por los que desees
        if username == "admin" and password == "1234":
            st.session_state["logged_in"] = True
            st.success("¡Bienvenido!")
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos")


if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    login_form()
    st.stop()


# ==========================
# CONFIGURACIÓN
# ==========================
st.title("Carga y procesamiento de compras y ventas")


TABLE_NAME = "wh_tbl_compra"
MODO_PRUEBA = False

# Subir archivo Excel
uploaded_file = st.file_uploader("Selecciona el archivo Excel de trabajo", type=["xlsx", "xls"], key="excel_file")
if uploaded_file is not None:
    NOMBRE_ARCHIVO = uploaded_file
    st.info(f"Archivo de trabajo: {uploaded_file.name}")
else:
    st.warning("Por favor, selecciona un archivo Excel para continuar.")
    st.stop()

# ==========================
# LOGS
# ==========================
logging.basicConfig(
    filename="errores.log",
    level=logging.ERROR,
    format="%(message)s"
)

def setup_logger(nombre, archivo, level, formato="%(message)s"):
    logger = logging.getLogger(nombre)
    logger.setLevel(level)

    handler = logging.FileHandler(archivo)
    handler.setFormatter(logging.Formatter(formato))

    if not logger.handlers:
        logger.addHandler(handler)

    return logger

log_clientes = setup_logger("clientes", "clientes_no_encontrados.log", logging.INFO)
log_proveedores = setup_logger("proveedores", "proveedores_no_encontrados.log", logging.WARNING, "%(asctime)s - %(message)s")
log_duplicados = setup_logger("duplicados", "duplicados.log", logging.WARNING, "%(asctime)s - %(message)s")

# ==========================
# LIMPIEZA
# ==========================
def clean(val):
    if pd.isna(val):
        return None

    # 🔥 Manejo correcto de números
    if isinstance(val, (int, float)):
        if float(val).is_integer():
            return str(int(val))
        return str(val)

    val = str(val).strip()

    if val.isdigit():
        val = val.lstrip('0')
        if val == "":
            val = "0"

    return val

# ==========================
# PARSE FECHA (D/M/A)
# ==========================
def parse_fecha(val, fila_excel):
    if pd.isna(val):
        logging.error(f"Fila {fila_excel} - Fecha vacía")
        return None

    try:
        val_str = str(val).strip()

        # Detectar ambigüedad
        if '/' in val_str:
            partes = val_str.split('/')
            if len(partes) == 3:
                try:
                    dia = int(partes[0])
                    mes = int(partes[1])
                    if dia <= 12 and mes <= 12:
                        logging.warning(f"Fila {fila_excel} - Fecha ambigua (D/M/A asumido): {val_str}")
                except:
                    pass

        fecha = pd.to_datetime(val, dayfirst=True, errors='coerce')

        if pd.isna(fecha):
            logging.error(f"Fila {fila_excel} - Fecha inválida: {val}")
            return None

        print(
            f"DEBUG -> Fila {fila_excel} | Original: {val} "
            f"| D/M/A: {fecha.strftime('%d/%m/%Y')} "
            f"| SQL: {fecha.strftime('%Y-%m-%d')}"
        )

        return fecha.strftime('%Y-%m-%d')

    except Exception as e:
        logging.error(f"Fila {fila_excel} - Error fecha: {val} | {e}")
        return None

# ==========================
# LEER EXCEL (🔥 CLAVE)
# ==========================

# ==========================
# STREAMLIT: Botón para procesar
# ==========================
if st.button("Procesar archivo y cargar a BD"):
    try:
        df = pd.read_excel(
            NOMBRE_ARCHIVO,
            dtype={1: str}  # 🔥 DOCUMENTO SIEMPRE STRING
        )
    except Exception as e:
        logging.error(f"Error al leer archivo: {e}")
        st.error(f"Error al leer archivo: {e}")
        st.stop()


    # Mapear columnas
    df['FECHA']     = df.iloc[:, 0]  # Columna A (índice 0)
    df['DOCUMENTO'] = df.iloc[:, 1]     # Columna B (índice 1) - dtype controlado
    df['CODIGO']    = df.iloc[:, 2]     # Columna C (índice 2)
    df['CLIENTE']   = df.iloc[:, 3]     # Columna D (índice 3)
    df['TOTAL']     = df.iloc[:, 4]     # Columna E (índice 4)
    df['DESCUENTO'] = df.iloc[:, 5]     # Columna F (índice 5)
    df['PORC_ASIG'] = df.iloc[:, 7]     # Columna H (índice 7)
    df['PROVEEDOR'] = df.iloc[:, 8]     # Columna I (índice 8)
    df['CONDICION'] = df.iloc[:, 10]    # Columna K (índice 10)

    # ==========================
    # CONEXIÓN BD
    # ==========================
    try:
        connection = get_connection()
    except Exception as e:
        logging.error(f"Error conexión BD: {e}")
        st.error(f"Error conexión BD: {e}")
        st.stop()

    try:
        with connection.cursor() as cursor:
            # ==========================
            # CACHE BD
            # ==========================
            cursor.execute("SELECT clien_nombre1, clien_ide, clien_vendedor FROM tbl_cliente")
            clientes_dict = {
                str(row[0]).strip().lower(): (row[1], row[2])
                for row in cursor.fetchall()
            }

            cursor.execute("SELECT prove_alias, prove_ide FROM tbl_proveedor")
            proveedores_dict = {
                str(row[0]).strip().lower(): row[1]
                for row in cursor.fetchall()
            }

            # ==========================
            # PROCESAMIENTO
            # ==========================
            for idx, row in df.iterrows():
                fila_excel = idx + 2
                try:
                    fecha         = parse_fecha(row['FECHA'], fila_excel)
                    documento     = clean(row['DOCUMENTO'])
                    cliente_nom   = clean(row['CLIENTE'])
                    proveedor_nom = clean(row['PROVEEDOR'])
                    porc_asig     = clean(row['PORC_ASIG'])

                    if not fecha:
                        continue

                    total = round(float(row['TOTAL']), 2) if pd.notna(row['TOTAL']) else None
                    descuento = float(row['DESCUENTO']) if pd.notna(row['DESCUENTO']) else None

                    # ==========================
                    # CONDICIÓN
                    # ==========================
                    condicion = None
                    cond = clean(row['CONDICION'])

                    if cond:
                        cond = cond.lower()
                        if cond == 'contado':
                            condicion = 0
                        elif cond == 'credito':
                            condicion = 1

                    # ==========================
                    # CLIENTE
                    # ==========================
                    cliente_key = cliente_nom.lower() if cliente_nom else None

                    if cliente_key not in clientes_dict:
                        log_clientes.info(f"Fila {fila_excel} - Cliente no encontrado: {cliente_nom}")
                        continue

                    cliente_ide, vende_ide = clientes_dict[cliente_key]

                    # ==========================
                    # PROVEEDOR
                    # ==========================
                    proveedor_key = proveedor_nom.lower() if proveedor_nom else None

                    if proveedor_key not in proveedores_dict:
                        log_proveedores.warning(f"Fila {fila_excel} - Proveedor no encontrado: {proveedor_nom}")
                        continue

                    proveedor = proveedores_dict[proveedor_key]

                    # ==========================
                    # VALIDAR DUPLICADO COMPRA
                    # ==========================
                    cursor.execute(
                        f"SELECT compra_ide FROM {TABLE_NAME} WHERE compra_num = %s",
                        (documento,)
                    )
                    if cursor.fetchone():
                        log_duplicados.warning(f"Fila {fila_excel} - Compra duplicada: {documento}")
                        continue

                    fecha_registro = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # ==========================
                    # INSERT COMPRA
                    # ==========================
                    if not MODO_PRUEBA:
                        cursor.execute(f"""
                            INSERT INTO {TABLE_NAME}
                            (compra_fecha, compra_prov_ide, compra_num, compra_clien_ide,
                             compra_monto, compra_porc_desc, compra_tipo, compra_tienda,
                             compra_condicion, compra_fecha_registro)
                            VALUES (%s, %s, %s, %s, %s, %s, 1, 1, %s, %s)
                        """, (
                            fecha, proveedor, documento, cliente_ide,
                            total, descuento, condicion, fecha_registro
                        ))

                        compra_ide = cursor.lastrowid
                    else:
                        compra_ide = 0

                    # ==========================
                    # VALIDAR DUPLICADO VENTA
                    # ==========================
                    cursor.execute(
                        "SELECT venta_ide FROM wh_tbl_venta WHERE venta_num = %s",
                        (documento,)
                    )
                    if cursor.fetchone():
                        log_duplicados.warning(f"Fila {fila_excel} - Venta duplicada: {documento}")
                        continue

                    # ==========================
                    # INSERT VENTA
                    # ==========================
                    if not MODO_PRUEBA:
                        cursor.execute("""
                            INSERT INTO wh_tbl_venta
                            (venta_origen_ide, venta_fecha, venta_num, venta_clien_ide,
                             venta_monto, venta_porc_desc,venta_porc_asig, venta_tipo, venta_tienda,
                             venta_compra_ide, venta_fecha_registro, venta_vende_ide, venta_condicion)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 1, %s, %s, %s, %s)
                        """, (
                            1, fecha, documento, cliente_ide,
                            total, descuento, porc_asig, compra_ide,
                            fecha_registro, vende_ide, condicion
                        ))

                    st.write(f"OK -> Fila {fila_excel} | Doc: {documento}")

                except Exception as e:
                    logging.error(f"Fila {fila_excel} - Error: {e}")
                    st.warning(f"Fila {fila_excel} - Error: {e}")
                    continue

        if not MODO_PRUEBA:
            connection.commit()
            st.success("¡Procesamiento y carga completados!")
        else:
            st.info("Modo prueba activo")

    except Exception as e:
        logging.error(f"Error general: {e}")
        connection.rollback()
        st.error(f"Error general: {e}")

    finally:
        connection.close()