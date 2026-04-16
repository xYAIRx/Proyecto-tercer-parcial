"""
Módulo de Exportación e Importación de datos.
Soporta CSV, JSON y SQL.
Incluye importación rápida de bases de datos completas (.sql).
"""

import csv
import json
import os
import subprocess
import threading
import flet as ft
from db_connection import DBConnection


def build_export_import_view(page: ft.Page):
    db = DBConnection.get_instance()

    log_text = ft.Text("", selectable=True, size=12, color=ft.Colors.AMBER_200)
    log_container = ft.Container(
        content=ft.Column([log_text], scroll=ft.ScrollMode.AUTO),
        bgcolor=ft.Colors.with_opacity(0.3, ft.Colors.BLACK),
        border_radius=10,
        padding=15,
        height=180,
    )

    def log(msg):
        log_text.value = (log_text.value or "") + f"\n{msg}"
        page.update()

    def _read_file(path):
        """Lee un archivo intentando múltiples encodings."""
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                with open(path, "r", encoding=enc) as f:
                    return f.read()
            except (UnicodeDecodeError, UnicodeError):
                continue
        raise RuntimeError("No se pudo leer el archivo con ningún encoding soportado.")

    # ================================================================
    # IMPORTACIÓN RÁPIDA DE BASE DE DATOS COMPLETA
    # ================================================================
    quick_import_path = ft.TextField(
        label="Archivo .sql ", width=500,
        hint_text="Selecciona el archivo .sql que quieres importar",
        border_color=ft.Colors.LIGHT_BLUE_400,
        focused_border_color=ft.Colors.LIGHT_BLUE_200,
        prefix_icon=ft.Icons.FILE_PRESENT,
    )
    quick_import_db_name = ft.TextField(
        label="Nombre de la base de datos (opcional)",
        width=350,
        hint_text="Se usa el nombre del archivo si lo dejas vacío",
        border_color=ft.Colors.LIGHT_BLUE_400,
        focused_border_color=ft.Colors.LIGHT_BLUE_200,
        prefix_icon=ft.Icons.DATASET,
    )
    quick_import_progress = ft.ProgressBar(visible=False, color=ft.Colors.LIGHT_BLUE_400)
    quick_import_status = ft.Text("", size=13)

    def pick_quick_import_file(e: ft.FilePickerResultEvent):
        if e.files:
            quick_import_path.value = e.files[0].path
            filename = os.path.splitext(os.path.basename(e.files[0].path))[0]
            clean_name = "".join(c if c.isalnum() or c == "_" else "_" for c in filename)
            quick_import_db_name.value = clean_name
            page.update()

    quick_import_picker = ft.FilePicker(on_result=pick_quick_import_file)
    page.overlay.append(quick_import_picker)

    def do_quick_import(e):
        path = quick_import_path.value
        if not path or not os.path.exists(path):
            log("Selecciona un archivo .sql válido.")
            return

        db_name = quick_import_db_name.value
        if not db_name:
            db_name = os.path.splitext(os.path.basename(path))[0]
            db_name = "".join(c if c.isalnum() or c == "_" else "_" for c in db_name)

        quick_import_progress.visible = True
        quick_import_status.value = "Importando base de datos..."
        quick_import_status.color = ft.Colors.LIGHT_BLUE_200
        page.update()

        def run_import():
            try:
                config = db._config

                # Crear la base de datos si no existe
                log(f"Creando base de datos '{db_name}' si no existe...")
                try:
                    db.execute_query(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
                except Exception as ex:
                    log(f"Nota al crear BD: {ex}")

                # Revisar si el archivo ya define su propia BD
                content_preview = ""
                for enc in ("utf-8-sig", "utf-8", "latin-1"):
                    try:
                        with open(path, "r", encoding=enc) as f:
                            content_preview = f.read(10000)
                        break
                    except (UnicodeDecodeError, UnicodeError):
                        continue

                has_create_db = "CREATE DATABASE" in content_preview.upper()

                # Importar usando mysql CLI
                log("Ejecutando importación SQL...")
                cmd = [
                    "mysql",
                    f"--host={config['host']}",
                    f"--port={config['port']}",
                    f"--user={config['user']}",
                    f"--password={config['password']}",
                    "--force",
                ]
                if not has_create_db:
                    cmd.append(db_name)

                with open(path, "r", encoding="latin-1") as f:
                    result = subprocess.run(
                        cmd, stdin=f, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, text=True, timeout=600
                    )

                # Procesar resultado
                file_size = os.path.getsize(path)
                size_str = _format_size(file_size)
                stderr = result.stderr.strip() if result.stderr else ""

                # Filtrar warnings de contraseña (son inofensivos)
                error_lines = []
                warning_count = 0
                for line in stderr.splitlines():
                    if "using a password on the command line" in line.lower():
                        continue
                    if "warning" in line.lower():
                        warning_count += 1
                        continue
                    if line.strip():
                        error_lines.append(line.strip())

                if result.returncode == 0 and not error_lines:
                    quick_import_status.value = "Importación completada"
                    quick_import_status.color = ft.Colors.GREEN_300
                    log(f"Base de datos '{db_name}' importada correctamente ({size_str})")
                    if warning_count > 0:
                        log(f"{warning_count} advertencias (no afectan la importación)")
                    refresh_dbs()
                elif error_lines:
                    # Con --force, puede haber errores parciales pero el resto se importa
                    real_errors = [l for l in error_lines if "ERROR" in l.upper()]
                    if len(real_errors) <= 3:
                        for err in real_errors:
                            log(f"Error parcial: {err[:150]}")
                    else:
                        for err in real_errors[:3]:
                            log(f"Error parcial: {err[:150]}")
                        log(f"...y {len(real_errors) - 3} errores más")

                    if result.returncode == 0:
                        quick_import_status.value = "Importado con advertencias"
                        quick_import_status.color = ft.Colors.AMBER_300
                        log(f"Importación parcial de '{db_name}' completada ({size_str})")
                    else:
                        quick_import_status.value = "Importado con algunos errores"
                        quick_import_status.color = ft.Colors.AMBER_300
                        log(f"Importación de '{db_name}' terminó con {len(real_errors)} errores")
                        log("Tip: Si el archivo solo contiene datos (INSERT), necesitas importar primero el archivo de estructura (CREATE TABLE).")
                    refresh_dbs()
                else:
                    quick_import_status.value = "Error al importar"
                    quick_import_status.color = ft.Colors.RED_300
                    log(f"No se pudo importar: {stderr[:300]}")

            except FileNotFoundError:
                log("mysql CLI no encontrado en el sistema, usando método alternativo...")
                _import_sql_python(path, db_name)
            except subprocess.TimeoutExpired:
                log("La importación tardó demasiado (más de 10 minutos).")
                quick_import_status.value = "Timeout"
                quick_import_status.color = ft.Colors.RED_300
            except Exception as ex:
                log(f"Error inesperado: {ex}")
                quick_import_status.value = "Error"
                quick_import_status.color = ft.Colors.RED_300
            finally:
                quick_import_progress.visible = False
                page.update()

        threading.Thread(target=run_import, daemon=True).start()

    def _import_sql_python(path, db_name):
        """Método alternativo cuando mysql CLI no está disponible."""
        try:
            content = _read_file(path)
            statements = _split_sql_statements(content)
            if not statements:
                log("No se encontraron sentencias SQL válidas.")
                return

            conn = db.get_connection()
            cursor = conn.cursor()

            try:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            except Exception:
                pass
            cursor.execute(f"USE `{db_name}`")

            executed = 0
            errors = 0
            total = len(statements)
            for i, stmt in enumerate(statements):
                try:
                    cursor.execute(stmt)
                    if cursor.with_rows:
                        cursor.fetchall()
                    executed += 1
                except Exception as stmt_ex:
                    errors += 1
                    if errors <= 5:
                        log(f"Error en sentencia {i+1}: {str(stmt_ex)[:100]}")

                if (i + 1) % 100 == 0:
                    quick_import_status.value = f"Progreso: {i+1}/{total} sentencias..."
                    page.update()

            conn.commit()
            cursor.close()

            msg = f"Importación completada: {executed} sentencias ejecutadas"
            if errors > 0:
                msg += f" ({errors} con error)"
            log(msg)
            quick_import_status.value = f"Importado ({executed} sentencias)"
            quick_import_status.color = ft.Colors.GREEN_300
            refresh_dbs()
        except Exception as ex:
            log(f"Error en importación alternativa: {ex}")

    def _split_sql_statements(sql_text):
        """Divide un archivo SQL en statements individuales."""
        statements = []
        current = []
        in_block_comment = False

        for line in sql_text.splitlines():
            stripped = line.strip()

            if in_block_comment:
                if "*/" in stripped:
                    in_block_comment = False
                continue
            if stripped.startswith("/*") and "*/" not in stripped:
                in_block_comment = True
                continue

            if not stripped:
                continue
            if stripped.startswith("--"):
                continue
            if stripped.startswith("/*") and stripped.endswith("*/"):
                continue
            if stripped.upper().startswith("DELIMITER"):
                continue

            current.append(line)
            if stripped.endswith(";"):
                stmt = "\n".join(current).strip()
                if stmt and stmt != ";":
                    statements.append(stmt)
                current = []

        if current:
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)
        return statements

    def _format_size(b):
        for unit in ["B", "KB", "MB", "GB"]:
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} TB"

    # ================================================================
    # EXPORTACIÓN COMPLETA DE BASE DE DATOS (mysqldump)
    # ================================================================
    full_export_db = ft.Dropdown(
        label="Base de datos", width=300,
        border_color=ft.Colors.ORANGE_400,
        focused_border_color=ft.Colors.ORANGE_200,
    )
    full_export_path = ft.TextField(
        label="Ruta de exportación (.sql)",
        width=500,
        value=os.path.join(os.path.expanduser("~"), "full_export.sql"),
        border_color=ft.Colors.ORANGE_400,
        focused_border_color=ft.Colors.ORANGE_200,
        prefix_icon=ft.Icons.FILE_PRESENT,
    )
    full_export_progress = ft.ProgressBar(visible=False, color=ft.Colors.ORANGE_400)
    full_export_status = ft.Text("", size=13)
    full_export_include_structure = ft.Checkbox(
        label="Incluir estructura (CREATE TABLE)", value=True,
        check_color=ft.Colors.WHITE, active_color=ft.Colors.ORANGE_400,
    )
    full_export_include_data = ft.Checkbox(
        label="Incluir datos (INSERT INTO)", value=True,
        check_color=ft.Colors.WHITE, active_color=ft.Colors.ORANGE_400,
    )
    full_export_include_routines = ft.Checkbox(
        label="Incluir rutinas, triggers y eventos", value=True,
        check_color=ft.Colors.WHITE, active_color=ft.Colors.ORANGE_400,
    )

    def pick_full_export_path(e: ft.FilePickerResultEvent):
        if e.path:
            full_export_path.value = e.path
            page.update()

    full_export_picker = ft.FilePicker(on_result=pick_full_export_path)
    page.overlay.append(full_export_picker)

    def do_full_export(e):
        database = full_export_db.value
        path = full_export_path.value
        if not path:
            log("Especifica una ruta para el archivo de exportación.")
            return

        is_all_dbs = (database == "* (Todas las bases de datos)")

        full_export_progress.visible = True
        full_export_status.value = "Exportando..."
        full_export_status.color = ft.Colors.ORANGE_200
        page.update()

        def run_export():
            try:
                config = db._config
                cmd = [
                    "mysqldump",
                    f"--host={config['host']}",
                    f"--port={config['port']}",
                    f"--user={config['user']}",
                    f"--password={config['password']}",
                    "--column-statistics=0",
                ]

                # Opciones de contenido
                if full_export_include_routines.value:
                    cmd.extend(["--routines", "--triggers", "--events"])
                if not full_export_include_data.value:
                    cmd.append("--no-data")
                if not full_export_include_structure.value:
                    cmd.append("--no-create-info")

                if is_all_dbs:
                    cmd.append("--all-databases")
                    log("Exportando TODAS las bases de datos...")
                elif database:
                    cmd.extend(["--databases", database])
                    log(f"Exportando base de datos '{database}'...")
                else:
                    log("Selecciona una base de datos.")
                    full_export_progress.visible = False
                    page.update()
                    return

                with open(path, "w", encoding="utf-8") as f:
                    result = subprocess.run(
                        cmd, stdout=f, stderr=subprocess.PIPE,
                        text=True, timeout=600
                    )

                # Procesar resultado
                stderr = result.stderr.strip() if result.stderr else ""
                # Filtrar warnings de contraseña
                error_lines = []
                for line in stderr.splitlines():
                    if "using a password on the command line" in line.lower():
                        continue
                    if line.strip():
                        error_lines.append(line.strip())

                if result.returncode == 0 and not error_lines:
                    file_size = os.path.getsize(path)
                    size_str = _format_size(file_size)
                    if is_all_dbs:
                        full_export_status.value = "Todas las BDs exportadas"
                    else:
                        full_export_status.value = f"'{database}' exportada"
                    full_export_status.color = ft.Colors.GREEN_300
                    log(f"Exportación completada: {path} ({size_str})")
                elif error_lines:
                    for err in error_lines[:5]:
                        log(f"Advertencia: {err[:200]}")
                    if result.returncode == 0:
                        file_size = os.path.getsize(path)
                        size_str = _format_size(file_size)
                        full_export_status.value = "Exportado con advertencias"
                        full_export_status.color = ft.Colors.AMBER_300
                        log(f"Exportación completada con advertencias: {path} ({size_str})")
                    else:
                        full_export_status.value = "Error al exportar"
                        full_export_status.color = ft.Colors.RED_300
                        log(f"Error en exportación: código {result.returncode}")
                else:
                    full_export_status.value = "Error al exportar"
                    full_export_status.color = ft.Colors.RED_300
                    log(f"Error: {stderr[:300]}")

            except FileNotFoundError:
                log("mysqldump no encontrado. Asegúrate de que esté en el PATH del sistema.")
                log("Tip: En Windows, agrega la carpeta bin de MariaDB/MySQL al PATH.")
                full_export_status.value = "mysqldump no encontrado"
                full_export_status.color = ft.Colors.RED_300
            except subprocess.TimeoutExpired:
                log("La exportación tardó demasiado (más de 10 minutos).")
                full_export_status.value = "Timeout"
                full_export_status.color = ft.Colors.RED_300
            except Exception as ex:
                log(f"Error inesperado: {ex}")
                full_export_status.value = "Error"
                full_export_status.color = ft.Colors.RED_300
            finally:
                full_export_progress.visible = False
                page.update()

        threading.Thread(target=run_export, daemon=True).start()

    # ================================================================
    # EXPORTACIÓN POR TABLA (CSV, JSON, SQL)
    # ================================================================
    export_db = ft.Dropdown(label="Base de datos", width=250, border_color=ft.Colors.AMBER_400,
                            focused_border_color=ft.Colors.AMBER_200)
    export_table = ft.Dropdown(label="Tabla", width=250, border_color=ft.Colors.AMBER_400,
                               focused_border_color=ft.Colors.AMBER_200)
    export_format = ft.Dropdown(
        label="Formato", width=150,
        options=[ft.dropdown.Option("CSV"), ft.dropdown.Option("JSON"), ft.dropdown.Option("SQL")],
        value="CSV",
        border_color=ft.Colors.AMBER_400,
        focused_border_color=ft.Colors.AMBER_200,
    )
    export_path = ft.TextField(
        label="Ruta de exportación",
        width=450,
        value=os.path.join(os.path.expanduser("~"), "export.csv"),
        border_color=ft.Colors.AMBER_400,
        focused_border_color=ft.Colors.AMBER_200,
    )

    def pick_export_path(e: ft.FilePickerResultEvent):
        if e.path:
            export_path.value = e.path
            page.update()

    export_picker = ft.FilePicker(on_result=pick_export_path)
    page.overlay.append(export_picker)

    def on_export_db_change(e):
        if export_db.value:
            try:
                tables = db.get_tables(export_db.value)
                export_table.options = [ft.dropdown.Option("* (Todas las tablas)")] + [ft.dropdown.Option(t) for t in tables]
                export_table.value = None
                page.update()
            except Exception as ex:
                log(f"Error: {ex}")

    export_db.on_change = on_export_db_change

    def do_export(e):
        database = export_db.value
        table_selection = export_table.value
        fmt = export_format.value
        path = export_path.value
        if not all([database, table_selection, path]):
            log("Completa todos los campos.")
            return

        is_all_tables = (table_selection == "* (Todas las tablas)")
        tables_to_export = db.get_tables(database) if is_all_tables else [table_selection]
        
        log(f"Exportando {'todas las tablas' if is_all_tables else table_selection} a {fmt}...")
        try:
            if is_all_tables:
                total_rows = 0
                if fmt == "JSON":
                    out_dict = {}
                    for t in tables_to_export:
                        cols, rows = db.execute_query(f"SELECT * FROM `{t}`", database=database)
                        out_dict[t] = [dict(zip(cols, [str(v) if v is not None else None for v in row])) for row in rows]
                        total_rows += len(rows)
                    with open(path, "w", encoding="utf-8") as f:
                        json.dump(out_dict, f, indent=2, ensure_ascii=False)
                    log(f"Exportación completada: {path} ({total_rows} registros en total)")
                elif fmt == "SQL":
                    with open(path, "w", encoding="utf-8") as f:
                        for t in tables_to_export:
                            cols, rows = db.execute_query(f"SELECT * FROM `{t}`", database=database)
                            f.write(f"\n-- Datos de la tabla {t}\n")
                            for row in rows:
                                vals = []
                                for v in row:
                                    if v is None:
                                        vals.append("NULL")
                                    else:
                                        s = str(v).replace('\\', '\\\\').replace("'", "''").replace('\n', '\\n').replace('\r', '\\r')
                                        vals.append(f"'{s}'")
                                vals_str = ", ".join(vals)
                                f.write(f"INSERT INTO `{t}` ({', '.join(f'`{c}`' for c in cols)}) VALUES ({vals_str});\n")
                            total_rows += len(rows)
                    log(f"Exportación completada: {path} ({total_rows} registros en total)")
                elif fmt == "CSV":
                    total_rows = 0
                    with open(path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        for t in tables_to_export:
                            cols, rows = db.execute_query(f"SELECT * FROM `{t}`", database=database)
                            f.write(f"--- TABLA: {t} ---\n")
                            writer.writerow(cols)
                            writer.writerows(rows)
                            f.write("\n")
                            total_rows += len(rows)
                    log(f"Exportación completada: único archivo CSV generado ({total_rows} registros)")
            else:
                cols, rows = db.execute_query(f"SELECT * FROM `{table_selection}`", database=database)
                if fmt == "CSV":
                    with open(path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(cols)
                        writer.writerows(rows)
                elif fmt == "JSON":
                    data = [dict(zip(cols, [str(v) if v is not None else None for v in row])) for row in rows]
                    with open(path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                elif fmt == "SQL":
                    with open(path, "w", encoding="utf-8") as f:
                        for row in rows:
                            vals = []
                            for v in row:
                                if v is None:
                                    vals.append("NULL")
                                else:
                                    s = str(v).replace('\\', '\\\\').replace("'", "''").replace('\n', '\\n').replace('\r', '\\r')
                                    vals.append(f"'{s}'")
                            vals_str = ", ".join(vals)
                            f.write(f"INSERT INTO `{table_selection}` ({', '.join(f'`{c}`' for c in cols)}) VALUES ({vals_str});\n")
                log(f"Exportación completada: {path} ({len(rows)} registros)")
        except Exception as ex:
            log(f"Error: {ex}")

    # ================================================================
    # IMPORTACIÓN POR TABLA (CSV, JSON, SQL)
    # ================================================================
    import_db = ft.Dropdown(label="Base de datos destino", width=250, border_color=ft.Colors.TEAL_400,
                            focused_border_color=ft.Colors.TEAL_200)
    import_table_name = ft.TextField(label="Nombre de tabla destino", width=250, border_color=ft.Colors.TEAL_400,
                                     focused_border_color=ft.Colors.TEAL_200, hint_text="Usa * para todas las tablas")
    import_format = ft.Dropdown(
        label="Formato", width=150,
        options=[ft.dropdown.Option("CSV"), ft.dropdown.Option("JSON"), ft.dropdown.Option("SQL")],
        value="CSV",
        border_color=ft.Colors.TEAL_400,
        focused_border_color=ft.Colors.TEAL_200,
    )
    import_path = ft.TextField(label="Archivo a importar", width=450, border_color=ft.Colors.TEAL_400,
                               focused_border_color=ft.Colors.TEAL_200)

    def pick_import_file(e: ft.FilePickerResultEvent):
        if e.files:
            import_path.value = e.files[0].path
            page.update()

    import_picker = ft.FilePicker(on_result=pick_import_file)
    page.overlay.append(import_picker)

    def do_import(e):
        database = import_db.value
        table = import_table_name.value
        fmt = import_format.value
        path = import_path.value
        if not database:
            log("Selecciona base de datos destino.")
            return
        if not path or not os.path.exists(path):
            log("Archivo no encontrado.")
            return

        is_all_tables = (table == "*" or table == "* (Todas las tablas)")

        log(f"Importando desde {path}...")
        try:
            if fmt == "CSV":
                if not table:
                    log("Especifica el nombre de la tabla destino.")
                    return
                content = _read_file(path)
                
                if is_all_tables:
                    lines = content.splitlines()
                    current_table = None
                    headers = []
                    rows_data = []
                    conn = db.get_connection()
                    cursor = conn.cursor()
                    cursor.execute(f"USE `{database}`")
                    
                    def save_current_table():
                        if current_table and headers and rows_data:
                            cursor.execute(f"SHOW TABLES LIKE '{current_table}'")
                            if not cursor.fetchall():
                                cols_def = ", ".join(f"`{h}` TEXT" for h in headers)
                                cursor.execute(f"CREATE TABLE `{current_table}` ({cols_def})")
                            count = 0
                            for row in rows_data:
                                placeholders = ", ".join(["%s"] * len(row))
                                cols_str = ", ".join(f"`{h}`" for h in headers)
                                cursor.execute(f"INSERT INTO `{current_table}` ({cols_str}) VALUES ({placeholders})", tuple(row))
                                count += 1
                            log(f"Importados {count} registros en {current_table}")

                    for line in lines:
                        stripped = line.strip()
                        if stripped.startswith("--- TABLA: "):
                            save_current_table()
                            current_table = stripped.replace("--- TABLA: ", "").replace(" ---", "")
                            headers = []
                            rows_data = []
                        elif current_table and stripped:
                            reader = csv.reader([line])
                            row = next(reader)
                            if not headers:
                                headers = row
                            else:
                                rows_data.append(row)
                    save_current_table()
                    conn.commit()
                    cursor.close()
                    log("Importación global de CSV completada.")
                else:
                    reader = csv.reader(content.splitlines())
                    headers = next(reader)
                    rows_data = list(reader)
                    conn = db.get_connection()
                    cursor = conn.cursor()
                    cursor.execute(f"USE `{database}`")
                    cursor.execute(f"SHOW TABLES LIKE '{table}'")
                    if not cursor.fetchall():
                        cols_def = ", ".join(f"`{h}` TEXT" for h in headers)
                        cursor.execute(f"CREATE TABLE `{table}` ({cols_def})")
                        log(f"Tabla '{table}' creada con {len(headers)} columnas.")
                    count = 0
                    for row in rows_data:
                        placeholders = ", ".join(["%s"] * len(row))
                        cols_str = ", ".join(f"`{h}`" for h in headers)
                        cursor.execute(
                            f"INSERT INTO `{table}` ({cols_str}) VALUES ({placeholders})", tuple(row)
                        )
                        count += 1
                    conn.commit()
                    cursor.close()
                    log(f"Importados {count} registros en {database}.{table}")

            elif fmt == "JSON":
                if not table:
                    log("Especifica el nombre de la tabla destino.")
                    return
                content = _read_file(path)
                data = json.loads(content)
                
                if is_all_tables:
                    if not isinstance(data, dict):
                        log("El archivo JSON debe contener un diccionario de tablas para importación global.")
                        return
                    conn = db.get_connection()
                    cursor = conn.cursor()
                    cursor.execute(f"USE `{database}`")
                    for t_name, t_data in data.items():
                        if not t_data: continue
                        headers = list(t_data[0].keys())
                        cursor.execute(f"SHOW TABLES LIKE '{t_name}'")
                        if not cursor.fetchall():
                            cols_def = ", ".join(f"`{h}` TEXT" for h in headers)
                            cursor.execute(f"CREATE TABLE `{t_name}` ({cols_def})")
                        count = 0
                        for record in t_data:
                            values = [record.get(h) for h in headers]
                            placeholders = ", ".join(["%s"] * len(headers))
                            cols_str = ", ".join(f"`{h}`" for h in headers)
                            cursor.execute(f"INSERT INTO `{t_name}` ({cols_str}) VALUES ({placeholders})", tuple(values))
                            count += 1
                        log(f"Importados {count} registros JSON en {t_name}")
                    conn.commit()
                    cursor.close()
                    log("Importación global de JSON completada.")
                else:
                    if not isinstance(data, list) or len(data) == 0:
                        log("El archivo JSON debe contener un array de objetos.")
                        return
                    headers = list(data[0].keys())
                    conn = db.get_connection()
                    cursor = conn.cursor()
                    cursor.execute(f"USE `{database}`")
                    cursor.execute(f"SHOW TABLES LIKE '{table}'")
                    if not cursor.fetchall():
                        cols_def = ", ".join(f"`{h}` TEXT" for h in headers)
                        cursor.execute(f"CREATE TABLE `{table}` ({cols_def})")
                        log(f"Tabla '{table}' creada con {len(headers)} columnas.")
                    count = 0
                    for record in data:
                        values = [record.get(h) for h in headers]
                        placeholders = ", ".join(["%s"] * len(headers))
                        cols_str = ", ".join(f"`{h}`" for h in headers)
                        cursor.execute(
                            f"INSERT INTO `{table}` ({cols_str}) VALUES ({placeholders})", tuple(values)
                        )
                        count += 1
                    conn.commit()
                    cursor.close()
                    log(f"Importados {count} registros JSON en {database}.{table}")

            elif fmt == "SQL":
                content = _read_file(path)
                statements = _split_sql_statements(content)
                if not statements:
                    log("No se encontraron sentencias SQL válidas.")
                    return
                conn = db.get_connection()
                cursor = conn.cursor()
                cursor.execute(f"USE `{database}`")
                executed = 0
                errors = 0
                for stmt in statements:
                    try:
                        cursor.execute(stmt)
                        if cursor.with_rows:
                            cursor.fetchall()
                        executed += 1
                    except Exception as stmt_ex:
                        errors += 1
                        if errors <= 3:
                            log(f"Error en sentencia: {str(stmt_ex)[:100]}")
                conn.commit()
                cursor.close()
                msg = f"SQL ejecutado: {executed} sentencias"
                if errors > 0:
                    msg += f" ({errors} con error)"
                log(msg)
        except Exception as ex:
            log(f"Error: {ex}")

    def refresh_dbs(e=None):
        try:
            databases = db.get_databases()
            full_export_db.options = (
                [ft.dropdown.Option("* (Todas las bases de datos)")] +
                [ft.dropdown.Option(d) for d in databases]
            )
            export_db.options = [ft.dropdown.Option(d) for d in databases]
            import_db.options = [ft.dropdown.Option(d) for d in databases]
            page.update()
        except Exception as ex:
            log(f"Error: {ex}")

    refresh_dbs()

    # ================================================================
    # LAYOUT
    # ================================================================
    return ft.Container(
        content=ft.Column(
            [
                ft.Row([
                    ft.Icon(ft.Icons.IMPORT_EXPORT, size=30, color=ft.Colors.AMBER_300),
                    ft.Text("Exportación e Importación", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE),
                    ft.IconButton(ft.Icons.REFRESH, on_click=refresh_dbs, tooltip="Refrescar",
                                  icon_color=ft.Colors.AMBER_300),
                ]),
                ft.Divider(color=ft.Colors.WHITE24),

                # Importar BD completa
                ft.Container(
                    content=ft.Column([
                        ft.Row([
                            ft.Icon(ft.Icons.CLOUD_DOWNLOAD, size=24, color=ft.Colors.LIGHT_BLUE_300),
                            ft.Text("Importar Base de Datos Completa",
                                    size=18, weight=ft.FontWeight.W_600, color=ft.Colors.LIGHT_BLUE_200),
                        ]),
                        ft.Text("Selecciona un archivo .sql descargado de internet, phpMyAdmin, mysqldump, etc.",
                                size=12, color=ft.Colors.WHITE54),
                        ft.Container(height=5),
                        ft.Row([
                            quick_import_path,
                            ft.IconButton(
                                ft.Icons.FILE_OPEN,
                                on_click=lambda _: quick_import_picker.pick_files(
                                    allowed_extensions=["sql"], allow_multiple=False
                                ),
                                tooltip="Seleccionar archivo .sql",
                                icon_color=ft.Colors.LIGHT_BLUE_300,
                                icon_size=28,
                            ),
                        ]),
                        quick_import_db_name,
                        ft.Container(height=5),
                        quick_import_progress,
                        quick_import_status,
                        ft.ElevatedButton(
                            "Importar Base de Datos",
                            icon=ft.Icons.PLAY_ARROW,
                            on_click=do_quick_import,
                            height=45,
                            style=ft.ButtonStyle(
                                bgcolor=ft.Colors.LIGHT_BLUE_700,
                                color=ft.Colors.WHITE,
                                shape=ft.RoundedRectangleBorder(radius=10),
                                elevation=4,
                            ),
                        ),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.2, ft.Colors.LIGHT_BLUE),
                    border=ft.border.all(1, ft.Colors.with_opacity(0.3, ft.Colors.LIGHT_BLUE_300)),
                    border_radius=14, padding=20,
                ),
                ft.Container(height=15),

                # Exportar BD completa
                ft.Container(
                    content=ft.Column([
                        ft.Row([
                            ft.Icon(ft.Icons.CLOUD_UPLOAD, size=24, color=ft.Colors.ORANGE_300),
                            ft.Text("Exportar Base de Datos Completa",
                                    size=18, weight=ft.FontWeight.W_600, color=ft.Colors.ORANGE_200),
                        ]),
                        ft.Text("Exporta estructura + datos usando mysqldump. Puedes exportar una BD o todas a la vez.",
                                size=12, color=ft.Colors.WHITE54),
                        ft.Container(height=5),
                        ft.Row([full_export_db], alignment=ft.MainAxisAlignment.START),
                        ft.Row([
                            full_export_include_structure,
                            full_export_include_data,
                            full_export_include_routines,
                        ], wrap=True),
                        ft.Row([
                            full_export_path,
                            ft.IconButton(
                                ft.Icons.FOLDER_OPEN,
                                on_click=lambda _: full_export_picker.save_file(
                                    allowed_extensions=["sql"], file_name="full_export.sql"
                                ),
                                tooltip="Seleccionar ruta",
                                icon_color=ft.Colors.ORANGE_300,
                                icon_size=28,
                            ),
                        ]),
                        full_export_progress,
                        full_export_status,
                        ft.ElevatedButton(
                            "Exportar Base de Datos",
                            icon=ft.Icons.UPLOAD_FILE,
                            on_click=do_full_export,
                            height=45,
                            style=ft.ButtonStyle(
                                bgcolor=ft.Colors.ORANGE_700,
                                color=ft.Colors.WHITE,
                                shape=ft.RoundedRectangleBorder(radius=10),
                                elevation=4,
                            ),
                        ),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.2, ft.Colors.ORANGE),
                    border=ft.border.all(1, ft.Colors.with_opacity(0.3, ft.Colors.ORANGE_300)),
                    border_radius=14, padding=20,
                ),
                ft.Container(height=15),

                # Exportación por tabla
                ft.Container(
                    content=ft.Column([
                        ft.Text("Exportar Datos por Tabla", size=18, weight=ft.FontWeight.W_600, color=ft.Colors.AMBER_200),
                        ft.Text("Para exportar tablas individuales en formato CSV, JSON o SQL.",
                                size=12, color=ft.Colors.WHITE54),
                        ft.Row([export_db, export_table, export_format], wrap=True),
                        ft.Row([
                            export_path,
                            ft.IconButton(ft.Icons.FOLDER_OPEN, on_click=lambda _: export_picker.save_file(
                                file_name="export.csv"
                            ), tooltip="Seleccionar ruta", icon_color=ft.Colors.AMBER_300),
                        ]),
                        ft.ElevatedButton("Exportar", icon=ft.Icons.UPLOAD_FILE, on_click=do_export,
                                          style=ft.ButtonStyle(bgcolor=ft.Colors.AMBER_700, color=ft.Colors.WHITE,
                                                               shape=ft.RoundedRectangleBorder(radius=8))),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.15, ft.Colors.AMBER),
                    border_radius=12, padding=20,
                ),
                ft.Container(height=10),

                # Importar por tabla
                ft.Container(
                    content=ft.Column([
                        ft.Text("Importar Datos por Tabla",
                                size=18, weight=ft.FontWeight.W_600, color=ft.Colors.TEAL_200),
                        ft.Text("Para importar archivos CSV, JSON o sentencias SQL a una tabla específica.",
                                size=12, color=ft.Colors.WHITE54),
                        ft.Row([import_db, import_table_name, import_format], wrap=True),
                        ft.Row([
                            import_path,
                            ft.IconButton(ft.Icons.FILE_OPEN, on_click=lambda _: import_picker.pick_files(
                                allowed_extensions=["csv", "json", "sql"], allow_multiple=False
                            ), tooltip="Seleccionar archivo", icon_color=ft.Colors.TEAL_300),
                        ]),
                        ft.ElevatedButton("Importar", icon=ft.Icons.DOWNLOAD, on_click=do_import,
                                          style=ft.ButtonStyle(bgcolor=ft.Colors.TEAL_700, color=ft.Colors.WHITE,
                                                               shape=ft.RoundedRectangleBorder(radius=8))),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.15, ft.Colors.TEAL),
                    border_radius=12, padding=20,
                ),
                ft.Container(height=10),
                ft.Text("Log", size=14, weight=ft.FontWeight.W_600, color=ft.Colors.WHITE70),
                log_container,
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=10,
        ),
        padding=25,
        expand=True,
    )
