"""
Módulo de Exportación e Importación de datos.
Soporta CSV, JSON y SQL.
"""

import csv
import json
import os
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

    # --- Exportación ---
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
                export_table.options = [ft.dropdown.Option(t) for t in tables]
                export_table.value = None
                page.update()
            except Exception as ex:
                log(f"❌ {ex}")

    export_db.on_change = on_export_db_change

    def do_export(e):
        database = export_db.value
        table = export_table.value
        fmt = export_format.value
        path = export_path.value
        if not all([database, table, path]):
            log("⚠️ Completa todos los campos.")
            return
        log(f"🔄 Exportando {database}.{table} a {fmt}...")
        try:
            cols, rows = db.execute_query(f"SELECT * FROM `{table}`", database=database)
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
                        vals = ", ".join(
                            [f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" if v is not None else "NULL" for v in row])
                        f.write(f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in cols)}) VALUES ({vals});\n")
            log(f"✅ Exportación completada: {path} ({len(rows)} registros)")
        except Exception as ex:
            log(f"❌ Error: {ex}")

    # --- Importación ---
    import_db = ft.Dropdown(label="Base de datos destino", width=250, border_color=ft.Colors.TEAL_400,
                            focused_border_color=ft.Colors.TEAL_200)
    import_table_name = ft.TextField(label="Nombre de tabla destino", width=250, border_color=ft.Colors.TEAL_400,
                                     focused_border_color=ft.Colors.TEAL_200)
    import_format = ft.Dropdown(
        label="Formato", width=150,
        options=[ft.dropdown.Option("CSV"), ft.dropdown.Option("SQL")],
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
            log("⚠️ Selecciona base de datos destino.")
            return
        if not path or not os.path.exists(path):
            log("⚠️ Archivo no encontrado.")
            return

        log(f"🔄 Importando desde {path}...")
        try:
            if fmt == "CSV":
                if not table:
                    log("⚠️ Especifica el nombre de la tabla destino.")
                    return
                with open(path, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    count = 0
                    conn = db.get_connection()
                    cursor = conn.cursor()
                    cursor.execute(f"USE `{database}`")
                    for row in reader:
                        placeholders = ", ".join(["%s"] * len(row))
                        cols_str = ", ".join(f"`{h}`" for h in headers)
                        cursor.execute(
                            f"INSERT INTO `{table}` ({cols_str}) VALUES ({placeholders})", row
                        )
                        count += 1
                    conn.commit()
                    cursor.close()
                    log(f"✅ Importados {count} registros a {database}.{table}")
            elif fmt == "SQL":
                with open(path, "r", encoding="utf-8") as f:
                    sql_content = f.read()
                db.execute_many(sql_content, database=database)
                log(f"✅ SQL ejecutado en {database}")
        except Exception as ex:
            log(f"❌ Error: {ex}")

    def refresh_dbs(e=None):
        try:
            databases = db.get_databases()
            options = [ft.dropdown.Option(d) for d in databases]
            export_db.options = options
            import_db.options = options
            page.update()
        except Exception as ex:
            log(f"❌ {ex}")

    refresh_dbs()

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
                # Exportación
                ft.Container(
                    content=ft.Column([
                        ft.Text("📤 Exportar Datos", size=18, weight=ft.FontWeight.W_600, color=ft.Colors.AMBER_200),
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
                # Importación
                ft.Container(
                    content=ft.Column([
                        ft.Text("📥 Importar Datos", size=18, weight=ft.FontWeight.W_600, color=ft.Colors.TEAL_200),
                        ft.Row([import_db, import_table_name, import_format], wrap=True),
                        ft.Row([
                            import_path,
                            ft.IconButton(ft.Icons.FILE_OPEN, on_click=lambda _: import_picker.pick_files(
                                allowed_extensions=["csv", "sql"], allow_multiple=False
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
                ft.Text("📝 Log", size=14, weight=ft.FontWeight.W_600, color=ft.Colors.WHITE70),
                log_container,
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=10,
        ),
        padding=25,
        expand=True,
    )
