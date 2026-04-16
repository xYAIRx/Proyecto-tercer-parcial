"""
Módulo de Respaldo y Restauración de bases de datos MariaDB.
Usa mysqldump para respaldos y mysql CLI para restauración.
"""

import subprocess
import os
import flet as ft
from db_connection import DBConnection


def build_backup_restore_view(page: ft.Page):
    db = DBConnection.get_instance()
    config = db._config

    # --- Estado ---
    log_text = ft.Text("", selectable=True, size=12, color=ft.Colors.GREEN_200)
    log_container = ft.Container(
        content=ft.Column([log_text], scroll=ft.ScrollMode.AUTO),
        bgcolor=ft.Colors.with_opacity(0.3, ft.Colors.BLACK),
        border_radius=10,
        padding=15,
        height=200,
    )

    def log(msg):
        log_text.value = (log_text.value or "") + f"\n{msg}"
        page.update()

    # --- Respaldo ---
    db_dropdown_backup = ft.Dropdown(
        label="Base de datos",
        width=300,
        border_color=ft.Colors.BLUE_400,
        focused_border_color=ft.Colors.BLUE_200,
    )
    backup_path = ft.TextField(
        label="Ruta del archivo de respaldo (.sql)",
        width=500,
        value=os.path.join(os.path.expanduser("~"), "backup.sql"),
        border_color=ft.Colors.BLUE_400,
        focused_border_color=ft.Colors.BLUE_200,
    )

    def pick_backup_path(e: ft.FilePickerResultEvent):
        if e.path:
            backup_path.value = e.path
            page.update()

    backup_picker = ft.FilePicker(on_result=pick_backup_path)
    page.overlay.append(backup_picker)

    def do_backup(e):
        database = db_dropdown_backup.value
        path = backup_path.value
        if not database:
            log("⚠️ Selecciona una base de datos.")
            return
        if not path:
            log("⚠️ Especifica una ruta para el archivo.")
            return

        log(f"🔄 Iniciando respaldo de '{database}'...")
        try:
            cmd = [
                "mysqldump",
                f"--host={config['host']}",
                f"--port={config['port']}",
                f"--user={config['user']}",
                f"--password={config['password']}",
                "--column-statistics=0",
                "--routines",
                "--triggers",
                "--events",
                "--databases",
                database,
            ]
            with open(path, "w", encoding="utf-8") as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, timeout=300)

            if result.returncode == 0:
                size = os.path.getsize(path)
                log(f"✅ Respaldo completado: {path} ({size:,} bytes)")
            else:
                log(f"❌ Error: {result.stderr.strip()}")
        except FileNotFoundError:
            log("❌ mysqldump no encontrado. Asegúrate de que esté en el PATH.")
        except Exception as ex:
            log(f"❌ Error: {ex}")

    # --- Restauración ---
    db_dropdown_restore = ft.Dropdown(
        label="Base de datos destino (Opcional)",
        width=300,
        border_color=ft.Colors.BLUE_400,
        focused_border_color=ft.Colors.BLUE_200,
    )
    restore_path = ft.TextField(
        label="Archivo .sql a restaurar",
        width=500,
        border_color=ft.Colors.BLUE_400,
        focused_border_color=ft.Colors.BLUE_200,
    )

    def pick_restore_file(e: ft.FilePickerResultEvent):
        if e.files:
            restore_path.value = e.files[0].path
            page.update()

    restore_picker = ft.FilePicker(on_result=pick_restore_file)
    page.overlay.append(restore_picker)

    def do_restore(e):
        database = db_dropdown_restore.value
        path = restore_path.value
        if not path or not os.path.exists(path):
            log("⚠️ Archivo no encontrado o no válido.")
            return

        if database:
            log(f"🔄 Restaurando '{path}' en '{database}'...")
        else:
            log(f"🔄 Restaurando '{path}' de forma global...")

        try:
            cmd = [
                "mysql",
                f"--host={config['host']}",
                f"--port={config['port']}",
                f"--user={config['user']}",
                f"--password={config['password']}",
            ]
            if database:
                cmd.append(database)

            with open(path, "r", encoding="utf-8") as f:
                result = subprocess.run(cmd, stdin=f, stderr=subprocess.PIPE, text=True, timeout=600)

            if result.returncode == 0:
                if database:
                    log(f"✅ Restauración completada en '{database}'.")
                else:
                    log("✅ Restauración global completada.")
            else:
                stderr_text = result.stderr.strip()
                if "No database selected" in stderr_text:
                    log(f"❌ Error: El archivo '{path}' no especifica su base de datos original. Por favor, selecciona una 'Base de datos destino' en el menú.")
                else:
                    log(f"❌ Error: {stderr_text}")
        except FileNotFoundError:
            log("❌ mysql CLI no encontrado. Asegúrate de que esté en el PATH.")
        except Exception as ex:
            log(f"❌ Error: {ex}")

    def refresh_dbs(e=None):
        try:
            databases = db.get_databases()
            db_dropdown_backup.options = [ft.dropdown.Option(d) for d in databases]
            db_dropdown_restore.options = [ft.dropdown.Option(d) for d in databases]
            page.update()
        except Exception as ex:
            log(f"❌ Error al listar bases de datos: {ex}")

    refresh_dbs()

    # --- Layout ---
    return ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Icon(ft.Icons.BACKUP, size=30, color=ft.Colors.BLUE_300),
                        ft.Text("Respaldo y Restauración", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE),
                        ft.IconButton(ft.Icons.REFRESH, on_click=refresh_dbs, tooltip="Refrescar BDs",
                                      icon_color=ft.Colors.BLUE_300),
                    ],
                    alignment=ft.MainAxisAlignment.START,
                ),
                ft.Divider(color=ft.Colors.WHITE24),
                # Sección Respaldo
                ft.Container(
                    content=ft.Column([
                        ft.Text("📦 Crear Respaldo", size=18, weight=ft.FontWeight.W_600, color=ft.Colors.BLUE_200),
                        ft.Row([db_dropdown_backup], alignment=ft.MainAxisAlignment.START),
                        ft.Row([
                            backup_path,
                            ft.IconButton(ft.Icons.FOLDER_OPEN, on_click=lambda _: backup_picker.save_file(
                                allowed_extensions=["sql"], file_name="backup.sql"
                            ), tooltip="Seleccionar ruta", icon_color=ft.Colors.BLUE_300),
                        ]),
                        ft.ElevatedButton(
                            "Crear Respaldo",
                            icon=ft.Icons.SAVE,
                            on_click=do_backup,
                            style=ft.ButtonStyle(
                                bgcolor=ft.Colors.BLUE_700,
                                color=ft.Colors.WHITE,
                                shape=ft.RoundedRectangleBorder(radius=8),
                            ),
                        ),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.15, ft.Colors.BLUE),
                    border_radius=12,
                    padding=20,
                ),
                ft.Container(height=10),
                # Sección Restauración
                ft.Container(
                    content=ft.Column([
                        ft.Text("♻️ Restaurar Base de Datos", size=18, weight=ft.FontWeight.W_600,
                                color=ft.Colors.GREEN_200),
                        ft.Row([db_dropdown_restore], alignment=ft.MainAxisAlignment.START),
                        ft.Row([
                            restore_path,
                            ft.IconButton(ft.Icons.FILE_OPEN, on_click=lambda _: restore_picker.pick_files(
                                allowed_extensions=["sql"], allow_multiple=False
                            ), tooltip="Seleccionar archivo", icon_color=ft.Colors.GREEN_300),
                        ]),
                        ft.ElevatedButton(
                            "Restaurar",
                            icon=ft.Icons.RESTORE,
                            on_click=do_restore,
                            style=ft.ButtonStyle(
                                bgcolor=ft.Colors.GREEN_700,
                                color=ft.Colors.WHITE,
                                shape=ft.RoundedRectangleBorder(radius=8),
                            ),
                        ),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.15, ft.Colors.GREEN),
                    border_radius=12,
                    padding=20,
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
