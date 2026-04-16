"""
Sistema Gestor de Bases de Datos MariaDB
Aplicación principal con Flet.
"""

import flet as ft
from db_connection import DBConnection
from backup_restore import build_backup_restore_view
from export_import import build_export_import_view
from user_admin import build_user_admin_view
from monitoring import build_monitoring_view
from console import build_console_view


def main(page: ft.Page):
    page.title = "Gojo DB Manager — MariaDB"
    page.theme_mode = ft.ThemeMode.DARK
    page.bgcolor = "#0d1117"
    page.padding = 0
    page.window.width = 1200
    page.window.height = 780
    page.fonts = {
        "Inter": "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
    }
    page.theme = ft.Theme(
        font_family="Inter",
        color_scheme_seed=ft.Colors.INDIGO,
    )

    db = DBConnection.get_instance()
    content_area = ft.Container(expand=True, padding=0)

    # ============================================================
    # Login Screen
    # ============================================================
    def build_login():
        host_field = ft.TextField(
            label="Host", value="localhost", width=300,
            border_color=ft.Colors.INDIGO_400, focused_border_color=ft.Colors.INDIGO_200,
            prefix_icon=ft.Icons.DNS,
        )
        port_field = ft.TextField(
            label="Puerto", value="3307", width=300,
            border_color=ft.Colors.INDIGO_400, focused_border_color=ft.Colors.INDIGO_200,
            prefix_icon=ft.Icons.NUMBERS,
        )
        user_field = ft.TextField(
            label="Usuario", value="root", width=300,
            border_color=ft.Colors.INDIGO_400, focused_border_color=ft.Colors.INDIGO_200,
            prefix_icon=ft.Icons.PERSON,
        )
        pass_field = ft.TextField(
            label="Contraseña", width=300, password=True, can_reveal_password=True,
            border_color=ft.Colors.INDIGO_400, focused_border_color=ft.Colors.INDIGO_200,
            prefix_icon=ft.Icons.LOCK,
        )
        error_text = ft.Text("", color=ft.Colors.RED_300, size=13)
        loading = ft.ProgressRing(visible=False, width=20, height=20, stroke_width=2)

        def do_connect(e):
            loading.visible = True
            error_text.value = ""
            page.update()

            try:
                db.configure(
                    host=host_field.value,
                    port=int(port_field.value),
                    user=user_field.value,
                    password=pass_field.value,
                )
                db.connect()
                loading.visible = False
                page.update()
                show_main_app()
            except Exception as ex:
                loading.visible = False
                error_text.value = f"❌ {ex}"
                page.update()

        login_card = ft.Container(
            content=ft.Column(
                [
                    ft.Icon(ft.Icons.STORAGE, size=60, color=ft.Colors.INDIGO_300),
                    ft.Text("Gojo DB Manager", size=32, weight=ft.FontWeight.BOLD,
                            color=ft.Colors.WHITE),
                    ft.Text("Conectar a MariaDB", size=14, color=ft.Colors.WHITE54),
                    ft.Container(height=15),
                    host_field,
                    port_field,
                    user_field,
                    pass_field,
                    ft.Container(height=10),
                    error_text,
                    ft.Row([
                        ft.ElevatedButton(
                            "Conectar",
                            icon=ft.Icons.LOGIN,
                            on_click=do_connect,
                            width=200,
                            height=45,
                            style=ft.ButtonStyle(
                                bgcolor=ft.Colors.INDIGO_700,
                                color=ft.Colors.WHITE,
                                shape=ft.RoundedRectangleBorder(radius=10),
                                elevation=4,
                            ),
                        ),
                        loading,
                    ], alignment=ft.MainAxisAlignment.CENTER),
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8,
            ),
            width=400,
            padding=40,
            border_radius=20,
            bgcolor=ft.Colors.with_opacity(0.6, "#161b22"),
            border=ft.border.all(1, ft.Colors.with_opacity(0.15, ft.Colors.WHITE)),
            shadow=ft.BoxShadow(
                spread_radius=0, blur_radius=30,
                color=ft.Colors.with_opacity(0.3, ft.Colors.INDIGO),
            ),
        )

        return ft.Container(
            content=login_card,
            alignment=ft.alignment.center,
            expand=True,
            gradient=ft.LinearGradient(
                begin=ft.alignment.top_left,
                end=ft.alignment.bottom_right,
                colors=["#0d1117", "#161b22", "#1a1040"],
            ),
        )

    # ============================================================
    # Main App (post-login)
    # ============================================================
    def show_main_app():
        current_index = ft.Ref[int]()
        current_index.current = 0

        # Header con info del servidor
        try:
            info = db.get_server_info()
            server_info_text = f"{info['user']}@{info['server_host']}:{info['server_port']} — MariaDB {info['server_version']}"
        except Exception:
            server_info_text = "Conectado"

        header = ft.Container(
            content=ft.Row(
                [
                    ft.Row([
                        ft.Icon(ft.Icons.STORAGE, color=ft.Colors.INDIGO_300, size=24),
                        ft.Text("Gojo DB Manager", size=18, weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE),
                    ]),
                    ft.Text(server_info_text, size=12, color=ft.Colors.WHITE54),
                    ft.IconButton(
                        ft.Icons.LOGOUT,
                        tooltip="Desconectar",
                        on_click=lambda e: disconnect(),
                        icon_color=ft.Colors.RED_300,
                        icon_size=20,
                    ),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            ),
            bgcolor="#161b22",
            padding=ft.padding.symmetric(horizontal=20, vertical=10),
            border=ft.border.only(bottom=ft.BorderSide(1, ft.Colors.WHITE10)),
        )

        # Área de contenido
        module_container = ft.Container(expand=True)

        # Cargar módulos bajo demanda
        module_cache = {}

        def load_module(index):
            if index not in module_cache:
                if index == 0:
                    module_cache[0] = build_backup_restore_view(page)
                elif index == 1:
                    module_cache[1] = build_export_import_view(page)
                elif index == 2:
                    module_cache[2] = build_user_admin_view(page)
                elif index == 3:
                    module_cache[3] = build_monitoring_view(page)
                elif index == 4:
                    module_cache[4] = build_console_view(page)
            return module_cache[index]

        def on_nav_change(e):
            idx = e.control.selected_index
            current_index.current = idx
            module_container.content = load_module(idx)
            page.update()

        nav_rail = ft.NavigationRail(
            selected_index=0,
            label_type=ft.NavigationRailLabelType.ALL,
            min_width=80,
            min_extended_width=200,
            bgcolor="#161b22",
            indicator_color=ft.Colors.with_opacity(0.2, ft.Colors.INDIGO),
            on_change=on_nav_change,
            destinations=[
                ft.NavigationRailDestination(
                    icon=ft.Icons.BACKUP_OUTLINED,
                    selected_icon=ft.Icons.BACKUP,
                    label_content=ft.Text("Respaldo", size=11, color=ft.Colors.WHITE70),
                ),
                ft.NavigationRailDestination(
                    icon=ft.Icons.IMPORT_EXPORT_OUTLINED,
                    selected_icon=ft.Icons.IMPORT_EXPORT,
                    label_content=ft.Text("Exportar", size=11, color=ft.Colors.WHITE70),
                ),
                ft.NavigationRailDestination(
                    icon=ft.Icons.PEOPLE_OUTLINED,
                    selected_icon=ft.Icons.PEOPLE,
                    label_content=ft.Text("Usuarios", size=11, color=ft.Colors.WHITE70),
                ),
                ft.NavigationRailDestination(
                    icon=ft.Icons.MONITOR_HEART_OUTLINED,
                    selected_icon=ft.Icons.MONITOR_HEART,
                    label_content=ft.Text("Monitor", size=11, color=ft.Colors.WHITE70),
                ),
                ft.NavigationRailDestination(
                    icon=ft.Icons.TERMINAL_OUTLINED,
                    selected_icon=ft.Icons.TERMINAL,
                    label_content=ft.Text("Consola", size=11, color=ft.Colors.WHITE70),
                ),
            ],
        )

        # Cargar primer módulo
        module_container.content = load_module(0)

        main_layout = ft.Column(
            [
                header,
                ft.Row(
                    [
                        ft.Container(
                            content=nav_rail,
                            border=ft.border.only(right=ft.BorderSide(1, ft.Colors.WHITE10)),
                        ),
                        ft.VerticalDivider(width=1, color=ft.Colors.WHITE10),
                        module_container,
                    ],
                    expand=True,
                    spacing=0,
                ),
            ],
            expand=True,
            spacing=0,
        )

        page.controls.clear()
        page.add(main_layout)
        page.update()

    def disconnect():
        try:
            db.disconnect()
        except Exception:
            pass
        page.controls.clear()
        page.add(build_login())
        page.update()

    # Iniciar con login
    page.add(build_login())


if __name__ == "__main__":
    ft.app(target=main)
