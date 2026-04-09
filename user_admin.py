"""
Módulo de Administración de Usuarios MariaDB.
CRUD completo de usuarios y gestión de privilegios.
"""

import flet as ft
from db_connection import DBConnection


def build_user_admin_view(page: ft.Page):
    db = DBConnection.get_instance()

    # --- Tabla de usuarios ---
    user_table = ft.DataTable(
        columns=[
            ft.DataColumn(ft.Text("Usuario", color=ft.Colors.WHITE, weight=ft.FontWeight.BOLD)),
            ft.DataColumn(ft.Text("Host", color=ft.Colors.WHITE, weight=ft.FontWeight.BOLD)),
            ft.DataColumn(ft.Text("Privilegios", color=ft.Colors.WHITE, weight=ft.FontWeight.BOLD)),
            ft.DataColumn(ft.Text("Acciones", color=ft.Colors.WHITE, weight=ft.FontWeight.BOLD)),
        ],
        rows=[],
        border=ft.border.all(1, ft.Colors.WHITE24),
        border_radius=10,
        heading_row_color=ft.Colors.with_opacity(0.3, ft.Colors.PURPLE),
        data_row_color={
            ft.ControlState.HOVERED: ft.Colors.with_opacity(0.1, ft.Colors.PURPLE),
        },
        column_spacing=30,
    )

    status_text = ft.Text("", size=13, color=ft.Colors.GREEN_200)

    def show_status(msg, color=ft.Colors.GREEN_200):
        status_text.value = msg
        status_text.color = color
        page.update()

    def load_users(e=None):
        try:
            cols, rows = db.execute_query("SELECT User, Host FROM mysql.user ORDER BY User")
            user_table.rows.clear()
            for row in rows:
                user, host = row[0], row[1]
                # Obtener privilegios
                try:
                    _, grants = db.execute_query(f"SHOW GRANTS FOR '{user}'@'{host}'")
                    privs = "; ".join([str(g[0])[:60] for g in grants])
                except Exception:
                    privs = "N/A"

                user_table.rows.append(
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(user, color=ft.Colors.WHITE)),
                            ft.DataCell(ft.Text(host, color=ft.Colors.WHITE70)),
                            ft.DataCell(ft.Container(
                                content=ft.Text(privs, size=11, color=ft.Colors.PURPLE_200),
                                width=300,
                            )),
                            ft.DataCell(ft.Row([
                                ft.IconButton(ft.Icons.KEY, tooltip="Cambiar contraseña",
                                              on_click=lambda e, u=user, h=host: open_change_password(u, h),
                                              icon_color=ft.Colors.AMBER_300, icon_size=18),
                                ft.IconButton(ft.Icons.SECURITY, tooltip="Gestionar privilegios",
                                              on_click=lambda e, u=user, h=host: open_privileges(u, h),
                                              icon_color=ft.Colors.BLUE_300, icon_size=18),
                                ft.IconButton(ft.Icons.DELETE, tooltip="Eliminar usuario",
                                              on_click=lambda e, u=user, h=host: confirm_delete(u, h),
                                              icon_color=ft.Colors.RED_300, icon_size=18),
                            ])),
                        ]
                    )
                )
            page.update()
            show_status(f"✅ {len(rows)} usuarios cargados")
        except Exception as ex:
            show_status(f"❌ Error: {ex}", ft.Colors.RED_300)

    # --- Crear usuario ---
    new_user = ft.TextField(label="Nombre de usuario", width=200, border_color=ft.Colors.PURPLE_400,
                            focused_border_color=ft.Colors.PURPLE_200)
    new_host = ft.TextField(label="Host", width=150, value="%", border_color=ft.Colors.PURPLE_400,
                            focused_border_color=ft.Colors.PURPLE_200)
    new_pass = ft.TextField(label="Contraseña", width=200, password=True, can_reveal_password=True,
                            border_color=ft.Colors.PURPLE_400, focused_border_color=ft.Colors.PURPLE_200)

    def create_user(e):
        user = new_user.value
        host = new_host.value or "%"
        pwd = new_pass.value
        if not user or not pwd:
            show_status("⚠️ Nombre y contraseña requeridos.", ft.Colors.AMBER_300)
            return
        try:
            db.execute_query(f"CREATE USER '{user}'@'{host}' IDENTIFIED BY '{pwd}'")
            show_status(f"✅ Usuario '{user}'@'{host}' creado.")
            new_user.value = ""
            new_pass.value = ""
            load_users()
        except Exception as ex:
            show_status(f"❌ {ex}", ft.Colors.RED_300)

    # --- Eliminar usuario ---
    def confirm_delete(user, host):
        def do_delete(e):
            try:
                db.execute_query(f"DROP USER '{user}'@'{host}'")
                show_status(f"✅ Usuario '{user}'@'{host}' eliminado.")
                dlg.open = False
                page.update()
                load_users()
            except Exception as ex:
                show_status(f"❌ {ex}", ft.Colors.RED_300)
                dlg.open = False
                page.update()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Confirmar eliminación", color=ft.Colors.WHITE),
            content=ft.Text(f"¿Eliminar el usuario '{user}'@'{host}'?", color=ft.Colors.WHITE70),
            actions=[
                ft.TextButton("Cancelar", on_click=lambda e: setattr(dlg, "open", False) or page.update()),
                ft.TextButton("Eliminar", on_click=do_delete,
                              style=ft.ButtonStyle(color=ft.Colors.RED_300)),
            ],
            bgcolor=ft.Colors.GREY_900,
        )
        page.overlay.append(dlg)
        dlg.open = True
        page.update()

    # --- Cambiar contraseña ---
    def open_change_password(user, host):
        pwd_field = ft.TextField(label="Nueva contraseña", password=True, can_reveal_password=True, width=300)

        def do_change(e):
            if not pwd_field.value:
                return
            try:
                db.execute_query(f"ALTER USER '{user}'@'{host}' IDENTIFIED BY '{pwd_field.value}'")
                show_status(f"✅ Contraseña cambiada para '{user}'@'{host}'.")
                dlg.open = False
                page.update()
            except Exception as ex:
                show_status(f"❌ {ex}", ft.Colors.RED_300)
                dlg.open = False
                page.update()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text(f"Cambiar contraseña: {user}@{host}", color=ft.Colors.WHITE),
            content=pwd_field,
            actions=[
                ft.TextButton("Cancelar", on_click=lambda e: setattr(dlg, "open", False) or page.update()),
                ft.TextButton("Cambiar", on_click=do_change),
            ],
            bgcolor=ft.Colors.GREY_900,
        )
        page.overlay.append(dlg)
        dlg.open = True
        page.update()

    # --- Gestionar privilegios ---
    def open_privileges(user, host):
        priv_db = ft.Dropdown(label="Base de datos", width=250)
        try:
            databases = db.get_databases()
            priv_db.options = [ft.dropdown.Option("*")] + [ft.dropdown.Option(d) for d in databases]
        except Exception:
            pass

        privileges = ["ALL PRIVILEGES", "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
                       "ALTER", "INDEX", "EXECUTE", "GRANT OPTION"]
        priv_checks = {p: ft.Checkbox(label=p, value=False) for p in privileges}

        def do_grant(e):
            selected = [p for p, cb in priv_checks.items() if cb.value]
            if not selected or not priv_db.value:
                return
            try:
                privs = ", ".join(selected)
                target = f"`{priv_db.value}`.*" if priv_db.value != "*" else "*.*"
                db.execute_query(f"GRANT {privs} ON {target} TO '{user}'@'{host}'")
                db.execute_query("FLUSH PRIVILEGES")
                show_status(f"✅ Privilegios otorgados a '{user}'@'{host}'.")
                dlg.open = False
                page.update()
                load_users()
            except Exception as ex:
                show_status(f"❌ {ex}", ft.Colors.RED_300)
                dlg.open = False
                page.update()

        def do_revoke(e):
            selected = [p for p, cb in priv_checks.items() if cb.value]
            if not selected or not priv_db.value:
                return
            try:
                privs = ", ".join(selected)
                target = f"`{priv_db.value}`.*" if priv_db.value != "*" else "*.*"
                db.execute_query(f"REVOKE {privs} ON {target} FROM '{user}'@'{host}'")
                db.execute_query("FLUSH PRIVILEGES")
                show_status(f"✅ Privilegios revocados de '{user}'@'{host}'.")
                dlg.open = False
                page.update()
                load_users()
            except Exception as ex:
                show_status(f"❌ {ex}", ft.Colors.RED_300)
                dlg.open = False
                page.update()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text(f"Privilegios: {user}@{host}", color=ft.Colors.WHITE),
            content=ft.Container(
                content=ft.Column([
                    priv_db,
                    ft.Text("Selecciona privilegios:", color=ft.Colors.WHITE70),
                    ft.Column([cb for cb in priv_checks.values()], height=250, scroll=ft.ScrollMode.AUTO),
                ], tight=True),
                width=350,
            ),
            actions=[
                ft.TextButton("Cancelar", on_click=lambda e: setattr(dlg, "open", False) or page.update()),
                ft.TextButton("GRANT", on_click=do_grant,
                              style=ft.ButtonStyle(color=ft.Colors.GREEN_300)),
                ft.TextButton("REVOKE", on_click=do_revoke,
                              style=ft.ButtonStyle(color=ft.Colors.RED_300)),
            ],
            bgcolor=ft.Colors.GREY_900,
        )
        page.overlay.append(dlg)
        dlg.open = True
        page.update()

    load_users()

    return ft.Container(
        content=ft.Column(
            [
                ft.Row([
                    ft.Icon(ft.Icons.PEOPLE, size=30, color=ft.Colors.PURPLE_300),
                    ft.Text("Administración de Usuarios", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE),
                    ft.IconButton(ft.Icons.REFRESH, on_click=load_users, tooltip="Refrescar",
                                  icon_color=ft.Colors.PURPLE_300),
                ]),
                ft.Divider(color=ft.Colors.WHITE24),
                # Crear usuario
                ft.Container(
                    content=ft.Column([
                        ft.Text("➕ Crear Usuario", size=18, weight=ft.FontWeight.W_600, color=ft.Colors.PURPLE_200),
                        ft.Row([new_user, new_host, new_pass,
                                ft.ElevatedButton("Crear", icon=ft.Icons.PERSON_ADD, on_click=create_user,
                                                  style=ft.ButtonStyle(bgcolor=ft.Colors.PURPLE_700,
                                                                       color=ft.Colors.WHITE,
                                                                       shape=ft.RoundedRectangleBorder(radius=8)))],
                               wrap=True),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.15, ft.Colors.PURPLE),
                    border_radius=12, padding=20,
                ),
                ft.Container(height=5),
                status_text,
                ft.Container(
                    content=ft.Column([user_table], scroll=ft.ScrollMode.AUTO),
                    expand=True,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=10,
        ),
        padding=25,
        expand=True,
    )
