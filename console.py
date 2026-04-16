"""
Módulo de Consola SQL.
Permite ejecutar queries SQL y ver resultados en una DataTable.
"""

import flet as ft
from db_connection import DBConnection


def build_console_view(page: ft.Page):
    db = DBConnection.get_instance()

    # --- Estado ---
    db_dropdown = ft.Dropdown(
        label="Base de datos",
        width=250,
        border_color=ft.Colors.INDIGO_400,
        focused_border_color=ft.Colors.INDIGO_200,
    )
    history_list = []
    history_dropdown = ft.Dropdown(
        label="Historial de queries",
        width=500,
        border_color=ft.Colors.INDIGO_400,
        focused_border_color=ft.Colors.INDIGO_200,
    )

    query_input = ft.TextField(
        label="Escribe tu query SQL aquí...",
        multiline=True,
        min_lines=5,
        max_lines=12,
        border_color=ft.Colors.INDIGO_400,
        focused_border_color=ft.Colors.INDIGO_200,
        text_style=ft.TextStyle(
            font_family="Consolas",
            size=14,
            color=ft.Colors.WHITE,
        ),
        expand=True,
    )

    result_table = ft.DataTable(
        columns=[ft.DataColumn(ft.Text("Resultado", color=ft.Colors.WHITE))],
        rows=[],
        border=ft.border.all(1, ft.Colors.WHITE24),
        border_radius=10,
        heading_row_color=ft.Colors.with_opacity(0.25, ft.Colors.INDIGO),
        data_row_color={ft.ControlState.HOVERED: ft.Colors.with_opacity(0.08, ft.Colors.INDIGO)},
        column_spacing=20,
    )

    result_info = ft.Text("", size=13, color=ft.Colors.GREEN_200)
    row_count_text = ft.Text("", size=12, color=ft.Colors.WHITE54)

    def show_result(msg, color=ft.Colors.GREEN_200):
        result_info.value = msg
        result_info.color = color
        page.update()

    def execute_query(e):
        query = query_input.value
        database = db_dropdown.value
        if not query or not query.strip():
            show_result("⚠️ Escribe una query.", ft.Colors.AMBER_300)
            return

        # Añadir al historial
        trimmed = query.strip()[:80]
        if trimmed not in history_list:
            history_list.insert(0, trimmed)
            if len(history_list) > 20:
                history_list.pop()
            history_dropdown.options = [ft.dropdown.Option(h) for h in history_list]

        try:
            if ";" in query.strip() and query.strip().count(";") > 1:
                # Múltiples statements
                results = db.execute_many(query, database=database)
                all_rows_info = []
                last_result = None
                for res_type, cols, data in results:
                    if res_type == "result":
                        last_result = (cols, data)
                        all_rows_info.append(f"{len(data)} filas")
                    else:
                        all_rows_info.append(f"{data} filas afectadas")

                if last_result:
                    display_result_table(last_result[0], last_result[1])
                show_result(f"✅ Ejecutado: {', '.join(all_rows_info)}")
            else:
                cols, rows = db.execute_query(query.strip().rstrip(";"), database=database)
                if cols:
                    display_result_table(cols, rows)
                    show_result(f"✅ Query ejecutada exitosamente")
                    row_count_text.value = f"{len(rows)} filas retornadas"
                else:
                    result_table.columns = [ft.DataColumn(ft.Text("Resultado", color=ft.Colors.WHITE))]
                    result_table.rows = [
                        ft.DataRow(cells=[ft.DataCell(ft.Text(f"✅ {rows} filas afectadas", color=ft.Colors.GREEN_200))])
                    ]
                    row_count_text.value = f"{rows} filas afectadas"
                    show_result("✅ Comando ejecutado exitosamente")
        except Exception as ex:
            result_table.columns = [ft.DataColumn(ft.Text("Error", color=ft.Colors.RED_300))]
            result_table.rows = [
                ft.DataRow(cells=[ft.DataCell(ft.Text(str(ex), color=ft.Colors.RED_200, selectable=True))])
            ]
            row_count_text.value = ""
            show_result(f"❌ Error", ft.Colors.RED_300)

        page.update()

    def display_result_table(cols, rows):
        result_table.columns = [
            ft.DataColumn(ft.Text(c, color=ft.Colors.WHITE, weight=ft.FontWeight.BOLD, size=12))
            for c in cols
        ]
        max_rows = 200
        display_rows = rows[:max_rows]
        result_table.rows = [
            ft.DataRow(cells=[
                ft.DataCell(ft.Text(
                    str(val)[:100] if val is not None else "NULL",
                    color=ft.Colors.WHITE70 if val is not None else ft.Colors.WHITE24,
                    size=12,
                    selectable=True,
                ))
                for val in row
            ])
            for row in display_rows
        ]
        row_count_text.value = f"{len(rows)} filas retornadas" + (f" (mostrando {max_rows})" if len(rows) > max_rows else "")

    def clear_query(e):
        query_input.value = ""
        page.update()

    def on_history_change(e):
        if history_dropdown.value:
            query_input.value = history_dropdown.value
            page.update()

    history_dropdown.on_change = on_history_change

    def refresh_dbs(e=None):
        try:
            databases = db.get_databases()
            db_dropdown.options = [ft.dropdown.Option(d) for d in databases]
            page.update()
        except Exception as ex:
            show_result(f"❌ {ex}", ft.Colors.RED_300)

    refresh_dbs()

    # --- Atajo Ctrl+Enter ---
    def on_keyboard(e: ft.KeyboardEvent):
        if e.key == "Enter" and e.ctrl:
            execute_query(e)

    page.on_keyboard_event = on_keyboard

    # Botones de ejemplo
    example_queries = [
        ("SHOW DATABASES", "SHOW DATABASES"),
        ("SHOW TABLES", "SHOW TABLES"),
        ("SHOW PROCESSLIST", "SHOW PROCESSLIST"),
        ("STATUS", "SHOW GLOBAL STATUS"),
        ("VARIABLES", "SHOW GLOBAL VARIABLES"),
    ]

    def set_query(q):
        def handler(e):
            query_input.value = q
            page.update()
        return handler

    quick_buttons = ft.Row(
        [ft.OutlinedButton(
            label, on_click=set_query(q),
            style=ft.ButtonStyle(
                color=ft.Colors.INDIGO_200,
                side=ft.BorderSide(1, ft.Colors.INDIGO_400),
                shape=ft.RoundedRectangleBorder(radius=6),
            ),
        ) for label, q in example_queries],
        wrap=True, spacing=8,
    )

    return ft.Container(
        content=ft.Column(
            [
                ft.Row([
                    ft.Icon(ft.Icons.TERMINAL, size=30, color=ft.Colors.INDIGO_300),
                    ft.Text("Consola SQL", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE),
                    ft.IconButton(ft.Icons.REFRESH, on_click=refresh_dbs, tooltip="Refrescar BDs",
                                  icon_color=ft.Colors.INDIGO_300),
                ]),
                ft.Divider(color=ft.Colors.WHITE24),
                ft.Row([db_dropdown, history_dropdown], wrap=True),
                ft.Text("⚡ Queries rápidas:", size=12, color=ft.Colors.WHITE54),
                quick_buttons,
                ft.Container(
                    content=ft.Column([
                        query_input,
                        ft.Row([
                            ft.ElevatedButton(
                                "Ejecutar",
                                icon=ft.Icons.PLAY_ARROW,
                                on_click=execute_query,
                                style=ft.ButtonStyle(
                                    bgcolor=ft.Colors.INDIGO_700,
                                    color=ft.Colors.WHITE,
                                    shape=ft.RoundedRectangleBorder(radius=8),
                                ),
                            ),
                            ft.OutlinedButton(
                                "Limpiar",
                                icon=ft.Icons.CLEAR,
                                on_click=clear_query,
                                style=ft.ButtonStyle(color=ft.Colors.WHITE54,
                                                     shape=ft.RoundedRectangleBorder(radius=8)),
                            ),
                            ft.Text("Ctrl+Enter para ejecutar", size=11, color=ft.Colors.WHITE38, italic=True),
                        ]),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.1, ft.Colors.INDIGO),
                    border_radius=12,
                    padding=20,
                ),
                ft.Container(height=5),
                ft.Row([result_info, row_count_text], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Container(
                    content=ft.Column([result_table], scroll=ft.ScrollMode.ALWAYS),
                    expand=True,
                    border_radius=10,
                    bgcolor=ft.Colors.with_opacity(0.05, ft.Colors.WHITE),
                    padding=10,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=10,
        ),
        padding=25,
        expand=True,
    )
