"""
Módulo de Monitoreo de Rendimiento de MariaDB.
Gráficas en tiempo real: Queries/s y Conexiones.
"""

import threading
import time
import flet as ft

from db_connection import DBConnection


class MonitorData:
    """Almacena datos históricos para las gráficas."""

    def __init__(self, max_points=30):
        self.max_points = max_points
        self.queries_history = []  # (timestamp, select/s, insert/s, update/s, delete/s)
        self.connections_history = []  # (current, max)
        self.prev_stats = {}
        self.prev_time = None

    def reset(self):
        self.queries_history.clear()
        self.connections_history.clear()
        self.prev_stats.clear()
        self.prev_time = None


def build_monitoring_view(page: ft.Page):
    db = DBConnection.get_instance()
    monitor = MonitorData(max_points=30)
    running = threading.Event()
    running.set()

    # --- Variables de estado ---
    status_cards_row = ft.Row([], wrap=True, spacing=10)
    stats_table = ft.DataTable(
        columns=[
            ft.DataColumn(ft.Text("Variable", weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE)),
            ft.DataColumn(ft.Text("Valor", weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE)),
        ],
        rows=[],
        border=ft.border.all(1, ft.Colors.WHITE24),
        border_radius=8,
        heading_row_color=ft.Colors.with_opacity(0.2, ft.Colors.CYAN),
        column_spacing=40,
    )

    # --- Gráfica 1: Queries por segundo (LineChart) ---
    queries_chart = ft.LineChart(
        min_y=0,
        max_y=100,
        min_x=0,
        max_x=29,
        animate=ft.Animation(300, ft.AnimationCurve.EASE_IN_OUT),
        expand=True,
        tooltip_bgcolor=ft.Colors.with_opacity(0.8, ft.Colors.BLACK),
        left_axis=ft.ChartAxis(
            title=ft.Text("Queries/s", size=11, color=ft.Colors.WHITE70),
            labels_size=40,
        ),
        bottom_axis=ft.ChartAxis(
            title=ft.Text("Tiempo", size=11, color=ft.Colors.WHITE70),
            show_labels=False,
        ),
        horizontal_grid_lines=ft.ChartGridLines(color=ft.Colors.WHITE10, width=1),
        data_series=[],
    )

    # --- Gráfica 2: Conexiones Activas vs Máximas (BarChart) ---
    connections_chart = ft.BarChart(
        min_y=0,
        max_y=200,
        animate=ft.Animation(300, ft.AnimationCurve.EASE_IN_OUT),
        expand=True,
        tooltip_bgcolor=ft.Colors.with_opacity(0.8, ft.Colors.BLACK),
        left_axis=ft.ChartAxis(
            title=ft.Text("Conexiones", size=11, color=ft.Colors.WHITE70),
            labels_size=40,
        ),
        horizontal_grid_lines=ft.ChartGridLines(color=ft.Colors.WHITE10, width=1),
        bar_groups=[],
    )

    def make_status_card(title, value, icon, color):
        return ft.Container(
            content=ft.Column([
                ft.Row([ft.Icon(icon, color=color, size=20),
                        ft.Text(title, size=12, color=ft.Colors.WHITE70)]),
                ft.Text(str(value), size=22, weight=ft.FontWeight.BOLD, color=color),
            ], spacing=5, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
            bgcolor=ft.Colors.with_opacity(0.15, color),
            border_radius=12,
            padding=15,
            width=180,
        )

    def get_status_var(status_dict, var_name):
        return int(status_dict.get(var_name, 0))

    def update_data():
        try:
            # Obtener GLOBAL STATUS
            cols, rows = db.execute_query("SHOW GLOBAL STATUS")
            status = {row[0]: row[1] for row in rows}

            # Obtener variables
            cols2, rows2 = db.execute_query("SHOW GLOBAL VARIABLES")
            variables = {row[0]: row[1] for row in rows2}

            current_time = time.time()

            # Calcular queries/s
            current_stats = {
                "Com_select": get_status_var(status, "Com_select"),
                "Com_insert": get_status_var(status, "Com_insert"),
                "Com_update": get_status_var(status, "Com_update"),
                "Com_delete": get_status_var(status, "Com_delete"),
            }

            if monitor.prev_time and monitor.prev_stats:
                dt = current_time - monitor.prev_time
                if dt > 0:
                    rates = {}
                    for key in current_stats:
                        diff = current_stats[key] - monitor.prev_stats.get(key, current_stats[key])
                        rates[key] = max(0, diff / dt)
                    monitor.queries_history.append(rates)
                    if len(monitor.queries_history) > monitor.max_points:
                        monitor.queries_history.pop(0)

            monitor.prev_stats = current_stats
            monitor.prev_time = current_time

            # Conexiones
            threads_connected = get_status_var(status, "Threads_connected")
            max_connections = int(variables.get("max_connections", 151))
            threads_running = get_status_var(status, "Threads_running")
            monitor.connections_history = (threads_connected, max_connections, threads_running)

            # Variables adicionales para tarjetas
            uptime = get_status_var(status, "Uptime")
            questions = get_status_var(status, "Questions")
            slow_queries = get_status_var(status, "Slow_queries")
            bytes_received = get_status_var(status, "Bytes_received")
            bytes_sent = get_status_var(status, "Bytes_sent")
            buffer_pool_size = int(variables.get("innodb_buffer_pool_size", 0))

            return {
                "uptime": uptime,
                "questions": questions,
                "slow_queries": slow_queries,
                "bytes_received": bytes_received,
                "bytes_sent": bytes_sent,
                "threads_connected": threads_connected,
                "max_connections": max_connections,
                "threads_running": threads_running,
                "buffer_pool_size": buffer_pool_size,
                "qps": questions / max(uptime, 1),
            }
        except Exception:
            return None

    def format_bytes(b):
        for unit in ["B", "KB", "MB", "GB"]:
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} TB"

    def format_time(s):
        days = s // 86400
        hours = (s % 86400) // 3600
        mins = (s % 3600) // 60
        if days > 0:
            return f"{days}d {hours}h"
        if hours > 0:
            return f"{hours}h {mins}m"
        return f"{mins}m {s % 60}s"

    def refresh_ui():
        info = update_data()
        if not info:
            return

        # Tarjetas de estado
        status_cards_row.controls = [
            make_status_card("Uptime", format_time(info["uptime"]), ft.Icons.TIMER, ft.Colors.CYAN_300),
            make_status_card("QPS", f"{info['qps']:.1f}", ft.Icons.SPEED, ft.Colors.GREEN_300),
            make_status_card("Conexiones", f"{info['threads_connected']}/{info['max_connections']}",
                             ft.Icons.PEOPLE, ft.Colors.PURPLE_300),
            make_status_card("Slow Queries", str(info["slow_queries"]), ft.Icons.WARNING, ft.Colors.AMBER_300),
            make_status_card("Tráfico ↓", format_bytes(info["bytes_received"]), ft.Icons.DOWNLOAD,
                             ft.Colors.BLUE_300),
            make_status_card("Tráfico ↑", format_bytes(info["bytes_sent"]), ft.Icons.UPLOAD,
                             ft.Colors.ORANGE_300),
        ]

        # Actualizar gráfica de queries/s
        if monitor.queries_history:
            max_val = 1
            series_data = {"Com_select": [], "Com_insert": [], "Com_update": [], "Com_delete": []}
            colors = {
                "Com_select": ft.Colors.CYAN_400,
                "Com_insert": ft.Colors.GREEN_400,
                "Com_update": ft.Colors.AMBER_400,
                "Com_delete": ft.Colors.RED_400,
            }

            for i, rates in enumerate(monitor.queries_history):
                for key in series_data:
                    val = rates.get(key, 0)
                    series_data[key].append(ft.LineChartDataPoint(i, val))
                    max_val = max(max_val, val)

            queries_chart.max_y = max(max_val * 1.3, 10)
            queries_chart.max_x = max(len(monitor.queries_history) - 1, 1)
            queries_chart.data_series = [
                ft.LineChartData(
                    data_points=points,
                    color=colors[key],
                    stroke_width=2,
                    curved=True,
                    stroke_cap_round=True,
                )
                for key, points in series_data.items()
            ]

        # Actualizar gráfica de conexiones
        if monitor.connections_history:
            connected, max_conn, running_threads = monitor.connections_history
            connections_chart.max_y = max(max_conn * 1.1, 10)
            connections_chart.bar_groups = [
                ft.BarChartGroup(
                    x=0,
                    bar_rods=[ft.BarChartRod(
                        from_y=0, to_y=connected,
                        color=ft.Colors.PURPLE_400,
                        width=50, border_radius=4,
                        tooltip=f"Conectadas: {connected}",
                    )],
                ),
                ft.BarChartGroup(
                    x=1,
                    bar_rods=[ft.BarChartRod(
                        from_y=0, to_y=running_threads,
                        color=ft.Colors.GREEN_400,
                        width=50, border_radius=4,
                        tooltip=f"Activas: {running_threads}",
                    )],
                ),
                ft.BarChartGroup(
                    x=2,
                    bar_rods=[ft.BarChartRod(
                        from_y=0, to_y=max_conn,
                        color=ft.Colors.with_opacity(0.3, ft.Colors.GREY),
                        width=50, border_radius=4,
                        tooltip=f"Máximo: {max_conn}",
                    )],
                ),
            ]
            connections_chart.bottom_axis = ft.ChartAxis(
                labels=[
                    ft.ChartAxisLabel(value=0, label=ft.Text("Conectadas", size=10, color=ft.Colors.WHITE70)),
                    ft.ChartAxisLabel(value=1, label=ft.Text("Activas", size=10, color=ft.Colors.WHITE70)),
                    ft.ChartAxisLabel(value=2, label=ft.Text("Máximo", size=10, color=ft.Colors.WHITE70)),
                ],
                labels_size=30,
            )

        # Tabla de stats
        important_vars = [
            ("Uptime", format_time(info["uptime"])),
            ("Questions", f"{info['questions']:,}"),
            ("QPS (promedio)", f"{info['qps']:.2f}"),
            ("Slow_queries", str(info["slow_queries"])),
            ("Threads_connected", str(info["threads_connected"])),
            ("Threads_running", str(info["threads_running"])),
            ("Max_connections", str(info["max_connections"])),
            ("Bytes_received", format_bytes(info["bytes_received"])),
            ("Bytes_sent", format_bytes(info["bytes_sent"])),
            ("InnoDB Buffer Pool", format_bytes(info["buffer_pool_size"])),
        ]
        stats_table.rows = [
            ft.DataRow(cells=[
                ft.DataCell(ft.Text(v[0], color=ft.Colors.CYAN_200, size=12)),
                ft.DataCell(ft.Text(v[1], color=ft.Colors.WHITE, size=12)),
            ]) for v in important_vars
        ]

        page.update()

    def auto_refresh():
        while running.is_set():
            try:
                refresh_ui()
            except Exception:
                pass
            time.sleep(2)

    # Leyenda de la gráfica de queries
    legend = ft.Row([
        ft.Row([ft.Container(width=12, height=12, bgcolor=ft.Colors.CYAN_400, border_radius=3),
                ft.Text("SELECT", size=11, color=ft.Colors.WHITE70)]),
        ft.Row([ft.Container(width=12, height=12, bgcolor=ft.Colors.GREEN_400, border_radius=3),
                ft.Text("INSERT", size=11, color=ft.Colors.WHITE70)]),
        ft.Row([ft.Container(width=12, height=12, bgcolor=ft.Colors.AMBER_400, border_radius=3),
                ft.Text("UPDATE", size=11, color=ft.Colors.WHITE70)]),
        ft.Row([ft.Container(width=12, height=12, bgcolor=ft.Colors.RED_400, border_radius=3),
                ft.Text("DELETE", size=11, color=ft.Colors.WHITE70)]),
    ], spacing=15)

    # Iniciar hilo de refresco
    thread = threading.Thread(target=auto_refresh, daemon=True)
    thread.start()

    def on_dispose():
        running.clear()

    page.on_disconnect = lambda _: on_dispose()

    return ft.Container(
        content=ft.Column(
            [
                ft.Row([
                    ft.Icon(ft.Icons.MONITOR_HEART, size=30, color=ft.Colors.CYAN_300),
                    ft.Text("Monitoreo de Rendimiento", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.WHITE),
                ]),
                ft.Divider(color=ft.Colors.WHITE24),
                status_cards_row,
                ft.Container(height=10),
                # Gráfica 1 - Queries/s
                ft.Container(
                    content=ft.Column([
                        ft.Text("📈 Queries por Segundo (tiempo real)", size=16, weight=ft.FontWeight.W_600,
                                color=ft.Colors.CYAN_200),
                        legend,
                        ft.Container(content=queries_chart, height=220),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.1, ft.Colors.CYAN),
                    border_radius=12, padding=20,
                ),
                ft.Container(height=10),
                # Gráfica 2 - Conexiones
                ft.Container(
                    content=ft.Column([
                        ft.Text("📊 Conexiones (Activas vs Máximo)", size=16, weight=ft.FontWeight.W_600,
                                color=ft.Colors.PURPLE_200),
                        ft.Container(content=connections_chart, height=200),
                    ]),
                    bgcolor=ft.Colors.with_opacity(0.1, ft.Colors.PURPLE),
                    border_radius=12, padding=20,
                ),
                ft.Container(height=10),
                # Tabla de variables
                ft.Container(
                    content=ft.Column([
                        ft.Text("📋 Variables del Servidor", size=16, weight=ft.FontWeight.W_600,
                                color=ft.Colors.WHITE70),
                        stats_table,
                    ], scroll=ft.ScrollMode.AUTO),
                    bgcolor=ft.Colors.with_opacity(0.05, ft.Colors.WHITE),
                    border_radius=12, padding=15,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=10,
        ),
        padding=25,
        expand=True,
    )
