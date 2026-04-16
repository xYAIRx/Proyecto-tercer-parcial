# 💠 DB Manager — MariaDB

¡Bienvenido a ** DB Manager**! Un robusto y elegante Sistema Gestor de Bases de Datos diseñado exclusivamente para **MariaDB** construido completamente sobre Python y [Flet](https://flet.dev/). DB te proporciona una inferfaz de escritorio visualmente atractiva (Dark Theme), altamente responsiva y llena de utilidades críticas para administración y gestión de servidores locales o remotos.

---

## ✨ Características Principales

El ecosistema está dividido en submódulos modulares accesibles a través del menú lateral:

* 🛡️ **Conexión Segura (Login):** Panel de autenticación al servidor conectando a través del host, puerto, usuario e interfaz de MariaDB.
* 📦 **Respaldo y Restauración Automático (`backup_restore.py`):**
  * Respaldos inteligentes de bases de datos haciendo uso de `mysqldump` adaptados a arquitecturas de MariaDB.
  * Módulo capaz de exportar e importar `.sql` **globales** (con capacidad de construir sus propias bases de datos).
* 🔄 **Exportación e Importación Dinámica (`export_import.py`):**
  * Migraciones limpias de datos por tablas específicas o por **bloque global completo**.
  * Soporte activo para formatos: **JSON** (diccionarios estructurados), **SQL** (sentencias insert) y nuestro formato avanzado de bloque en **CSV múltiple**.
* 👥 **Administración de Usuarios (`user_admin.py`):** Modificación, creación, eliminación y asignación intuitiva de privilegios (`GRANT`/`REVOKE`).
* 📊 **Monitoreo en Vivo (`monitoring.py`):** Consola de métricas en tiempo real sobre el estado vital del servidor base de datos.
* 💻 **Consola Interactiva (`console.py`):** Sandbox directo para ejecutar y desplegar cualquier tipo de sentencias o rutinas nativas SQL.

---

## 🛠️ Prerequisitos y Dependencias

Para poder ejecutar la aplicación, tu sistema debe de satisfacer lo siguiente:

1. **Python 3.10+**
2. **Sistema CLI de bases de datos en PATH:** Tanto las utilerías de cliente genéricas de `mysql` como `mysqldump` deben de estar reconocidas por las variables de entorno (`PATH`) de tu Sistema Operativo.

Asegúrate de instalar los requerimientos oficiales del proyecto.
```bash
pip install -r requirements.txt
```
*(Librerías usadas: `flet>=0.25.0`, `mysql-connector-python>=9.0.0`)*

---

## 🚀 Instalación y Despliegue

1. **Clonar este repositorio:**
    ```bash
    git clone https://github.com/xYAIRx/Proyecto-tercer-parcial.git
    cd 
    ```

2. **Asegurar dependencias instaladas** (revisa los prerequisitos arriba).

3. **Ejecutar DB Manager:** Utilizar directamente el archivo de arranque principal.
    ```bash
    python main.py
    ```

Verás aparecer una ventana flotante de alta resolución con un panel de acceso. Solo ingresa `localhost`, `3306`, y tus credenciales nativas de root u operario, ¡y a gestionar se ha dicho!

---

## 💡 Notas Especiales para Usuarios Windows

Si a la hora de hacer copias de seguridad o restauración el programa te arroja errores de "*No se encuentra mysqldump*", asegúrate de haber marcado la casilla de agregar a la lista binaria **"Add to PATH"** en el instalador de MariaDB.
Alternativamente, la ruta por defecto suele ser `C:\Program Files\MariaDB [version]\bin`. Añádela a tus variables de entorno en *Propiedades del Sistema > Variables de Entorno > Path*.

---
*Desarrollado por Barrios Nava Yair, Felipe Estrella Bautista, Hernandez Barragan Cristian y Mendoza Balderas Yaneli.*
