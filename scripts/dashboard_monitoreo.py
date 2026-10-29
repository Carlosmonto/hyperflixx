"""Dashboard de monitoreo de la arquitectura de datos (sample_mflix).

Genera un PNG con 6 paneles:
    1. Documentos por coleccion, coloreados por capa (OLTP / OLAP / Data Warehouse)
    2. Latencia de las 10 consultas del benchmark vs SLA, con el plan real (IXSCAN/COLLSCAN)
    3. Almacenamiento real por coleccion (datos vs indices), leido de $collStats
    4. Tendencia mensual de comentarios (el mes en curso se marca como parcial)
    5. Uso del limite de almacenamiento del plan de Atlas
    6. Resumen ejecutivo

Ejecucion:
    python scripts/dashboard_monitoreo.py              # usa reportes/benchmark_ultimo.json
    python scripts/dashboard_monitoreo.py --benchmark  # ejecuta el benchmark antes

Variables opcionales (.env):
    ATLAS_PLAN="M0 (gratuito)"   ATLAS_COSTO_MENSUAL=0   ATLAS_LIMITE_MB=512
"""

import json, os, statistics, sys, time
from datetime import datetime
from pathlib import Path
import matplotlib
matplotlib.use("Agg")  # genera el PNG sin abrir ventanas
import matplotlib.pyplot as plt
from pymongo.errors import OperationFailure
RAIZ = Path(__file__).resolve().parents[1]
sys.path.append(str(RAIZ))
sys.path.append(str(Path(__file__).resolve().parent))

CARPETA_REPORTES = RAIZ / "reportes"
ARCHIVO_BENCHMARK = CARPETA_REPORTES / "benchmark_ultimo.json"
PLAN_ATLAS = os.getenv("ATLAS_PLAN", "M0 (gratuito)")
COSTO_MENSUAL = float(os.getenv("ATLAS_COSTO_MENSUAL", "0"))
LIMITE_MB = float(os.getenv("ATLAS_LIMITE_MB", "512"))
MESES_TENDENCIA = 24

CAPAS = {"OLTP": "#4C72B0", "OLAP": "#DD8452", "Data Warehouse": "#55A868", "Otra": "#999999"}
OLTP = {"movies", "comments", "users", "theaters", "sessions", "embedded_movies"}


def capa_de(nombre):
    if nombre in OLTP:
        return "OLTP"
    if nombre == "peliculas_analiticas":
        return "OLAP"
    if nombre.startswith(("dim_", "fact_", "agg_")):
        return "Data Warehouse"
    return "Otra"


# ---------------------------------------------------------------- recoleccion

def estadisticas_coleccion(db, nombre):
    try:
        stats = next(db[nombre].aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
    except (OperationFailure, StopIteration):
        stats = db.command("collStats", nombre)  # respaldo para servidores antiguos
    return {"nombre": nombre, "capa": capa_de(nombre), "documentos": stats.get("count", 0),
            "datos_mb": stats.get("storageSize", 0) / 1024 ** 2,
            "indices_mb": stats.get("totalIndexSize", 0) / 1024 ** 2}


def tendencia_mensual(db):
    if db.fact_comentarios.estimated_document_count() > 0:
        fuente, mes = "fact_comentarios", {"$substrBytes": ["$fecha_id", 0, 7]}
    else:
        fuente, mes = "comments", {"$dateToString": {"format": "%Y-%m", "date": "$date"}}
    filas = list(db[fuente].aggregate([
        {"$group": {"_id": mes, "comentarios": {"$sum": 1}}},
        {"$match": {"_id": {"$ne": None}}},
        {"$sort": {"_id": -1}},
        {"$limit": MESES_TENDENCIA},
        {"$sort": {"_id": 1}},
    ], allowDiskUse=True))
    return fuente, [(f["_id"], f["comentarios"]) for f in filas]


def cargar_benchmark(ejecutar):
    if ejecutar or not ARCHIVO_BENCHMARK.exists():
        print("[DASHBOARD] Ejecutando benchmark de consultas...")
        from benchmark_consultas_analiticas import ejecutar_benchmark
        return ejecutar_benchmark(guardar=True)
    print(f"[DASHBOARD] Usando benchmark guardado: {ARCHIVO_BENCHMARK.name}")
    return json.loads(ARCHIVO_BENCHMARK.read_text(encoding="utf-8"))


def recolectar_metricas(db, cliente, benchmark):
    colecciones = [estadisticas_coleccion(db, n)
                   for n in sorted(db.list_collection_names(filter={"type": "collection"}))
                   if not n.startswith("system.")]
    pings = []
    for _ in range(5):
        inicio = time.perf_counter()
        cliente.admin.command("ping")
        pings.append((time.perf_counter() - inicio) * 1000)
    fuente, tendencia = tendencia_mensual(db)
    return {"fecha": datetime.now(), "base": db.name, "colecciones": colecciones,
            "latencia_red_ms": statistics.median(pings), "fuente_tendencia": fuente,
            "tendencia": tendencia, "benchmark": benchmark}


# ---------------------------------------------------------------- graficos

def panel_documentos(ax, m):
    cols = sorted(m["colecciones"], key=lambda c: c["documentos"])
    barras = ax.barh([c["nombre"] for c in cols], [c["documentos"] for c in cols],
                     color=[CAPAS[c["capa"]] for c in cols])
    ax.bar_label(barras, labels=[f"{c['documentos']:,}" for c in cols], padding=3, fontsize=8)
    ax.set_title("Documentos por coleccion")
    ax.set_xlabel("Documentos")
    ax.margins(x=0.2)
    capas = sorted({c["capa"] for c in cols})
    ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=CAPAS[c]) for c in capas], labels=capas,
              fontsize=8, loc="lower right")


def panel_latencias(ax, m):
    b = m["benchmark"]
    consultas = [c for c in (b or {}).get("consultas", []) if c.get("estado") == "OK"]
    ax.set_title("Latencia de consultas analiticas (mediana)")
    if not consultas:
        ax.text(0.5, 0.5, "Sin datos de benchmark.\nEjecute con --benchmark", ha="center", va="center")
        ax.set_axis_off()
        return
    sla = b["sla_ms"]
    barras = ax.bar([f"C{c['numero']}" for c in consultas], [c["latencia_ms"] for c in consultas],
                    color=["#55A868" if c["dentro_sla"] else "#C44E52" for c in consultas])
    abreviado = {"IXSCAN": "IX", "COLLSCAN": "COL", "MIXTO": "MIX", "DESCONOCIDO": "?"}
    ax.bar_label(barras, labels=[abreviado.get(c["plan"], "?") for c in consultas], fontsize=7, padding=2)
    ax.axhline(sla, color="#C44E52", linestyle="--", linewidth=1, label=f"SLA {sla:.0f} ms")
    ax.axhline(m["latencia_red_ms"], color="#777777", linestyle=":", linewidth=1,
               label=f"Red (ping) {m['latencia_red_ms']:.0f} ms")
    ax.set_ylabel("ms")
    ax.set_ylim(0, max(sla, max(c["latencia_ms"] for c in consultas)) * 1.15)
    ax.legend(fontsize=8, loc="upper left")
    ax.text(0.99, 0.97, "IX = usa indice  COL = recorre coleccion", transform=ax.transAxes,
            ha="right", va="top", fontsize=7, color="#555555")


def panel_almacenamiento(ax, m):
    cols = sorted(m["colecciones"], key=lambda c: c["datos_mb"] + c["indices_mb"])
    nombres = [c["nombre"] for c in cols]
    datos = [c["datos_mb"] for c in cols]
    ax.barh(nombres, datos, color="#4C72B0", label="Datos")
    ax.barh(nombres, [c["indices_mb"] for c in cols], left=datos, color="#8172B3", label="Indices")
    ax.set_title("Almacenamiento por coleccion (comprimido)")
    ax.set_xlabel("MB")
    ax.legend(fontsize=8, loc="lower right")


def panel_tendencia(ax, m):
    ax.set_title(f"Tendencia mensual de comentarios ({m['fuente_tendencia']})")
    puntos = m["tendencia"]
    if not puntos:
        ax.text(0.5, 0.5, "Sin comentarios con fecha", ha="center", va="center")
        ax.set_axis_off()
        return
    meses, valores = [p[0] for p in puntos], [p[1] for p in puntos]
    parcial = meses[-1] == m["fecha"].strftime("%Y-%m")
    fin = len(meses) - 1 if parcial else len(meses)
    ax.plot(meses[:fin], valores[:fin], marker="o", color="#4C72B0", linewidth=1.5)
    if parcial:
        # El mes en curso aun no termina: se dibuja aparte para no leerlo como una caida.
        ax.plot(meses[-2:], valores[-2:], linestyle="--", color="#999999")
        ax.plot(meses[-1], valores[-1], marker="o", markerfacecolor="white", color="#999999")
        ax.annotate("mes en curso\n(parcial)", (len(meses) - 1, valores[-1]), textcoords="offset points",
                    xytext=(-10, 12), ha="right", fontsize=7, color="#555555")
    ax.set_ylabel("Comentarios")
    paso = max(1, len(meses) // 12)
    ax.set_xticks(range(0, len(meses), paso))
    ax.set_xticklabels(meses[::paso], rotation=45, ha="right", fontsize=8)
    ax.grid(alpha=0.3)


def panel_limite(ax, m, usado_mb):
    pct = usado_mb / LIMITE_MB * 100 if LIMITE_MB else 0
    color = "#55A868" if pct < 60 else "#DD8452" if pct < 80 else "#C44E52"
    ax.barh([0], [LIMITE_MB], color="#EEEEEE", height=0.5)
    ax.barh([0], [min(usado_mb, LIMITE_MB)], color=color, height=0.5)
    ax.axvline(LIMITE_MB * 0.8, color="#C44E52", linestyle="--", linewidth=1, label="80% del limite")
    ax.text(LIMITE_MB / 2, 0, f"{usado_mb:,.1f} MB de {LIMITE_MB:,.0f} MB ({pct:.1f}%)",
            ha="center", va="center", fontsize=10, fontweight="bold")
    ax.set_title(f"Uso del almacenamiento del plan: {PLAN_ATLAS}")
    ax.set_xlim(0, LIMITE_MB)
    ax.set_yticks([])
    ax.set_xlabel("MB (datos + indices)")
    ax.legend(fontsize=8, loc="upper right")


def texto_resumen(m, total_docs, usado_mb):
    b = m["benchmark"] or {}
    ejecutadas = b.get("ejecutadas", 0)
    pct = usado_mb / LIMITE_MB * 100 if LIMITE_MB else 0
    alertas = []
    if ejecutadas and b.get("dentro_sla", 0) < ejecutadas:
        alertas.append("consultas fuera de SLA")
    if pct >= 80:
        alertas.append("almacenamiento >= 80%")
    estado = "OPERATIVO" if not alertas else "ATENCION: " + ", ".join(alertas)
    lineas = [
        "RESUMEN EJECUTIVO DEL SISTEMA",
        "-" * 38,
        f"Base de datos:        {m['base']}",
        f"Colecciones activas:  {len(m['colecciones'])}",
        f"Total documentos:     {total_docs:,}",
        f"Almacenamiento:       {usado_mb:,.1f} MB",
        f"Latencia de red:      {m['latencia_red_ms']:.1f} ms",
    ]
    if ejecutadas:
        lineas += [
            f"Latencia mediana:     {b['latencia_mediana_ms']} ms",
            f"Latencia maxima:      {b['latencia_maxima_ms']} ms",
            f"Dentro de SLA:        {b['dentro_sla']}/{ejecutadas}",
            f"Usan indice:          {b['con_indice']}/{ejecutadas}",
        ]
    else:
        lineas.append("Benchmark:            sin datos")
    lineas += [
        f"Meses con datos:      {len(m['tendencia'])} (ultimos {MESES_TENDENCIA})",
        f"Plan / costo mensual: {PLAN_ATLAS} / ${COSTO_MENSUAL:,.2f}",
        "-" * 38,
        f"Estado: {estado}",
    ]
    return "\n".join(lineas)


def dibujar_dashboard(m, ruta):
    total_docs = sum(c["documentos"] for c in m["colecciones"])
    usado_mb = sum(c["datos_mb"] + c["indices_mb"] for c in m["colecciones"])
    fig, ejes = plt.subplots(2, 3, figsize=(20, 11))
    fig.suptitle(f"DASHBOARD DE MONITOREO - Arquitectura de datos {m['base']}   "
                 f"({m['fecha']:%Y-%m-%d %H:%M})", fontsize=15, fontweight="bold")
    panel_documentos(ejes[0][0], m)
    panel_latencias(ejes[0][1], m)
    panel_almacenamiento(ejes[0][2], m)
    panel_tendencia(ejes[1][0], m)
    panel_limite(ejes[1][1], m, usado_mb)
    ax = ejes[1][2]
    ax.set_axis_off()
    ax.text(0.02, 0.98, texto_resumen(m, total_docs, usado_mb), family="monospace", fontsize=10,
            va="top", ha="left", transform=ax.transAxes,
            bbox={"boxstyle": "round,pad=0.8", "facecolor": "#F7F2E4", "edgecolor": "#999999"})
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(ruta, dpi=120)
    plt.close(fig)
    return total_docs, usado_mb


def generar_dashboard():
    from conexion import conectar_mongodb
    benchmark = cargar_benchmark("--benchmark" in sys.argv)
    cliente, db = conectar_mongodb()
    try:
        print("[DASHBOARD] Recolectando metricas de Atlas...")
        metricas = recolectar_metricas(db, cliente, benchmark)
    finally:
        cliente.close()
        print("[DASHBOARD] Conexion cerrada.")

    CARPETA_REPORTES.mkdir(exist_ok=True)
    ruta = CARPETA_REPORTES / f"dashboard_monitoreo_{metricas['fecha']:%Y%m%d_%H%M%S}.png"
    total_docs, usado_mb = dibujar_dashboard(metricas, ruta)
    b = benchmark or {}
    print("\n" + "=" * 70)
    print(" ESTADISTICAS DEL DASHBOARD")
    print("=" * 70)
    print(f" Total de documentos en el sistema: {total_docs:,}")
    print(f" Almacenamiento (datos + indices):  {usado_mb:,.1f} MB de {LIMITE_MB:,.0f} MB")
    print(f" Latencia de red (ping):            {metricas['latencia_red_ms']:.2f} ms")
    if b.get("ejecutadas"):
        print(f" Latencia mediana de consultas:     {b['latencia_mediana_ms']} ms "
              f"({b['dentro_sla']}/{b['ejecutadas']} dentro de SLA)")
    print(f" Plan / costo mensual:              {PLAN_ATLAS} / ${COSTO_MENSUAL:,.2f}")
    print(f" Meses con datos:                   {len(metricas['tendencia'])}")
    print("=" * 70)
    print(f" Dashboard generado: reportes/{ruta.name}")
    print("=" * 70)


if __name__ == "__main__":
    generar_dashboard()
