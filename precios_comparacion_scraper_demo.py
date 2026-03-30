"""
PROYECTO NIVEL SENIOR - Competitive Price Intelligence System
Autor: Portfolio Demo

Arquitectura:
- Scrapy (multi-spider)
- Pipeline de procesamiento
- Normalización de datos
- SQLite (persistencia)
- APScheduler (automatización)
- Flask API (exposición de datos)
- Rotación de User-Agent
- Sistema de alertas desacoplado

Este archivo es una versión compacta (single-file) pensada para portfolio.
"""

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import sqlite3
from datetime import datetime
import random
import threading

# ==========================
# CONFIG
# ==========================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
]

# ==========================
# BASE DE DATOS
# ==========================
class PriceDB:
    def __init__(self, db_name="prices.db"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self._create()

    def _create(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY,
            product TEXT,
            source TEXT,
            price REAL,
            url TEXT,
            timestamp TEXT
        )
        """)
        self.conn.commit()

    def insert(self, item):
        self.conn.execute(
            "INSERT INTO prices (product, source, price, url, timestamp) VALUES (?, ?, ?, ?, ?)",
            (item['product'], item['source'], item['price'], item['url'], datetime.now().isoformat())
        )
        self.conn.commit()

    def last_price(self, product, source):
        cur = self.conn.execute(
            "SELECT price FROM prices WHERE product=? AND source=? ORDER BY id DESC LIMIT 1",
            (product, source)
        )
        r = cur.fetchone()
        return r[0] if r else None

# ==========================
# ALERTAS
# ==========================
class AlertService:
    def notify(self, product, source, old, new):
        print(f"[ALERTA] {product} ({source}): {old} -> {new}")

# ==========================
# ITEM PIPELINE
# ==========================
class PricePipeline:
    def __init__(self, db, alerts):
        self.db = db
        self.alerts = alerts

    def process(self, item):
        last = self.db.last_price(item['product'], item['source'])
        if last and last != item['price']:
            self.alerts.notify(item['product'], item['source'], last, item['price'])
        self.db.insert(item)

# ==========================
# MIDDLEWARE (UA ROTATION)
# ==========================
class RandomUserAgentMiddleware:
    def process_request(self, request):
        request.headers['User-Agent'] = random.choice(USER_AGENTS)

# ==========================
# SPIDERS
# ==========================
class BaseSpider(scrapy.Spider):
    custom_settings = {
        "LOG_LEVEL": "ERROR"
    }

    def __init__(self, pipeline, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline

    def handle_item(self, item):
        self.pipeline.process(item)
        return item


class DemoStoreSpider(BaseSpider):
    name = "demo_store"
    start_urls = ["https://example.com/product1"]

    def parse(self, response):
        product = response.css("h1::text").get() or "Demo Product"
        price = float((response.css(".price::text").get() or "$0").replace("$", ""))

        yield self.handle_item({
            "product": product,
            "price": price,
            "url": response.url,
            "source": "demo_store"
        })


class SecondStoreSpider(BaseSpider):
    name = "second_store"
    start_urls = ["https://example.com/product2"]

    def parse(self, response):
        product = response.css("h1::text").get() or "Demo Product"
        price = float((response.css(".price::text").get() or "$0").replace("$", ""))

        yield self.handle_item({
            "product": product,
            "price": price,
            "url": response.url,
            "source": "second_store"
        })

# ==========================
# SCHEDULER
# ==========================
def scheduled_run(process, spiders, interval=60):
    import time
    while True:
        print("[SCHEDULER] Running scraping cycle...")
        for sp in spiders:
            process.crawl(sp)
        process.start(stop_after_crawl=True)
        time.sleep(interval)

# ==========================
# API (FLASK)
# ==========================
from flask import Flask, jsonify

app = Flask(__name__)
GLOBAL_DB = PriceDB()

@app.route("/prices")
def get_prices():
    cur = GLOBAL_DB.conn.execute("SELECT product, source, price, timestamp FROM prices ORDER BY id DESC LIMIT 20")
    data = [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]
    return jsonify(data)

# ==========================
# BOOTSTRAP
# ==========================
def run_system():
    db = GLOBAL_DB
    alerts = AlertService()
    pipeline = PricePipeline(db, alerts)

    process = CrawlerProcess(get_project_settings())

    spiders = [
        lambda: DemoStoreSpider(pipeline=pipeline),
        lambda: SecondStoreSpider(pipeline=pipeline)
    ]

    t = threading.Thread(target=scheduled_run, args=(process, spiders, 120))
    t.daemon = True
    t.start()

    app.run(port=5000)

# ==========================
# ENTRYPOINT
# ==========================
if __name__ == "__main__":
    run_system()

# ==========================
# EXTENSIONES SENIOR (IDEAS)
# ==========================
"""
- Integración con Redis + Scrapy Cluster
- Proxy Pool dinámico
- Detección de anti-bot (Cloudflare bypass)
- ML para predicción de precios
- Docker + CI/CD
- Multi-thread scraping distribuido
- Dashboard React + WebSockets
"""
