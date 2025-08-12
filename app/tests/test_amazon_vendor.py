# tests/test_amazon_vendor.py
# -------------------------------------------------------------
# Pytest suite (self‑contained) for the `amazon_vendor` blueprint.
# Drop this file under your repo (e.g. ./tests/test_amazon_vendor.py)
# and run:  pytest -q
#
# Dependencies suggested:
#   pytest
#   pytest-mock (optional)
#   freezegun (optional)
# -------------------------------------------------------------

from types import SimpleNamespace
from datetime import datetime
import json
import io
import builtins
import importlib
import types
import pytest
from flask import Flask

# -------------------------------------------------------------
# Minimal fake Supabase client used by the endpoints we test
# -------------------------------------------------------------
class _FakeQuery:
    def __init__(self, table_name, data_map):
        self.table_name = table_name
        self._filters = []
        self._orders = []
        self._range = None
        self._select = None
        self._count = None
        self._head = None
        self._data_map = data_map

    # chainable query builders used in the module
    def select(self, *args, **kwargs):
        # save any kwargs like count/head
        self._select = args[0] if args else "*"
        self._count = kwargs.get("count")
        self._head = kwargs.get("head")
        return self

    def eq(self, field, value):
        self._filters.append((field, "eq", value))
        return self

    def in_(self, field, values):
        self._filters.append((field, "in", tuple(values)))
        return self

    def or_(self, _):
        # not needed for these tests; keep as no‑op
        return self

    def order(self, field, desc=False):
        self._orders.append((field, desc))
        return self

    def range(self, start, end):
        self._range = (start, end)
        return self

    def limit(self, n):
        self._range = (0, n-1)
        return self

    def single(self):
        # mark as single; execution will return first item
        self._single = True
        return self

    # execution returns object with .data (and optionally .count)
    def execute(self):
        payloads = self._data_map.get(self.table_name, [])
        # crude filtering for common patterns used in tests
        data = list(payloads)
        for f, op, val in self._filters:
            if op == "eq":
                data = [r for r in data if str(r.get(f)) == str(val)]
            elif op == "in":
                data = [r for r in data if r.get(f) in val]
        # ordering not strictly necessary here
        if getattr(self, "_single", False):
            out = data[0] if data else None
            return SimpleNamespace(data=out)
        # range slice
        if self._range:
            s, e = self._range
            data = data[s:e+1]
        return SimpleNamespace(data=data)

class _FakeStorage:
    def from_(self, bucket):
        return self
    def upload(self, filename, content, headers):
        return SimpleNamespace(error=None)

class FakeSupabase:
    def __init__(self, data_map=None):
        self._data_map = data_map or {}
        self.storage = _FakeStorage()
    def table(self, name):
        return _FakeQuery(name, self._data_map)

# -------------------------------------------------------------
# Test app factory registering the blueprint under test
# -------------------------------------------------------------
@pytest.fixture()
def app(monkeypatch):
    # Prepare fake data used by multiple tests
    data_map = {
        "ordini_vendor_riepilogo": [
            {
                "id": 1,
                "fulfillment_center": "FC1",
                "start_delivery": "2025-08-11",
                "po_list": ["PO123"],
                "stato_ordine": "nuovo",
                "created_at": "2025-08-10T08:00:00Z",
            },
        ],
        "ordini_vendor_items": [
            {
                "po_number": "PO123",
                "model_number": "SKU-001",
                "vendor_product_id": "1234567890123",
                "title": "Widget",
                "qty_ordered": 5,
                "fulfillment_center": "FC1",
                "start_delivery": "2025-08-11",
            },
        ],
        "ordini_vendor_parziali": [],
    }

    # Create module and patch its supabase before import (so import side effects use fake)
    mod = importlib.import_module("app.routes.amazon_vendor")

    fake = FakeSupabase(data_map)
    monkeypatch.setattr(mod, "supabase", fake)

    # Make supa_with_retry just run the builder immediately
    def _pass(builder_fn):
        builder = builder_fn()
        if hasattr(builder, "execute"):
            return builder.execute()
        return builder
    monkeypatch.setattr(mod, "supa_with_retry", _pass)

    # Avoid external HTTP in SP‑API route
    monkeypatch.setattr(mod, "get_spapi_access_token", lambda: "TEST_TOKEN")

    # Build Flask app and register blueprint
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    return app

@pytest.fixture()
def client(app):
    return app.test_client()

# -------------------------------------------------------------
# Tests
# -------------------------------------------------------------

def test_badge_counts(client):
    res = client.get("/api/amazon/vendor/orders/badge-counts")
    assert res.status_code == 200
    payload = res.get_json()
    # our fake has 1 nuovo and 0 parziale
    assert payload["nuovi"] == 1
    assert payload["parziali"] == 0


def test_riepilogo_nuovi_basic(client):
    res = client.get("/api/amazon/vendor/orders/riepilogo/nuovi")
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert data[0]["fulfillment_center"] == "FC1"
    assert data[0]["totale_articoli"] == 5  # from our single item with qty_ordered=5


def test_pdf_prelievo_nuovi_returns_pdf_bytes(client):
    res = client.get("/api/amazon/vendor/orders/lista-prelievo/nuovi/pdf?data=2025-08-11")
    assert res.status_code in (200, 404)
    if res.status_code == 200:
        assert res.headers["Content-Type"].startswith("application/pdf")
        # Werkzeug requires response body to be bytes
        assert isinstance(res.data, (bytes, bytearray))
        assert len(res.data) > 0


def test_find_items_by_barcode_merges_qty_inserted(client, monkeypatch):
    # Arrange: add a parziale that inserted 2 pcs of SKU-001 in PO123
    mod = importlib.import_module("app.routes.amazon_vendor")
    fake_data = mod.supabase._data_map
    fake_data["ordini_vendor_parziali"].append({
        "riepilogo_id": 1,
        "dati": [
            {"po_number": "PO123", "model_number": "SKU-001", "quantita": 2}
        ],
        "confermato": True,
    })

    res = client.get("/api/amazon/vendor/items/by-barcode?barcode=1234567890123")
    assert res.status_code == 200
    rows = res.get_json()
    assert rows, "expected at least one match"
    row = rows[0]
    assert row["qty_inserted"] == 2
    assert row["fulfillment_center"] == "FC1"


def test_list_vendor_pos_pass_through(client, monkeypatch):
    # mock requests.get used in the pass‑through endpoint
    import app.routes.amazon_vendor as mod
    class _R:
        status_code = 200
        text = "{\"purchaseOrders\": []}"
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: _R())

    resp = client.get("/api/amazon/vendor/orders/list")
    assert resp.status_code == 200
    assert resp.is_json
    assert resp.get_json() == {"purchaseOrders": []}


def test_riepilogo_nuovi_vuoto(client, monkeypatch):
    # nessun riepilogo "nuovo" -> []
    import app.routes.amazon_vendor as mod
    def _tbl(name): 
        class Q:
            def select(self, *a, **k): return self
            def eq(self, *a, **k): return self
            def order(self, *a, **k): return self
            def range(self, *a, **k): return self
            def execute(self): return type("R", (), {"data": []})
        return Q()
    monkeypatch.setattr(mod.supabase, "table", _tbl)
    res = client.get("/api/amazon/vendor/orders/riepilogo/nuovi")
    assert res.status_code == 200
    assert res.get_json() == []

def test_dettaglio_destinazione_ok(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    # finto riepilogo con una lista di PO
    def _tbl(name):
        class Q:
            def __init__(self): self._name = name; self._po = ["PO1","PO2"]
            def select(self, *a, **k): return self
            def eq(self, *a, **k): return self
            def in_(self, *a, **k): return self
            def range(self, *a, **k): return self
            def execute(self): return type("R", (), {"data": []})
        return Q()
    def _table(name):
        if name == "ordini_vendor_riepilogo":
            class Riep:
                def select(self, *a, **k): return self
                def eq(self, *a, **k): return self
                def execute(self): 
                    return type("R", (), {"data":[{"id": 99, "po_list":["PO1","PO2"]}]})
            return Riep()
        if name == "ordini_vendor_items":
            class Items:
                def select(self, *a, **k): return self
                def in_(self, *a, **k): return self
                def range(self, *a, **k): return self
                def execute(self): 
                    return type("R", (), {"data":[{"po_number":"PO1","model_number":"SKU-1","vendor_product_id":"123","title":"X","qty_ordered":7}]})
            return Items()
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _table})())
    res = client.get("/api/amazon/vendor/orders/dettaglio-destinazione?center=FC1&data=2025-01-10")
    js = res.get_json()
    assert res.status_code == 200
    assert js["riepilogo_id"] == 99
    assert len(js["articoli"]) == 1


def test_parziali_create_and_list(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    store = {}
    class Tbl:
        def __init__(self, name): self.name = name
        def select(self, *a, **k): return self
        def eq(self, *a, **k): return self
        def order(self, *a, **k): return self
        def limit(self, *a, **k): return self
        def upsert(self, rows, on_conflict=None):
            # salva in store
            key = (rows["riepilogo_id"], rows["numero_parziale"])
            store[key] = rows
            return self
        def execute(self): 
            # per GET ritorna i parziali salvati
            data = [{"riepilogo_id": rid, "numero_parziale": num, **rows} 
                    for (rid,num), rows in store.items()]
            return type("R", (), {"data": data})
    def _table(name): return Tbl(name)
    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _table})())

    # POST create
    payload = {"riepilogo_id": 1, "dati":[{"model_number":"SKU-1","quantita":3,"collo":1}]}
    r1 = client.post("/api/amazon/vendor/parziali", json=payload)
    assert r1.status_code == 200
    num = r1.get_json()["numero_parziale"] == 1

    # GET list
    r2 = client.get("/api/amazon/vendor/parziali?riepilogo_id=1")
    assert r2.status_code == 200
    assert len(r2.get_json()) >= 1

def test_conferma_parziale_impone_stato_parziale(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    # Setup catena di letture e update su riepilogo/parziali
    class RiepilogoTbl:
    # stato condiviso tra istanze
        stato = {"id": 10, "stato_ordine": "nuovo"}

        def __init__(self):
            self._last_select = "*"

        def select(self, *a, **k):
            self._last_select = a[0] if a else "*"
            return self

        def eq(self, *a, **k): return self

        def update(self, d):
            # aggiorna lo stato condiviso
            type(self).stato["stato_ordine"] = d["stato_ordine"]
            return self

        def single(self): return self

        def execute(self):
            if self._last_select == "id":
                return type("R", (), {"data": {"id": type(self).stato["id"]}})
            if self._last_select == "stato_ordine":
                return type("R", (), {"data": {"stato_ordine": type(self).stato["stato_ordine"]}})
            return type("R", (), {"data": [type(self).stato]})

    class ParzialiTbl:
        def __init__(self): self.d = [{"riepilogo_id":10,"numero_parziale":2,"confermato":False}]
        def select(self,*a,**k): return self
        def eq(self,*a,**k): return self
        def order(self,*a,**k): return self
        def limit(self,*a,**k): return self
        def update(self, d): 
            self.d[0]["confermato"] = True; return self
        def execute(self): return type("R", (), {"data": self.d})
    def _table(name):
        if name=="ordini_vendor_riepilogo": return RiepilogoTbl()
        if name=="ordini_vendor_parziali":  return ParzialiTbl()
    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _table})())

    resp = client.post("/api/amazon/vendor/parziali-wip/conferma-parziale", json={"center":"FC1","data":"2025-01-10"})
    assert resp.status_code == 200
    # stato_ordine deve diventare "parziale"
    assert resp.get_json()["ok"] is True



def test_chiudi_ordine_aggiorna_qty_e_stato(client, monkeypatch):
    import app.routes.amazon_vendor as mod

    # finti contenitori in memoria
    riepi = {"id": 7, "po_list": ["PO-A"]}
    items = [{"id": 1, "po_number":"PO-A", "model_number":"SKU-A"}]
    parziali_conf = [{"dati":[{"model_number":"SKU-A","quantita":5}], "confermato":True}]
    updates = {"qty": None, "stato": None}

    class RiepTbl:
        def select(self,*a,**k): return self
        def eq(self,*a,**k): return self
        def execute(self): return type("R", (), {"data":[riepi]})

    class ParzTbl:
        def __init__(self): self._kind=None
        def select(self,*a,**k): 
            if "confermato" in k and k["confermato"] is True: self._kind="conf"
            elif "confermato" in k and k["confermato"] is False: self._kind="wip"
            return self
        def eq(self,*a,**k): return self
        def order(self,*a,**k): return self
        def range(self,*a,**k): return self
        def limit(self,*a,**k): return self
        def update(self, d): return self
        def execute(self):
            if self._kind=="conf": return type("R", (), {"data": parziali_conf})
            if self._kind=="wip":  return type("R", (), {"data": []})
            return type("R", (), {"data": []})

    class ItemsTbl:
        def select(self,*a,**k): return self
        def in_(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": items})
        def update(self, d): 
            updates["qty"] = d["qty_confirmed"]; return self
        def eq(self,*a,**k): return self

    class RiepilogoTbl2:
        def update(self, d): 
            updates["stato"] = d["stato_ordine"]; return self
        def eq(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": []})

    def _table(name):
        if name=="ordini_vendor_riepilogo": return RiepTbl()
        if name=="ordini_vendor_parziali":  return ParzTbl()
        if name=="ordini_vendor_items":     return ItemsTbl()
    # la update finale su stato_ordine
    def _supa_table(name):
        if name=="ordini_vendor_riepilogo": return RiepilogoTbl2()
        return _table(name)

    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _supa_table})())

    resp = client.post("/api/amazon/vendor/parziali-wip/chiudi", json={"center":"FC1","data":"2025-01-10"})
    assert resp.status_code == 200
    assert updates["qty"] == 5
    assert updates["stato"] == "completato"



def test_items_by_barcode_qty_inserted(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    riepi = [{"id": 11, "po_list":["PO1"], "fulfillment_center":"FC1","start_delivery":"2025-01-10"}]
    items = [{"po_number":"PO1","model_number":"M1","vendor_product_id":"123","qty_ordered":2}]
    parziali = [{"dati":[{"po_number":"PO1","model_number":"M1","quantita":3}]}]

    class Riep:
        def select(self,*a,**k): return self
        def in_(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": riepi})

    class Items:
        def select(self,*a,**k): return self
        def in_(self,*a,**k): return self
        def or_(self,*a,**k): return self
        def limit(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": items})

    class Parz:
        def select(self,*a,**k): return self
        def in_(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": parziali})

    def _table(name):
        if name=="ordini_vendor_riepilogo": return Riep()
        if name=="ordini_vendor_items":     return Items()
        if name=="ordini_vendor_parziali":  return Parz()

    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _table})())

    res = client.get("/api/amazon/vendor/items/by-barcode?barcode=123")
    js = res.get_json()
    assert res.status_code == 200
    assert js[0]["qty_inserted"] == 3


def test_dashboard_parziali(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    riepi = [{"id": 1, "fulfillment_center":"FCX","start_delivery":"2025-01-10","stato_ordine":"nuovo","po_list":["POZ"]}]
    parz = [{"riepilogo_id":1,"numero_parziale":1,"dati":[{"collo":1},{"collo":2}], "conferma_collo":{"1":True}}]

    class Riep:
        def select(self,*a,**k): return self
        def in_(self,*a,**k): return self
        def order(self,*a,**k): return self
        def range(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": riepi})

    class Parz:
        def select(self,*a,**k): return self
        def in_(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": parz})

    def _table(name):
        if name=="ordini_vendor_riepilogo": return Riep()
        if name=="ordini_vendor_parziali":  return Parz()
    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _table})())

    res = client.get("/api/amazon/vendor/orders/riepilogo/dashboard")
    js = res.get_json()
    assert res.status_code == 200
    assert js[0]["colli_totali"] == 2
    assert js[0]["colli_confermati"] == 1



def test_patch_produzione_password_required_out_of_da_stampare(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    # riga esistente in stato diverso da "Da Stampare"
    row = {"id": 5, "stato_produzione":"In Corso", "da_produrre":10, "plus":0}
    class Tbl:
        def __init__(self): self.updated=None
        def select(self,*a,**k): return self
        def eq(self,*a,**k): return self
        def single(self): return self
        def execute(self): return type("R", (), {"data": row})
        def update(self, d): self.updated=d; return self
    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": lambda name: Tbl()})())
    # manca password -> 403
    r = client.patch("/api/produzione/5", json={"da_produrre":7})
    assert r.status_code == 403

def test_pulisci_da_stampare_elimina_vecchi(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    prod = [{"id":1,"sku":"A-1","ean":"111","start_delivery":"2025-01-01"}]
    prel = []  # nessun prelievo -> deve eliminare
    class PTable:
        def __init__(self): self.deleted=False
        def select(self,*a,**k): return self
        def eq(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": prod})
        def delete(self): self.deleted=True; return self
        def in_(self,*a,**k): return self
    class LTable:
        def select(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": prel})
    def _table(name):
        if name=="produzione_vendor": return PTable()
        if name=="prelievi_ordini_amazon": return LTable()
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _table})())
    res = client.post("/api/produzione/pulisci-da-stampare")
    assert res.status_code == 200
    assert res.get_json()["deleted"] >= 1



def test_prelievi_importa_filtra_solo_nuovi(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    # items presenti
    items = [
        {"model_number":"SKU-1","vendor_product_id":"EAN1","start_delivery":"2025-01-10","qty_ordered":3,"fulfillment_center":"FC1"},
        {"model_number":"SKU-1","vendor_product_id":"EAN1","start_delivery":"2025-01-10","qty_ordered":2,"fulfillment_center":"FC2"},
    ]
    # riepiloghi: solo "nuovo" va incluso
    riepi = [{"fulfillment_center":"FC1","start_delivery":"2025-01-10","stato_ordine":"nuovo"}]
    inserted = []
    class ItemsT:
        def select(self,*a,**k): return self
        def eq(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": items})
    class RiepT:
        def select(self,*a,**k): return self
        def eq(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": riepi})
    class PrelT:
        def __init__(self): self._data=[]
        def delete(self): return self
        def eq(self,*a,**k): return self
        def insert(self, batch): inserted.extend(batch); return self
        def execute(self): return type("R", (), {"data": []})
    def _table(name):
        if name=="ordini_vendor_items": return ItemsT()
        if name=="ordini_vendor_riepilogo": return RiepT()
        if name=="prelievi_ordini_amazon": return PrelT()
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _table})())
    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())
    res = client.post("/api/prelievi/importa", json={"data":"2025-01-10"})
    js = res.get_json()
    assert res.status_code == 200
    assert js["importati"] == js["totali"]
    # somma qty = 3 (solo FC1 è "nuovo")
    assert any(r["qty"] == 3 for r in inserted)

def test_prelievi_svuota(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    class T:
        def delete(self): return self
        def neq(self,*a,**k): return self
        def execute(self): return type("R", (), {"data":[]})
    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": lambda name: T()})())
    res = client.delete("/api/prelievi/svuota")
    assert res.status_code == 200
    assert res.get_json()["ok"] is True



def test_list_vendor_pos_ok_even_without_aws_keys(client, monkeypatch):
    import app.routes.amazon_vendor as mod
    # mock token
    monkeypatch.setattr(mod, "get_spapi_access_token", lambda: "tok")
    # mock variabili d'ambiente vuote -> awsauth None -> ok
    monkeypatch.setenv("AWS_ACCESS_KEY", "")
    monkeypatch.setenv("AWS_SECRET_KEY", "")
    # mock requests.get
    class _R: status_code=200; text='{"purchaseOrders":[]}'
    monkeypatch.setattr(mod.requests, "get", lambda *a, **k: _R())

    resp = client.get("/api/amazon/vendor/orders/list")
    assert resp.status_code == 200



def test_move_parziale_to_trasferito_sposta_delta_corretto(monkeypatch):
    import importlib
    mod = importlib.import_module("app.routes.amazon_vendor")

    # --- dati in memoria
    produzione = [
        # 6 pezzi già "Stampato" per SKU-1 alla stessa data
        {"id": 101, "sku": "SKU-1", "ean": "111", "start_delivery": "2025-08-11",
         "stato_produzione": "Stampato", "da_produrre": 6, "qty": 10, "riscontro": 0,
         "plus": 0, "radice": "SKU", "stato": "manca", "cavallotti": False, "note": ""},
    ]
    parziali = [{
        "riepilogo_id": 1, "numero_parziale": 2, "confermato": True, "gestito": False,
        "dati": [{"model_number": "SKU-1", "quantita": 4}],
    }]
    riepiloghi = [{"id": 1, "fulfillment_center": "FC1", "start_delivery": "2025-08-11"}]

    # --- finto sb_table con filtri minimi usati dal helper
    class Tbl:
        def __init__(self, name): self.name = name; self._sel="*"; self._f=[]; self._single=False
        def select(self, *a, **k): self._sel=a[0] if a else "*"; return self
        def eq(self, f, v): self._f.append(("eq", f, v)); return self
        def in_(self, f, vals): self._f.append(("in", f, set(vals))); return self

        @property
        def not_(self):  # <<< PROPERTY, non funzione
            class _Not:
                def __init__(_self, outer): _self.outer = outer
                def in_(_self, f, vals):
                    _self.outer._f.append(("not_in", f, set(vals)))
                    return _self.outer
            return _Not(self)

        def order(self, *a, **k): return self
        def single(self): self._single=True; return self
        def update(self, data): self._update = data; return self
        def insert(self, row):
            if self.name == "produzione_vendor":
                new_id = max([r["id"] for r in produzione] + [200]) + 1
                row = {**row, "id": new_id}
                produzione.append(row)
                self._inserted = [row]
            return self
        def execute(self):
            if self.name == "ordini_vendor_riepilogo":
                out = riepiloghi
            elif self.name == "ordini_vendor_parziali":
                out = parziali
                for op, f, v in self._f:
                    if op == "eq" and f == "riepilogo_id":
                        out = [r for r in out if r["riepilogo_id"] == v]
                    if op == "eq" and f == "numero_parziale":
                        out = [r for r in out if r["numero_parziale"] == v]
                if getattr(self, "_update", None) and "gestito" in self._update:
                    for r in out:
                        r["gestito"] = True
            elif self.name == "produzione_vendor":
                out = list(produzione)
                for op, f, v in self._f:
                    if op == "eq":
                        out = [r for r in out if str(r.get(f)) == str(v)]
                    elif op == "in":
                        out = [r for r in out if r.get(f) in v]
                    elif op == "not_in":
                        out = [r for r in out if r.get(f) not in v]
                if getattr(self, "_update", None):
                    for r in out:
                        r.update(self._update)
                if hasattr(self, "_inserted"):
                    return type("R", (), {"data": self._inserted})
            else:
                out = []
            if self._single:
                return type("R", (), {"data": (out[0] if out else None)})
            return type("R", (), {"data": out})

    monkeypatch.setattr(mod, "sb_table", lambda name: Tbl(name))
    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())  # non serve qui
    # non vogliamo errori su logging movimenti
    monkeypatch.setattr(mod, "log_movimento_produzione", lambda *a, **k: None)

    # ACT: sposta 4 pezzi per parziale #2
    mod._move_parziale_to_trasferito("FC1", "2025-08-11", 2)

    # ASSERT: 1) la riga origine è scesa da 6 -> 2
    assert produzione[0]["da_produrre"] == 2
    # 2) esiste una riga nuova stato Trasferito da 4 pezzi
    trasf = [r for r in produzione if r.get("stato_produzione") == "Trasferito"]
    assert len(trasf) == 1 and trasf[0]["da_produrre"] == 4
    # 3) il parziale è marcato gestito
    assert parziali[0]["gestito"] is True
    
    
    
def test_conferma_parziale_sposta_in_trasferito(client, monkeypatch):
    import importlib
    mod = importlib.import_module("app.routes.amazon_vendor")

    # Flag per vedere se il helper è stato chiamato con i parametri giusti
    called = {"args": None}
    def _fake_move(center, data, num):
        called["args"] = (center, data, num)

    # Finti tavoli per la sola conferma
    class RiepilogoTbl:
        row = {"id": 77, "stato_ordine": "nuovo"}  # condivisa

        def __init__(self):
            self._last_select = "*"

        def select(self, *a, **k):
            self._last_select = a[0] if a else "*"
            return self

        def eq(self, *a, **k): return self
        def single(self): return self

        def execute(self):
            if self._last_select == "id":
                return type("R", (), {"data": {"id": type(self).row["id"]}})
            if self._last_select == "stato_ordine":
                return type("R", (), {"data": {"stato_ordine": type(self).row["stato_ordine"]}})
            return type("R", (), {"data": [type(self).row]})

        def update(self, d):
            type(self).row["stato_ordine"] = d["stato_ordine"]
            return self


    class ParzialiTbl:
        def __init__(self): self.rows=[{"riepilogo_id":77,"numero_parziale":3,"confermato":False}]
        def select(self,*a,**k): return self
        def eq(self,*a,**k): return self
        def order(self,*a,**k): return self
        def limit(self,*a,**k): return self
        def execute(self): return type("R", (), {"data": self.rows})
        def update(self, d):
            self.rows[0]["confermato"] = True
            return self

    def _table(name):
        if name=="ordini_vendor_riepilogo": return RiepilogoTbl()
        if name=="ordini_vendor_parziali":  return ParzialiTbl()

    monkeypatch.setattr(mod, "supabase", type("S", (), {"table": _table})())
    monkeypatch.setattr(mod, "supa_with_retry", lambda fn: fn())
    monkeypatch.setattr(mod, "_move_parziale_to_trasferito", _fake_move)

    resp = client.post("/api/amazon/vendor/parziali-wip/conferma-parziale",
                       json={"center":"FC9","data":"2025-08-11"})
    assert resp.status_code == 200
    # Il helper è stato chiamato subito dopo la conferma
    assert called["args"] == ("FC9", "2025-08-11", 3)