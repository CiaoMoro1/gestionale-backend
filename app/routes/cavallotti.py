from flask import Blueprint, request, jsonify
import io, requests, base64
import barcode
from barcode.writer import ImageWriter
from app.supabase_client import supabase

bp = Blueprint("cavallotti", __name__)

# Logo base64 PNG (copia la stringa completa come ti ho suggerito prima)
LOGO_BASE64 = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAvsAAADPCAYAAABr9R7cAAAACXBIWXMAAC4jAAAuIwF4pT92AAAGYmlUWHRYTUw6Y29tLmFkb2JlLnhtcAAAAAAAPD94cGFja2V0IGJlZ2luPSLvu78iIGlkPSJXNU0wTXBDZWhpSHpyZVN6TlRjemtjOWQiPz4gPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyIgeDp4bXB0az0iQWRvYmUgWE1QIENvcmUgNS42LWMxNDUgNzkuMTYzNDk5LCAyMDE4LzA4LzEzLTE2OjQwOjIyICAgICAgICAiPiA8cmRmOlJERiB4bWxuczpyZGY9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkvMDIvMjItcmRmLXN5bnRheC1ucyMiPiA8cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0iIiB4bWxuczp4bXA9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC8iIHhtbG5zOmRjPSJodHRwOi8vcHVybC5vcmcvZGMvZWxlbWVudHMvMS4xLyIgeG1sbnM6cGhvdG9zaG9wPSJodHRwOi8vbnMuYWRvYmUuY29tL3Bob3Rvc2hvcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RFdnQ9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZUV2ZW50IyIgeG1wOkNyZWF0b3JUb29sPSJBZG9iZSBQaG90b3Nob3AgQ0MgMjAxOSAoV2luZG93cykiIHhtcDpDcmVhdGVEYXRlPSIyMDI1LTA2LTI3VDIzOjU2OjA4KzAyOjAwIiB4bXA6TW9kaWZ5RGF0ZT0iMjAyNS0wNy0xN1QxNjoxMjo1NiswMjowMCIgeG1wOk1ldGFkYXRhRGF0ZT0iMjAyNS0wNy0xN1QxNjoxMjo1NiswMjowMCIgZGM6Zm9ybWF0PSJpbWFnZS9wbmciIHBob3Rvc2hvcDpDb2xvck1vZGU9IjMiIHBob3Rvc2hvcDpJQ0NQcm9maWxlPSJzUkdCIElFQzYxOTY2LTIuMSIgeG1wTU06SW5zdGFuY2VJRD0ieG1wLmlpZDpkODMwNmQwNi1kNGJmLTZkNGYtYjNlNy04ZjA2NjNiNGJiMDUiIHhtcE1NOkRvY3VtZW50SUQ9ImFkb2JlOmRvY2lkOnBob3Rvc2hvcDpkMDM0NjlmYy1mNGU1LTFlNDMtYWYzZC1jYjQ2NjA4Y2Q1MDciIHhtcE1NOk9yaWdpbmFsRG9jdW1lbnRJRD0ieG1wLmRpZDoyZDU4ODA1ZS05OTQ4LWFlNDAtYTFiZi1jMjliN2NlZjc5ODQiPiA8eG1wTU06SGlzdG9yeT4gPHJkZjpTZXE+IDxyZGY6bGkgc3RFdnQ6YWN0aW9uPSJjcmVhdGVkIiBzdEV2dDppbnN0YW5jZUlEPSJ4bXAuaWlkOjJkNTg4MDVlLTk5NDgtYWU0MC1hMWJmLWMyOWI3Y2VmNzk4NCIgc3RFdnQ6d2hlbj0iMjAyNS0wNi0yN1QyMzo1NjowOCswMjowMCIgc3RFdnQ6c29mdHdhcmVBZ2VudD0iQWRvYmUgUGhvdG9zaG9wIENDIDIwMTkgKFdpbmRvd3MpIi8+IDxyZGY6bGkgc3RFdnQ6YWN0aW9uPSJjb252ZXJ0ZWQiIHN0RXZ0OnBhcmFtZXRlcnM9ImZyb20gYXBwbGljYXRpb24vdm5kLmFkb2JlLnBob3Rvc2hvcCB0byBpbWFnZS9wbmciLz4gPHJkZjpsaSBzdEV2dDphY3Rpb249InNhdmVkIiBzdEV2dDppbnN0YW5jZUlEPSJ4bXAuaWlkOmQ4MzA2ZDA2LWQ0YmYtNmQ0Zi1iM2U3LThmMDY2M2I0YmIwNSIgc3RFdnQ6d2hlbj0iMjAyNS0wNy0xN1QxNjoxMjo1NiswMjowMCIgc3RFdnQ6c29mdHdhcmVBZ2VudD0iQWRvYmUgUGhvdG9zaG9wIENDIDIwMTkgKFdpbmRvd3MpIiBzdEV2dDpjaGFuZ2VkPSIvIi8+IDwvcmRmOlNlcT4gPC94bXBNTTpIaXN0b3J5PiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/Pv85x3wAACfZSURBVHja7d1tbCXXfd/xw8d94D7cjVYPK0XeK0uK7cIqr5yksFHbewXkASgSLBcNUDcIutwmL2wEiLh9gK3UrriuEVtoGlIuAvtFUq6CApGABqQQpEBSA8t1FMSJG/Cq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"  # metti la tua stringa lunga qui

# Background geometrico SVG (la tua stringa di prima)
SVG_BG = "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='60' height='60' viewBox='0 0 60 60'%3E%3Cg fill-rule='evenodd'%3E%3Cg fill='%23eb9c00' fill-opacity='0.1' fill-rule='nonzero'%3E%3Cpath d='M29 58.58l7.38-7.39A30.95 30.95 0 0 1 29 37.84a30.95 30.95 0 0 1-7.38 13.36l7.37 7.38zm1.4 1.41l.01.01h-2.84l-7.37-7.38A30.95 30.95 0 0 1 6.84 60H0v-1.02a28.9 28.9 0 0 0 18.79-7.78L0 32.41v-4.84L18.78 8.79A28.9 28.9 0 0 0 0 1.02V0h6.84a30.95 30.95 0 0 1 13.35 7.38L27.57 0h2.84l7.39 7.38A30.95 30.95 0 0 1 51.16 0H60v27.58-.01V60h-8.84a30.95 30.95 0 0 1-13.37-7.4L30.4 60zM29 1.41l-7.4 7.38A30.95 30.95 0 0 1 29 22.16 30.95 30.95 0 0 1 36.38 8.8L29 1.4zM58 1A28.9 28.9 0 0 0 39.2 8.8L58 27.58V1.02zm-20.2 9.2A28.9 28.9 0 0 0 30.02 29h26.56L37.8 10.21zM30.02 31a28.9 28.9 0 0 0 7.77 18.79l18.79-18.79H30.02zm9.18 20.2A28.9 28.9 0 0 0 58 59V32.4L39.2 51.19zm-19-1.4a28.9 28.9 0 0 0 7.78-18.8H1.41l18.8 18.8zm7.78-20.8A28.9 28.9 0 0 0 20.2 10.2L1.41 29h26.57z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E\");"

@bp.route('/api/cavallotto/html')
def cavallotto_html():
    sku = request.args.get("sku")
    formato = request.args.get("formato", "A5").upper()
    num_copie = int(request.args.get("copie", 1))  # Parametro copie

    # 1. Cerca la riga produzione_vendor
    produzione = supabase.table("produzione_vendor").select("*").eq("sku", sku).limit(1).execute().data
    if not produzione:
        return "Articolo non trovato in produzione", 404
    produzione = produzione[0]
    ean = produzione.get("ean") or ""
    sku = produzione.get("sku") or ""
    # 2. Prendi anche dati prodotti (immagine, titoli)
    prodotto = supabase.table("products").select("*").eq("sku", sku).single().execute().data
    image_url = prodotto.get("image_url") if prodotto else ""
    product_title = prodotto.get("product_title") if prodotto else ""
    variant_title = prodotto.get("variant_title") if prodotto else ""
    # 3. Barcode EAN in base64 (PNG)
    barcode64 = ""
    if ean and len(ean) in [8, 12, 13]:
        barcode_img = io.BytesIO()
        try:
            bc = barcode.get("ean13", ean.zfill(13), writer=ImageWriter())
            bc.write(barcode_img)
            barcode_img.seek(0)
            barcode64 = "data:image/png;base64," + base64.b64encode(barcode_img.read()).decode()
        except Exception:
            barcode64 = ""

    # 4. Formati (dimensioni e scala font proporzionale)
    if formato == "A3":
        larghezza = 42
        altezza = 29.7
        font_scale = 1.5
    elif formato == "A4":
        larghezza = 29.7
        altezza = 21
        font_scale = 1.15
    else:  # A5 o default
        larghezza = 21
        altezza = 14.8
        font_scale = 0.85
    larghezza_str = f"{larghezza}cm"
    altezza_str = f"{altezza}cm"

    # 5. Blocchetto cavallotto singolo (stile + HTML)
    cavallotto_block = f"""
        <div class="cav-main">
            <div class="cav-img">
                <img src="{image_url}" alt="Immagine prodotto" />
            </div>
            <div class="cav-info-outer">
              <div class="cav-info">
                <img class="cav-logo" src="{LOGO_BASE64}" alt="Logo" />
                <div class="cav-titlebox">
                  <div class="cav-title">{product_title or sku}</div>
                </div>
                <div class="cav-variant">{variant_title}</div>
                <div class="cav-bar" style="font-size: {1.1 * font_scale}em; color: #1e1e1e; font-weight: 700;">{sku}</div>
                <div class="cav-barcode">{f'<img src="{barcode64}" />' if barcode64 else ""}</div>
                <div class="cav-wash">
                  <img src="/static/lavaggio.png" alt="Simboli lavaggio" />
                </div>
              </div>
            </div>
        </div>
    """

    # 6. Duplica per quante copie servono!
    cavallotti_html = cavallotto_block * num_copie

    # 7. HTML completo
    html = f"""
    <html>
    <head>
      <title>Cavallotti {sku} x{num_copie}</title>
      <style>
        html, body {{
          width:100vw; height:100vh;
          margin:0; padding:0;
          background:#f8fafc;
        }}
        @media print {{
          html, body {{
            margin:0 !important; padding:0 !important; background:white !important;
            overflow:hidden;
          }}
          .no-print {{ display: none !important; }}
        }}
        body {{
          margin:0; padding:0;
          font-family: 'Segoe UI', Arial, sans-serif;
        }}
        .cav-main {{
          width: {larghezza_str}; height: {altezza_str};
          min-width: {larghezza_str}; min-height: {altezza_str};
          max-width: {larghezza_str}; max-height: {altezza_str};
          box-sizing: border-box;
          border-radius: 28px;
          box-shadow: 0 2px 24px 0 #bbb4;
          background-color: #fff;
          background-image: {SVG_BG};
          background-size: 50px 50px;   /* fissa come A5 */
          background-repeat: repeat;
          display: flex;
          flex-direction: row;
          overflow: hidden;
          position: relative;
          margin: 0 auto 18px auto;
          page-break-after: always;   /* OGNI CAVALLOTTO SU UNA PAGINA */
        }}
        .cav-img {{
          flex: 1 1 60%;
          display: flex;
          align-items: center;
          justify-content: center;
          background: transparent;
          height: 100%;
          min-width: 0;
          padding-left: 3%;
        }}
        .cav-img img {{
          max-width: 96%; max-height: 96%;
          border-radius: 18px;
          border: 1.5px solid #bbb;
          object-fit: contain;
        }}
        .cav-info-outer {{
          flex: 1 1 40%;
          display: flex;
          align-items: stretch;
          justify-content: stretch;
          height: 80%;
          padding: 0;
          margin: 0;
          box-sizing: border-box;
          /* Qui puoi mettere eventuali padding o background extra */
        }}
        .cav-info {{
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: space-between;
          width: 100%;
          height: 100%;
          padding-top: 7%;
          padding-bottom: 7%;
          padding-left: 20px;
          padding-right: 20px;
          background: transparent;
          text-align: center;
          box-sizing: border-box;
        }}
        .cav-logo {{
          width: 80%;
          max-width: 400px;
          min-width: 200px;
          height: auto;
          margin-bottom: 1em;
          margin-top: 0;
        }}
        .cav-title {{
          font-size: {2.2 * font_scale}em;
          font-weight: 800;
          margin-bottom: {int(10 * font_scale)}px;
          color: #998468;
        }}
        .cav-variant {{
          font-size: {1.1 * font_scale}em;
          color: #c4a982;
          margin-bottom: {int(14 * font_scale)}px;
          font-weight: 500;
        }}
        .cav-bar {{
          font-size: {1 * font_scale}em;
          color: #222;
          margin-bottom: {int(6 * font_scale)}px;
        }}
        .cav-barcode {{
          margin-top: {int(18 * font_scale)}px;
          margin-bottom: {int(8 * font_scale)}px;
        }}
        .cav-barcode img {{
          max-width: {int(170 * font_scale)}px;
        }}
        .print-btn {{
          position: fixed; top: 12px; right: 18px; z-index: 999;
          padding: 8px 28px; border-radius: 14px;
          background: #00b3c7; color: white; border: none;
          font-weight: bold; font-size: 18px;
        }}
      </style>
    </head>
    <body>
      <button class="print-btn no-print" onclick="window.print()">🖨️ Stampa</button>
      {cavallotti_html}
    </body>
    </html>
    """
    return html
