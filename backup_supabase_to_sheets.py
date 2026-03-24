"""
Melcity CRM · Backup Supabase → Google Sheets
----------------------------------------------
Corre automáticamente desde GitHub Actions.
Lee contactos y actividades de Supabase
y los vuelca en una Google Sheet con fecha.

Requiere estas variables de entorno (GitHub Secrets):
  SUPABASE_URL         → https://xxxx.supabase.co
  SUPABASE_SERVICE_KEY → tu service_role key (no la anon key)
  GOOGLE_CREDENTIALS   → contenido del JSON de service account de Google
  SPREADSHEET_ID       → ID de la Google Sheet destino
"""

import os
import json
import datetime
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── CONFIG ────────────────────────────────────────────────────────────────────
SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SPREADSHEET_ID      = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDENTIALS  = os.environ["GOOGLE_CREDENTIALS"]

TODAY = datetime.date.today().strftime("%Y-%m-%d")

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

# ── FETCH DATA FROM SUPABASE ──────────────────────────────────────────────────
def fetch_table(table: str) -> list:
    url = f"{SUPABASE_URL}/rest/v1/{table}?select=*&order=created_at.asc"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

# ── GOOGLE SHEETS CLIENT ──────────────────────────────────────────────────────
def get_sheets_service():
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

# ── WRITE TO SHEET ────────────────────────────────────────────────────────────
def write_sheet(service, sheet_name: str, rows: list):
    """
    Crea o reemplaza una hoja con el nombre dado y escribe los datos.
    """
    spreadsheet = service.get(spreadsheetId=SPREADSHEET_ID).execute()
    existing = [s["properties"]["title"] for s in spreadsheet["sheets"]]

    if sheet_name in existing:
        # Limpiar contenido existente
        service.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{sheet_name}'!A1:ZZ",
        ).execute()
    else:
        # Crear hoja nueva
        service.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()

    if not rows:
        print(f"  {sheet_name}: sin datos")
        return

    service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()
    print(f"  {sheet_name}: {len(rows)-1} registros escritos")


# ── CONTACTOS ─────────────────────────────────────────────────────────────────
def backup_contactos(service):
    data = fetch_table("contactos")
    if not data:
        write_sheet(service, f"contactos_{TODAY}", [])
        return

    headers = [
        "id", "created_at", "nombre", "empresa", "email",
        "telefono", "fuente", "estado", "etapa", "valor",
        "vendedor", "tags", "ultima_actividad",
    ]
    rows = [headers]
    for c in data:
        rows.append([
            str(c.get("id", "")),
            str(c.get("created_at", "")),
            c.get("nombre", ""),
            c.get("empresa", ""),
            c.get("email", ""),
            c.get("telefono", ""),
            c.get("fuente", ""),
            c.get("estado", ""),
            c.get("etapa", ""),
            str(c.get("valor", 0)),
            c.get("vendedor", ""),
            ", ".join(c.get("tags") or []),
            c.get("ultima_actividad", ""),
        ])

    write_sheet(service, f"contactos_{TODAY}", rows)


# ── ACTIVIDADES ───────────────────────────────────────────────────────────────
def backup_actividades(service):
    data = fetch_table("actividades")
    if not data:
        write_sheet(service, f"actividades_{TODAY}", [])
        return

    headers = [
        "id", "created_at", "tipo", "titulo",
        "contacto_nombre", "fecha_label", "nota", "estado",
    ]
    rows = [headers]
    for a in data:
        rows.append([
            str(a.get("id", "")),
            str(a.get("created_at", "")),
            a.get("tipo", ""),
            a.get("titulo", ""),
            a.get("contacto_nombre", ""),
            a.get("fecha_label", ""),
            a.get("nota", ""),
            a.get("estado", ""),
        ])

    write_sheet(service, f"actividades_{TODAY}", rows)


# ── RESUMEN ───────────────────────────────────────────────────────────────────
def backup_resumen(service):
    """Hoja 'resumen' con una fila por backup para llevar historial."""
    contactos  = fetch_table("contactos")
    actividades = fetch_table("actividades")

    resumen_sheet = "resumen_backups"
    spreadsheet = service.get(spreadsheetId=SPREADSHEET_ID).execute()
    existing = [s["properties"]["title"] for s in spreadsheet["sheets"]]

    if resumen_sheet not in existing:
        service.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": resumen_sheet}}}]},
        ).execute()
        # Escribir encabezado
        service.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{resumen_sheet}'!A1",
            valueInputOption="RAW",
            body={"values": [["fecha", "total_contactos", "total_actividades", "status"]]},
        ).execute()

    # Append fila de hoy
    service.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{resumen_sheet}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[TODAY, len(contactos), len(actividades), "ok"]]},
    ).execute()
    print(f"  resumen: {len(contactos)} contactos, {len(actividades)} actividades")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"Backup Melcity CRM · {TODAY}")
    service = get_sheets_service()
    print("Contactos...")
    backup_contactos(service)
    print("Actividades...")
    backup_actividades(service)
    print("Resumen...")
    backup_resumen(service)
    print("Backup completado.")

if __name__ == "__main__":
    main()
