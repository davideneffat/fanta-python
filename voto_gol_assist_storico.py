import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time

# Genera dinamicamente l'elenco delle stagioni da analizzare
stagioni = [f"{anno}-{str(anno+1)[-2:]}" for anno in range(2015, 2025)]
# -> ['2015-16', '2016-17', ..., '2024-25']

all_players_data_multiseason = []
base_url_template = "https://www.fantacalcio.it/voti-fantacalcio-serie-a/{stagione}/"

# Loop esterno per ogni stagione
for stagione in stagioni:
    base_url = base_url_template.format(stagione=stagione)
    print(f"--- Inizio scraping per la stagione: {stagione} ---")

    for giornata in range(1, 39):
        url = f"{base_url}{giornata}"
        print(f"Scraping dati per Stagione {stagione}, Giornata {giornata}")
        time.sleep(0.5)

        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"  -> Errore o giornata non disponibile: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        
        player_rows = soup.select("div.team-table-body tr")

        if not player_rows:
            print(f"  -> Nessun dato trovato per questa giornata.")
            continue

        for row in player_rows:
            name_cell = row.find("div", class_="player-item")
            if not name_cell:
                continue
            
            name = name_cell.get_text(strip=True)
            
            grade = row.find("span", class_="player-grade")
            fanta_grade = row.find("span", class_="player-fanta-grade")
            voto = grade["data-value"] if grade and grade.has_attr("data-value") else None
            fantavoto = fanta_grade["data-value"] if fanta_grade and fanta_grade.has_attr("data-value") else None
            
            bonuses = row.find_all("span", class_="player-bonus")
            bonus_dict = {b["title"]: b["data-value"] for b in bonuses if b.has_attr("title")}
            
            player_info = {
                "Stagione": stagione, 
                "Giocatore": name,
                "Giornata": giornata,
                "Voto": voto,
                "Fantavoto": fantavoto,
            }
            player_info.update(bonus_dict)
            all_players_data_multiseason.append(player_info)

# Crea il DataFrame completo da tutte le stagioni
df_multiseason_raw = pd.DataFrame(all_players_data_multiseason)

raw_filename = "fantavoto_multiseason_raw.csv"
try:
    df_multiseason_raw.to_csv(raw_filename, index=False)
    print(f"\nDATI GREZZI DI TUTTE LE STAGIONI SALVATI IN '{raw_filename}'")
except Exception as e:
    print(f"Errore durante il salvataggio del file grezzo: {e}")


# ====================================================================== #
# PARTE 2: ELABORAZIONE DEI DATI     
# ====================================================================== #

try:
    df_full = pd.read_csv("fantavoto_multiseason_raw.csv")
    print("\n--- Inizio elaborazione del file grezzo multi-stagione ---")
except FileNotFoundError:
    print("ERRORE: File 'fantavoto_multiseason_raw.csv' non trovato.")
    print("Assicurati di eseguire prima la Parte 1 (scraping) per creare questo file.")
    exit()

# --- 1. Pulizia e Conversione Numerica ---
text_cols = ["Giocatore", "Stagione", "Squadra"] # Aggiunta 'Stagione' e 'Squadra' per sicurezza
potential_numeric_cols = [col for col in df_full.columns if col not in text_cols]

for col in potential_numeric_cols:
    if col in df_full.columns:
        series = df_full[col].astype(str)
        series = series.replace('55', np.nan)
        series = series.str.replace(',', '.', regex=False)
        df_full[col] = pd.to_numeric(series, errors="coerce")

# --- 2. Consolidamento Gol e Rimozione Colonne ---
if 'Gol segnati' in df_full.columns and 'Rigori segnati' in df_full.columns:
    df_full['Gol segnati'] = df_full['Gol segnati'].fillna(0) + df_full['Rigori segnati'].fillna(0)
    df_full['Gol segnati'] = df_full['Gol segnati'].astype(int)

cols_to_drop = [
    'Squadra', 'Gol subiti', 'Autoreti', 'Rigori segnati',
    'Rigori parati', 'Player of the match', 'Rigori sbagliati'
]
df_full.drop(columns=cols_to_drop, inplace=True, errors='ignore')

# --- 3. Calcolo delle Statistiche Mobili ---
# Ordina per Giocatore, Stagione e Giornata per mantenere la cronologia corretta
df_full = df_full.sort_values(by=["Giocatore", "Stagione", "Giornata"]).reset_index(drop=True)

df_valido = df_full.dropna(subset=['Fantavoto']).copy()

print("Calcolo delle statistiche mobili...")
df_valido['Fantavoto_Media_Ultime_5'] = df_valido.groupby('Giocatore')['Fantavoto'].rolling(5, 1).mean().reset_index(0, drop=True).shift(1)
if 'Gol segnati' in df_valido.columns:
    df_valido['Gol_Somma_Ultime_5'] = df_valido.groupby('Giocatore')['Gol segnati'].rolling(5, 1).sum().reset_index(0, drop=True).shift(1)
if 'Assist' in df_valido.columns:
    df_valido['Assist_Somma_Ultime_5'] = df_valido.groupby('Giocatore')['Assist'].rolling(5, 1).sum().reset_index(0, drop=True).shift(1)

# --- 4. Unione e Propagazione dei Dati (Forward Fill) ---
cols_to_merge = ['Giocatore', 'Stagione', 'Giornata', 'Fantavoto_Media_Ultime_5', 'Gol_Somma_Ultime_5', 'Assist_Somma_Ultime_5']
existing_cols_to_merge = [col for col in cols_to_merge if col in df_valido.columns]
df_full = pd.merge(df_full, df_valido[existing_cols_to_merge], on=['Giocatore', 'Stagione', 'Giornata'], how='left')

cols_to_ffill = [col for col in existing_cols_to_merge if col not in ['Giocatore', 'Stagione', 'Giornata']]
print(f"Propagazione valori (ffill) per: {cols_to_ffill}")
for col in cols_to_ffill:
    # Raggruppa solo per Giocatore per far sì che la forma continui tra le stagioni
    df_full[col] = df_full.groupby('Giocatore')[col].ffill()

# --- 5. Output Finale ---
print("\nDataFrame finale processato (esempio su un giocatore):")
# Seleziona un giocatore che ha giocato in più stagioni per un buon esempio
esempio_giocatore = df_full['Giocatore'].value_counts().index[0]
print(df_full[df_full['Giocatore'] == esempio_giocatore].tail(15))

try:
    output_filename = "fantavoto_multiseason_processed.csv"
    df_full.to_csv(output_filename, index=False)
    print(f"\nDataFrame finale salvato come '{output_filename}'")
except PermissionError:
    print(f"\nERRORE: Impossibile salvare il file '{output_filename}'. Chiudi il file se è aperto in Excel e riprova.")