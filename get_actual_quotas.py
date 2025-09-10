import requests
import json
import pandas as pd # <-- 1. Importa la libreria pandas

# L'URL esatto dell'endpoint API
url = "https://apipreview.snai.it/sports/sports/scommesse?slug=sport%2Fcalcio%2Fserie%20a%2F1x2%20finale"

# Gli "headers" della richiesta
headers = {
    'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJpdC5zbmFpLmFwaWdhZCIsImp0aSI6IjAyMzYxZDhkYmZhZjViMGIwMDMwMDZkNTgwNzc2YjY3NDBjNzY1MTM2NmViMzllYzQwZjZhMjQzMGRmOWM3NmNjODZjOWYyMThiN2Y5MDUxIiwiaWF0IjoxNzU3NTM3ODAzLjczOTQ3OCwibmJmIjoxNzU3NTM3ODAzLjczOTQ3OSwiZXhwIjoxNzU3NTM5MDAzLjczNDQ3Mywic3ViIjoiIiwic2NvcGVzIjpbImNvbW1vbiJdLCJzbmFpLXRva2VuIjoiIiwic25haS10b2tlbl9jcnlwdCI6Ijc2NDk0YjUwNzg1MTRjNDQyYjU3NDEzNDUwNGU0YTcwNmU1NDc3Njg1MTY3M2QzZDNhNGQ1NDQ5Nzk0ZDdhNTUzMTRlNmE1MTMzNGU2ZDVhNmM1YTQ3NGU2OTU5NTEzZDNkIiwic25haS1jYXJ0YSI6IiJ9.MWUMrWaYof0KN_UoVMbf5jhqcKLGZyQBMX86hkEf7jLWnf6QSH0XbndGPldyNUcb0dSU1zz8z0eamxmtt6a8mexQWxvYBqUOJ8MeLcMaUoq4KPTMHUU5K2tHxrwr16zBS6JomN6cVYJ5h7m2PfM9WENiDo_ijR5i_EHREni7w_9tzWeDfAsHaNKesOnqZJY2fK1LeBITFfVVOL_XYxq-z2PGLRlsyb_BlMtMczwwuF8cKlmLi_C43PcrOw9ifcjCtwpuTLWRwqB7sX53GfJgq5sCJSHVg51Ziq6mcvQMtxDOllRV-1CxzLtrOzU0zD21F-CE8YF8TDn-2gxzSL0lZg',
    'Accept': '*/*',
    'Accept-Language': 'it-IT,it;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
}

try:
    print("Sto inviando la richiesta all'API di SNAI...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print("Richiesta riuscita! Status code:", response.status_code)
    
    data = response.json()
    
    lista_avvenimenti = data.get('avvenimentoList', [])
    mappa_quote = data.get('esitoMap', {})
    
    # <-- 2. Crea una lista vuota per contenere i dati delle partite
    dati_partite = []

    if not lista_avvenimenti:
        print("Nessun avvenimento trovato nella risposta dell'API.")
    else:
        for evento in lista_avvenimenti:
            id_evento = evento['key']
            squadra_casa = evento.get('firstCompetitor', {}).get('descrizione', 'N/D')
            squadra_ospite = evento.get('secondCompetitor', {}).get('descrizione', 'N/D')
            data_ora = evento.get('dataOra', 'N/D')

            base_key = f"{id_evento}-1$3-00000000"
            key_quota_1 = f"{base_key}-1"
            key_quota_X = f"{base_key}-2"
            key_quota_2 = f"{base_key}-3"
            
            quota_1_obj = mappa_quote.get(key_quota_1)
            quota_X_obj = mappa_quote.get(key_quota_X)
            quota_2_obj = mappa_quote.get(key_quota_2)
            
            quota_1 = quota_1_obj['quota'] if quota_1_obj else None # Usiamo None per i dati mancanti
            quota_X = quota_X_obj['quota'] if quota_X_obj else None
            quota_2 = quota_2_obj['quota'] if quota_2_obj else None

            # <-- 3. Invece di stampare, crea un dizionario e aggiungilo alla lista
            partita_dict = {
                'Squadra Casa': squadra_casa,
                'Squadra Ospite': squadra_ospite,
                'Data Ora': data_ora,
                'Quota 1': quota_1,
                'Quota X': quota_X,
                'Quota 2': quota_2
            }
            dati_partite.append(partita_dict)
    
    # <-- 4. Dopo il ciclo, se la lista non è vuota, crea il DataFrame
    if dati_partite:
        df = pd.DataFrame(dati_partite)
        
        # (Opzionale ma consigliato) Converte la colonna della data in un formato datetime
        df['Data Ora'] = pd.to_datetime(df['Data Ora'])
        
        # Ordina il DataFrame per data
        df = df.sort_values(by='Data Ora')
        
        print("\n--- DATAFRAME CON LE QUOTE DELLA SERIE A ---")
        print(df)

        # Ora puoi fare quello che vuoi con il DataFrame, ad esempio salvarlo in un file CSV
        # df.to_csv('quote_serie_a.csv', index=False)
        # print("\nDati salvati su quote_serie_a.csv")

except requests.exceptions.HTTPError as errh:
    print(f"Errore HTTP: {errh}")
    print(f"Contenuto della risposta: {response.text}")
except requests.exceptions.ConnectionError as errc:
    print(f"Errore di Connessione: {errc}")
except requests.exceptions.Timeout as errt:
    print(f"Errore di Timeout: {errt}")
except requests.exceptions.RequestException as err:
    print(f"Qualcosa è andato storto: {err}")