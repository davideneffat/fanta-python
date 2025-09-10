# ERRORE: TROVA CIRCA 240 PARTITE A STAGIONE QUANDO IN REALTA' SONO 380
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup

# --- Configurazione ---
stagioni_map = {
    "2023-2024": "serie-a-2023-2024", "2022-2023": "serie-a-2022-2023",
    "2021-2022": "serie-a-2021-2022", "2020-2021": "serie-a-2020-2021",
    "2019-2020": "serie-a-2019-2020", "2018-2019": "serie-a-2018-2019",
    "2017-2018": "serie-a-2017-2018", "2016-2017": "serie-a-2016-2017",
    "2015-2016": "serie-a-2015-2016",
}
base_url_template = "https://www.centroquote.it/football/italy/{stagione_path}/results/"
all_matches_data = []

# --- Setup di Selenium ---
print("Avvio del browser Chrome con Selenium...")
options = webdriver.ChromeOptions()
options.add_experimental_option('excludeSwitches', ['enable-logging'])
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

try:
    for stagione_str, stagione_path in stagioni_map.items():
        print(f"--- Inizio scraping stagione: {stagione_str} ---")

        
        url = base_url_template.format(stagione_path=stagione_path)
        driver.get(url)

        if stagione_str == list(stagioni_map.keys())[0]:
            try:
                cookie_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'ACCETTA')]"))
                )
                cookie_button.click()
                time.sleep(2)
            except Exception:
                print("    -> Nessun banner cookie trovato o già accettato.")

        page = 1
        while True:
            print(f"  -> Analisi Pagina: {page}")
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='game-row']"))
                )
            except TimeoutException:
                 print("  -> Timeout in attesa dei dati o pagina vuota. Fine stagione.")
                 break

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            result_elements = soup.select("div[data-testid='game-row']")
            
            if not result_elements:
                print("  -> Nessun elemento trovato sulla pagina. Fine stagione.")
                break

            for element in result_elements:
                # --- Estrazione Squadre (invariata) ---
                team_elements = element.select("p.participant-name")
                home_team = team_elements[0].get_text(strip=True) if len(team_elements) > 0 else None
                away_team = team_elements[1].get_text(strip=True) if len(team_elements) > 1 else None

                # --- Estrazione Quote (invariata) ---
                odds_elements = element.select("div[data-testid*='odd-container']")
                quota_1 = odds_elements[0].get_text(strip=True) if len(odds_elements) > 0 else None
                quota_x = odds_elements[1].get_text(strip=True) if len(odds_elements) > 1 else None
                quota_2 = odds_elements[2].get_text(strip=True) if len(odds_elements) > 2 else None
                
                if home_team and away_team and (quota_1 or quota_x or quota_2):
                    all_matches_data.append({
                        'Stagione': stagione_str,
                        'HomeTeam': home_team,
                        'AwayTeam': away_team,
                        'Quota_1': quota_1,
                        'Quota_X': quota_x,
                        'Quota_2': quota_2,
                    })
                    print(f"    -> [St. {stagione_str} ] {home_team} vs {away_team} | Quote: 1={quota_1}, X={quota_x}, 2={quota_2}")
                    

            # --- Logica di paginazione (invariata) ---
            try:
                first_row_before_click = driver.find_element(By.CSS_SELECTOR, "div[data-testid='game-row']")
                next_page_button = driver.find_element(By.XPATH, "//a[text()='Avanti']")
                driver.execute_script("arguments[0].click();", next_page_button)
                WebDriverWait(driver, 10).until(EC.staleness_of(first_row_before_click))
                page += 1
            except NoSuchElementException:
                print("  -> Non è stato trovato il bottone 'Avanti'. Fine della stagione.")
                break
            except TimeoutException:
                print("  -> La pagina non si è aggiornata dopo il click. Fine della stagione.")
                break

finally:
    print("Chiusura del browser...")
    driver.quit()

if all_matches_data:
    df = pd.DataFrame(all_matches_data)
    for col in ['Quota_1', 'Quota_X', 'Quota_2']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print("\n--- Esempio del DataFrame Finale ---")
    print(df.head())
    print("\n...")
    print(df.tail())

    output_filename = "quote_multiseason.csv"
    df.to_csv(output_filename, index=False)
    print(f"\nDataset pulito salvato con successo in '{output_filename}'")
else:
    print("\nNessun dato raccolto. Controllare l'output per eventuali errori.")