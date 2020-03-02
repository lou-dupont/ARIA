from bs4 import BeautifulSoup
import csv
import json
import math
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import os
import re
import requests
import time
import urllib.request

MAX_TIME_OUT = 60

url_root = "https://www.aria.developpement-durable.gouv.fr/"
url_search = url_root + "?s=&fwp_types_de_publication=accident&fwp_per_page=100&fwp_paged=%s"

# Répertoire d'accueil des pages
folder = "ARIA_pages/"
try : 
    os.mkdir(folder)
except : 
    print('Le dossier existe déjà')


def appelerAPI(numAria) : 
    url = "https://www.aria.developpement-durable.gouv.fr/wp-json/facetwp/v1/refresh"
    facets = {
        "recherche":"",
        "enseignements_sectoriels":[],
        "enseignements_thmatiques":[],
        "types_de_publication":["accident"],
        "date_de_publication":[],
        "numero_aria":[numAria],
        "date_de_survenue":[],
        "pays":[],
        "region":[],
        "departements":[],
        "commune":[],
        "code_naf":[],
        "documents_complementaires":[],
        "types_daccidents":[],
        "types_dvnement":[],
        "matieres":[],
        "classes_de_danger_clp":[],
        "equipements":[],
        "eish_public":[],
        "consequences":[],
        "echelle_matiere":[],
        "echelle_humaine":[],
        "echelle_environnement":[],
        "echelle_economie":[],
        "causes_premieres":[],
        "causes_profondes":[]
    }

    facets = json.dumps(facets)

    myobj = {
        "action": "facetwp_refresh",
        "data[facets]": facets,
        "data[http_params][get][s]": "",
        "data[http_params][get][fwp_numero_aria]": numAria,
        "data[http_params][uri]": "",
        "data[http_params][url_vars][numero_aria][]": numAria,
        "data[http_params][lang]": "fr",
        "data[template]": "recherche_accidents"
        #"data[extras][pager]": "true",
        #"data[extras][per_page]": "default",
        #"data[extras][counts]": "true",
        #"data[extras][sort]": "default",
        #"data[soft_refresh]": 0,
        #"data[is_bfcache]": 1,
        #"data[first_load]": 0,
        #"data[paged]": 1,
    }

    response = requests.post(url, data = myobj, timeout = MAX_TIME_OUT)
    return(response)


# ------------------------
# Scraping des liens d'accidents
# A vérifier : si la fiche a changé, incrément de numéro XXXX-2.

links_csv_file = 'url_accidents_ARIA.csv'

if os.path.exists(links_csv_file):
    print("Le fichier des URL existe déjà.")
else:
    # Nombre de pages, de résultats
    page_resultats = appelerAPI('').text
    nb_resultats = json.loads(page_resultats)['settings']['pager']['total_rows']
    nb_pages = math.ceil(nb_resultats/100) # Ne pas utiliser total_pages car assis sur 10 résultats par page
    print("On parcourt les %s pages"%(nb_pages))
    outF = open("url_accidents_ARIA.csv", "w", encoding='utf-8')
    for n in range(nb_pages) :
        print(n, end = ' ')
        page_response = requests.get(url_search%(str(n+1)), timeout = MAX_TIME_OUT) # Part de 1 à ... 
        page_content = BeautifulSoup(page_response.content, "html.parser")
        results = page_content.find_all("h2")
        for result in results : 
            link = result.find_all('a')[0]['href']
            outF.writelines(str(link) + '\n')
    outF.close()

# -----------------------------------
# Scraping des pages html

# Lecture des liens d'accidents
with open(links_csv_file, 'r') as f:
    reader = csv.reader(f)
    links = list(reader)
links = [item for sublist in links for item in sublist]
print("Nombre d'URL dans la liste des URL : %d." % len(links))

# Pages déjà présentes dans le dossier
pages = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
pages_dict = {}
for page in pages:
    pages_dict[page] = True

# Nouvelles pages à télécharger
missing_links = [link for link in links if re.sub('.*/([^/]*)/', 'fiche_\\1.html', link) not in pages_dict]        
print("Nombre de nouvelles fiches à télécharger : %d." % len(missing_links))

def telechargerPageAccident(link) :
    # Chargement des pages
    page_name = re.sub('.*/([^/]*)/', folder + 'fiche_\\1.html', link)
    meta_page_name = re.sub('.*/([^/]*)/', folder + 'meta_\\1.txt', link)

    try : 
        page = requests.get(link, timeout = MAX_TIME_OUT)
        #missing_links.remove(link)
    except : 
        print("Erreur sur le lien %s"%(link))
        return()

    # Si la page est trouvée, téléchargement du contenu (allégé)
    content = BeautifulSoup(page.content, "lxml")
    title = content.find('span', {"class" : "page-header-title wpex-clr"})
    url = '<link href = "%s">'%(link)
    article = content.find('article')
    numAria = int(re.sub('N° ', '', article.find("strong").text)) # Utile pour la recherche meta
    
    # Sauvegarde des pages dans un fichier html
    outF = open(page_name, "w", encoding='utf-8')
    outF.writelines(str(url))
    outF.writelines(str(title))
    outF.writelines(str(article))
    outF.close()
    
    # Sauvegarde des metadonnées en appellant l'API
    try: 
        data = json.loads(appelerAPI(numAria).text)['facets']
        outM = open(meta_page_name, "w", encoding='utf-8')
        for child in data : 
            options = BeautifulSoup(data[child], "html.parser").findAll("option")
            options = [x.text for x in options]
            options = [re.sub("(.*) {{.*", "\\1", x) for x in options if re.match(".*\\(1\\)", x)]
            options = ','.join(options)
            outM.write(str(child + '=' + options + "\n"))
        outM.close()
    except Exception as inst: 
        print("Erreur sur le lien %s"%(link))
        print(inst)
        os.remove(page_name)
        return()
        
    print("Ok > " + page_name)

# Parallélisation, sinon c'est vraiment trop long :D (100k fichiers)
pool = ThreadPool(20)
pool.map(telechargerPageAccident, missing_links)
pool.close()
pool.join()

# Si le chargement a été interrompu, le fichier meta est surement vide. Dans ce cas, 
pages = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
pages = [f for f in pages if os.path.getsize(folder + f) == 0]
pages = [re.sub('meta_(.*).txt', 'fiche_\\1.html', f) for f in pages]
print("Nombre de fiches corrompues : %d." % len(pages))

for page in pages : 
    print(page)
    try :
        os.remove(folder + page)
    except : 
        pass
