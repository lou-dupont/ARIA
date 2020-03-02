from os import listdir
from os.path import isfile, join
from bs4 import BeautifulSoup
import requests
import lxml.html as lh
import pandas as pd
import re
import json
import csv
import os

folder = "ARIA_pages/"
pages = [f for f in listdir(folder) if isfile(join(folder, f))]
pages_html = [f for f in pages if re.match('fiche_(.*).html', f)]
pages_txt = [f for f in pages if re.match('meta_(.*).txt', f)]


def traiterMetadonnees(listeMeta, champ) :
    info = [x for x in listeMeta if re.match(champ + "=", x)][0]
    info = re.sub(champ+"=", '', info)
    info = re.sub('-\xa0-', '', info)
    info = re.sub('\\s+', ' ', info).lstrip()
    return(info)


def nomFichierPage(page):
    return folder + page

    
def nomFichierMeta(page):
    return folder + re.sub('fiche_(.*).html', 'meta_\\1.txt', page)


def traiterAccident(page) : 
    accident = {}

    # Lecture de la page
    raw_page = open(nomFichierPage(page), encoding="utf-8")
    content = BeautifulSoup(raw_page, "lxml")
    raw_page.close()
    
    # Récupération des informations
    accident['Titre'] = content.findAll("span")[0].text
    accident['Type de publication'] = "" # Initialisation à vide pour positionnement

    identification = content.find('div').findAll("strong")
    accident['Date'] = identification[1].text
    accident['Numéro ARIA'] = re.sub('N° ', '', identification[0].text)

    activite_code_libelle = content.find('em').text # Idem code_naf des metadonnées
    accident['Code NAF'] = re.sub('([^-]*) - (.*)', '\\2', activite_code_libelle)

    localisation = identification[2].text
    accident['Pays'] = re.sub('(.*) - (.*) - (.*)', '\\1', localisation)
    accident['Département'] = re.sub('(.*) - (.*) - (.*)', '\\2', localisation)
    accident['Commune'] = re.sub('(.*) - (.*) - (.*)', '\\3', localisation)

    # Traitement des métadonnées --> On appelle l'API de recherche avec un seul numéro ARIA
    meta_file = open(nomFichierMeta(page), 'r', encoding="utf-8")
    meta = meta_file.read().splitlines()
    meta_file.close()

    type_publication = traiterMetadonnees(meta, "types_de_publication")
    type_publication = re.sub('Accident,Fiche détaillée', 'Accident avec fiche détaillée', type_publication)

    accident["Type de publication"] = type_publication
    accident["Type d'accident"] = traiterMetadonnees(meta, "types_daccidents")
    accident["Type évènement"] = traiterMetadonnees(meta, "types_dvnement")
    accident["Matière"] = traiterMetadonnees(meta, "matieres")
    accident["Equipements"] = traiterMetadonnees(meta, "equipements")
    accident["Classe de danger CLP"] = traiterMetadonnees(meta, "classes_de_danger_clp")
    accident["Causes profondes"] = traiterMetadonnees(meta, "causes_profondes")
    accident["Causes premières"] = traiterMetadonnees(meta, "causes_premieres")
    accident["Conséquences"] = traiterMetadonnees(meta, "consequences")


    # Echelle au format 2H, 0En, 2Ec, 1M
    # Notes
    image_url = "https://www.aria.developpement-durable.gouv.fr/wp-content/themes/Total/library/images/recherche/"
    M = len(content.findAll("img", {'src': image_url + "note-jaune.png"}))
    H = len(content.findAll("img", {'src': image_url + "note-rouge.png"}))
    En = len(content.findAll("img", {'src': image_url + "note-verte.png"}))
    Ec = len(content.findAll("img", {'src': image_url + "note-bleue.png"}))
    accident["Echelle"] = "%sH, %sEn, %sEc, %sM"%(H, En, Ec, M)

    accident["Adresse web"] = content.find("link")['href']
    accident["Contenu"] = content.find("div", {"class" : "content"}).text


    return(accident)

# Boucle sur toutes les pages
# On lit la page HTML, dont on extrait peu d'infos.
# On appelle l'API de recherche afin d'extraire les métadonnées (absentes de la page html assez svt)

fiches_en_erreur = []
accidents = []

from datetime import datetime
print(datetime.now())

i = 0
for page in pages_html: 
    if i % 1000 == 0: 
        print("Après %d pages, heure %s" % (i, datetime.now()))
    i = i+1
    try:
        accidents.append(traiterAccident(page))
    except:
        fiches_en_erreur.append(page)
        if os.path.exists(nomFichierPage(page)):
            os.remove(nomFichierPage(page))
        if os.path.exists(nomFichierMeta(page)):
            os.remove(nomFichierMeta(meta))

print(datetime.now())
print("Nombre de fiches en erreur détruites à télécharger à nouveau : %d" % len(fiches_en_erreur))

# Passage du dictionnaire en tableau
df = pd.DataFrame(accidents, columns=accidents[0].keys())

# Sauvegarde
df.to_csv('ARIA.csv', encoding='utf-8', index=False)
df.to_excel('ARIA.xlsx', index=False)
