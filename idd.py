import sqlite3
import json
import pandas as pd

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

##############################
#création des tables
##############################

cursor.execute("DROP TABLE IF EXISTS entreprise")
cursor.execute("DROP TABLE IF EXISTS offres")
cursor.execute("DROP TABLE IF EXISTS networking")
cursor.execute("DROP TABLE IF EXISTS transports")
conn.commit()

cursor.execute('''
CREATE TABLE offres (
    id TEXT PRIMARY KEY,
    intitule TEXT,
    description TEXT,
    lieu_travail_libelle TEXT,
    entreprise_nom TEXT,
    type_contrat TEXT,
    experience_exige TEXT,
    salaire_libelle TEXT,
    secteur TEXT
)
''')

cursor.execute('''
CREATE TABLE entreprise (
    nom TEXT,
    url TEXT
)
''')

cursor.execute('''
CREATE TABLE networking (
    id TEXT PRIMARY KEY,
    nom TEXT,
    timezone TEXT,
    url TEXT,
    date_debut TEXT,
    categorie TEXT
)
''')

cursor.execute('''
CREATE TABLE transports (
    station TEXT PRIMARY KEY,
    trafic INTEGER,
    correspondance1 TEXT,
    correspondance2 TEXT,
    correspondance3 TEXT,
    correspondance4 TEXT,
    correspondance5 TEXT,
    ville TEXT
)
''')
conn.commit()

cursor.execute('''
CREATE TRIGGER after_insert_offres
AFTER INSERT ON offres
BEGIN
    INSERT INTO entreprise (nom, url)
    SELECT NEW.entreprise_nom, NULL
    WHERE NOT EXISTS (SELECT 1 FROM entreprise WHERE nom = NEW.entreprise_nom);
END;
''')

cursor.execute('''
CREATE TRIGGER after_delete_offres
AFTER DELETE ON offres
BEGIN
    DELETE FROM entreprise WHERE nom NOT IN (SELECT entreprise_nom FROM offres);
END;
''')

cursor.execute('''
CREATE TRIGGER after_update_offres
AFTER UPDATE ON offres
BEGIN
    UPDATE entreprise SET nom = NEW.entreprise_nom WHERE nom = OLD.entreprise_nom;
END;
''')

conn.commit()

##############################
#insertion des valeurs dans les tables
##############################

with open('jobsoffer.json', encoding='utf-8') as f:
    data1 = json.load(f)

with open('francetravail.json', encoding='utf-8') as f:
    data2 = json.load(f)

offres_data = []
entreprises_data = set()

for offre in data1:
    offres_data.append((
        offre.get('id'),
        offre.get('title'),
        offre.get('description'),
        offre.get('location'),
        offre.get('companyName'),
        offre.get('contractType'),
        offre.get('experienceLevel'),
        offre.get('salary'),
        offre.get('sector')
    ))
    entreprises_data.add((offre.get('companyName'), offre.get('companyUrl')))

for offre in data2["resultats"]:
    offres_data.append((
        offre.get("id"),
        offre.get("intitule"),
        offre.get("description"),
        offre.get("lieuTravail", {}).get("libelle"),
        offre.get("entreprise", {}).get("nom"),
        offre.get("typeContrat"),
        offre.get("experienceExige"),
        offre.get("salaire", {}).get("commentaire"),
        offre.get("secteurActiviteLibelle")
    ))
    entreprises_data.add((offre.get("entreprise", {}).get("nom"), None))

cursor.executemany("""
INSERT INTO offres (id, intitule, description, lieu_travail_libelle, entreprise_nom, type_contrat, experience_exige, salaire_libelle, secteur)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""", offres_data)

cursor.executemany("""
INSERT INTO entreprise (nom, url) VALUES (?, ?)
""", list(entreprises_data))

conn.commit()

df_events = pd.read_csv("events.results.csv")
df_transport = pd.read_csv("trafic-annuel-entrant-par-station-du-reseau-ferre-2020.csv", delimiter=";")

networking_data = [(
    str(row['events.results.id']), row['events.results.name'], row['events.results.timezone'],
    row['events.results.tickets_url'], row['events.results.start_date'], row['suggested_categories[0].name']
) for _, row in df_events.iterrows()]

transport_data = [(
    row['Station'], row['Trafic'], row['Correspondance_1'], row['Correspondance_2'], row['Correspondance_3'], row['Correspondance_4'], row['Correspondance_5'], row['Ville']
) for _, row in df_transport.iterrows()]

cursor.executemany("""
INSERT INTO networking (id, nom, timezone, url, date_debut, categorie)
VALUES (?, ?, ?, ?, ?, ?)
""", networking_data)

cursor.executemany("""
INSERT INTO transports (station, trafic, correspondance1, correspondance2, correspondance3, correspondance4, correspondance5, ville)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", transport_data)

conn.commit()

##############################
#création des views 
##############################

cursor.execute("DROP VIEW IF EXISTS offres_view")
cursor.execute("DROP VIEW IF EXISTS offres_networking_view")
cursor.execute("DROP VIEW IF EXISTS offres_transport_view")
cursor.execute("DROP VIEW IF EXISTS recrutement_networking_view")
cursor.execute("DROP VIEW IF EXISTS sector_networking_view")
cursor.execute("DROP VIEW IF EXISTS recent_networking_offres_view")
cursor.execute("DROP VIEW IF EXISTS company_sector_city_view")
cursor.execute("DROP VIEW IF EXISTS formation_events_view")

cursor.execute('''
CREATE VIEW offres_view AS
SELECT o.id, o.intitule, o.description, o.lieu_travail_libelle,
       o.entreprise_nom, e.url AS entreprise_url,
       o.type_contrat, o.experience_exige, o.salaire_libelle, o.secteur
FROM offres o
LEFT JOIN entreprise e ON o.entreprise_nom = e.nom
''')

cursor.execute('''
CREATE VIEW offres_networking_view AS
SELECT o.id, o.intitule, o.secteur, n.nom AS event_name, 
        n.url AS event_url, n.date_debut
FROM offre o
JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
''')

cursor.execute('''
CREATE VIEW offres_transport_view AS
SELECT o.id, o.intitule, o.lieu_travail_libelle, t.station, t.trafic
FROM offres o
JOIN transports t ON LOWER(o.lieu_travail_libelle) LIKE LOWER('%' || t.ville || '%') ORDER BY t.trafic DESC
''')

cursor.execute('''
CREATE VIEW recrutement_networking_view AS
SELECT o.entreprise_nom, COUNT(o.id) AS nombres_offres, n.nom AS event_name, n.date_debut
FROM offres o
JOIN networking n ON LOWER(o.entreprise_nom) LIKE LOWER('%' || n.nom || '%')
''')

cursor.execute('''
CREATE VIEW sector_networking_view AS
SELECT o.secteur, COUNT(o.id) AS nombre_offres, COUNT(n.id) AS nombre_evenements
FROM offres o
LEFT JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
GROUP BY o.secteur
ORDER BY nombre_offres DESC, nombre_evenements DESC
''')

cursor.execute('''
CREATE VIEW recent_networking_offres_view AS
SELECT o.id, o.intitule, o.secteur, n.nom AS event_name, n.date_debut
FROM offres o
JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
WHERE n.date_debut >= SYSDATE - 30
''')

cursor.execute('''
CREATE VIEW company_sector_city_view AS
SELECT entreprise_nom, COUNT(DISTINCT lieu_travail_libelle) AS nombre_villes, 
       COUNT(DISTINCT secteur) AS nombre_secteurs
FROM offres
GROUP BY entreprise_nom
ORDER BY nombre_villes DESC, nombre_secteurs DESC
''')

cursor.execute('''
CREATE VIEW formation_events_view AS
SELECT o.id, o.intitule, o.entreprise_nom, n.nom AS event_name, n.date_debut, n.url
FROM offres o
JOIN networking n ON LOWER(o.entreprise_nom) LIKE LOWER('%' || n.nom || '%')
WHERE LOWER(n.categorie) LIKE '%formation%'
''')

conn.commit()

##############################
#fonction de mise à jour automatiques des views et triggers
##############################

cursor.execute('''
CREATE TABLE IF NOT EXISTS trigger_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT,
    action TEXT,
    trigger_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.executescript('''
CREATE TRIGGER IF NOT EXISTS after_insert_offres
AFTER INSERT ON offres
BEGIN
    INSERT INTO trigger_log (table_name, action) VALUES ('offres', 'INSERT');
END;

CREATE TRIGGER IF NOT EXISTS after_update_offres
AFTER UPDATE ON offres
BEGIN
    INSERT INTO trigger_log (table_name, action) VALUES ('offres', 'UPDATE');
END;

CREATE TRIGGER IF NOT EXISTS after_delete_offres
AFTER DELETE ON offres
BEGIN
    INSERT INTO trigger_log (table_name, action) VALUES ('offres', 'DELETE');
END;
''')

query_offres = '''CREATE VIEW offres_view AS
SELECT o.id, o.intitule, o.description, o.lieu_travail_libelle,
       o.entreprise_nom, e.url AS entreprise_url,
       o.type_contrat, o.experience_exige, o.salaire_libelle, o.secteur
FROM offres o
LEFT JOIN entreprise e ON o.entreprise_nom = e.nom
'''

query_offres_networking = '''
CREATE VIEW offres_networking_view AS
SELECT o.id, o.intitule, o.secteur, n.nom AS event_name, 
        n.url AS event_url, n.date_debut
FROM offre o
JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
'''

query_offres_transport = '''CREATE VIEW offres_transport_view AS
SELECT o.id, o.intitule, o.lieu_travail_libelle, t.station, t.trafic
FROM offres o
JOIN transports t ON LOWER(o.lieu_travail_libelle) LIKE LOWER('%' || t.ville || '%') ORDER BY t.trafic DESC
'''

query_recrutement_networking = '''
CREATE VIEW recrutement_networking_view AS
SELECT o.entreprise_nom, COUNT(o.id) AS nombres_offres, n.nom AS event_name, n.date_debut
FROM offres o
JOIN networking n ON LOWER(o.entreprise_nom) LIKE LOWER('%' || n.nom || '%')
'''

query_sector_networking = '''
CREATE VIEW sector_networking_view AS
SELECT o.secteur, COUNT(o.id) AS nombre_offres, COUNT(n.id) AS nombre_evenements
FROM offres o
LEFT JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
GROUP BY o.secteur
ORDER BY nombre_offres DESC, nombre_evenements DESC
'''

query_recent_networking = '''
CREATE VIEW recent_networking_offres_view AS
SELECT o.id, o.intitule, o.secteur, n.nom AS event_name, n.date_debut
FROM offres o
JOIN networking n ON LOWER(o.secteur) = LOWER(n.categorie)
WHERE n.date_debut >= SYSDATE - 30
'''

query_company_sector_city = '''
CREATE VIEW company_sector_city_view AS
SELECT entreprise_nom, COUNT(DISTINCT lieu_travail_libelle) AS nombre_villes, 
       COUNT(DISTINCT secteur) AS nombre_secteurs
FROM offres
GROUP BY entreprise_nom
ORDER BY nombre_villes DESC, nombre_secteurs DESC
'''

query_formation_events = '''
CREATE VIEW formation_events_view AS
SELECT o.id, o.intitule, o.entreprise_nom, n.nom AS event_name, n.date_debut, n.url
FROM offres o
JOIN networking n ON LOWER(o.entreprise_nom) LIKE LOWER('%' || n.nom || '%')
WHERE LOWER(n.categorie) LIKE '%formation%'
'''

def refresh_view(name, creation_query):
    cursor.execute(f"DROP VIEW IF EXISTS {name}")
    cursor.execute(creation_query)
    conn.commit()

def refresh_all_views():
    views = [
        ('offres_view', query_offres),
        ('offres_networking_view', query_offres_networking),
        ('offres_transport_view', query_offres_transport),
        ('recrutement_networking_view', query_recrutement_networking),
        ('sector_networking_view', query_sector_networking),
        ('recent_networking_offres_view', query_recent_networking),
        ('company_sector_city_view', query_company_sector_city),
        ('formation_events_view', query_formation_events),
    ]
    for name, query in views:
        refresh_view(name, query)

def check_and_refresh():
    cursor.execute("SELECT * FROM trigger_log WHERE table_name = 'offres'")
    logs = cursor.fetchall()

    if logs:
        print("\nChangements détectés sur 'offres' : Rafraîchissement des vues...")
        refresh_all_views()
        cursor.execute("DELETE FROM trigger_log WHERE table_name = 'offres'")
        conn.commit()
    else:
        print("Aucune mise à jour détectée sur 'offres'.")


#cursor.execute('''
#CREATE TRIGGER after_insert_offres_view
#AFTER INSERT ON offres
#BEGIN
#    SELECT refresh_view();
#END;
#''')

#cursor.execute('''
#CREATE TRIGGER after_update_offres_view
#AFTER UPDATE ON offres
#BEGIN
#    SELECT refresh_view();
#END;
#''')

#cursor.execute('''
#CREATE TRIGGER after_delete_offres_view
#AFTER DELETE ON offres
#BEGIN
#    SELECT refresh_view();
#END;
#''')

conn.commit()

cursor.close()
conn.close()

print("Données insérées et vue mise à jour avec succès !")

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

cursor.execute("SELECT * from networking limit 1")
networking = cursor.fetchall()

print("liste de networking : ")
for network in networking:
    print(networking)

cursor.close()
conn.close()

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

print("Tables :")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
for table in tables:
    print(f"- {table[0]}")

print("\nVues :")
cursor.execute("SELECT name FROM sqlite_master WHERE type='view';")
views = cursor.fetchall()
for view in views:
    print(f"- {view[0]}")

conn.close()