# Import des modules nécessaires
import datetime
import mysql.connector

# Connexion à la base de données
# Veuillez remplacer 'utilisateur' et 'motdepasse' par vos identifiants MySQL
# et 'ma_base_de_donnees' par le nom de votre base de données
conn = mysql.connector.connect(user='utilisateur', password='motdepasse', host='localhost', database='ma_base_de_donnees')

# Création d'un objet curseur pour exécuter les requêtes SQL
cursor = conn.cursor()

# Création de la table 'individus' si elle n'existe pas déjà
cursor.execute("""
    CREATE TABLE IF NOT EXISTS individus (
        id INT AUTO_INCREMENT PRIMARY KEY,
        prenom VARCHAR(255),
        nom VARCHAR(255),
        sexe VARCHAR(10),
        date_de_naissance DATE,
        date_de_deces DATE,
        profession VARCHAR(255),
        nationalite VARCHAR(255)
    )
""")

# Lecture des données depuis les fichiers
# 'individus.txt' contient les informations individuelles
# 'nationalites.txt' contient les nationalités correspondantes aux individus
with open('individus.txt', 'r', encoding='utf-8') as f1, open('nationalites.txt', 'r', encoding='utf-8') as f2:
    # Création d'un dictionnaire pour stocker les nationalités
    nationalites = {}
    for line in f2:
        id, nationalite = line.strip().split(';')
        nationalites[id] = nationalite

    # Lecture de toutes les lignes de 'individus.txt' et stockage dans une liste
    individus_raw = list(f1)

    # Parcours des informations individuelles
    individus = []
    for i, line in enumerate(individus_raw, start=1):
        # Extraction des informations
        id, prenom, nom_complet, sexe, date_naissance, date_deces, profession = line.strip().split(';')

        # Vérification de la présence de l'individu dans 'nationalites.txt'
        if id not in nationalites:
            continue

        # Conversion des dates en objets datetime.date ou None si 'NA'
        date_naissance = None if date_naissance == 'NA' else datetime.datetime.strptime(date_naissance, '%d-%m-%Y').date()
        date_deces = None if date_deces == 'NA' else datetime.datetime.strptime(date_deces, '%d-%m-%Y').date()

        # Récupération de la nationalité correspondante à l'individu
        nationalite = nationalites[id]

        # Création d'un dictionnaire pour stocker les informations de l'individu
        individu = {
            'id': id,
            'prenom': prenom,
            'nom': nom_complet,
            'sexe': sexe,
            'date_naissance': date_naissance,
            'date_deces': date_deces,
            'profession': profession,
            'nationalite': nationalite
        }

        # Ajout de l'individu à la liste des individus
        individus.append(individu)

        # Affichage de la progression
        print(f"{i}/{len(individus_raw)} lignes traitées.")

# Boucle pour envoyer les données des individus sur la base de données
sql = "INSERT INTO individus (id, prenom, nom, sexe, date_de_naissance, date_de_deces, profession, nationalite) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE prenom=VALUES(prenom), nom=VALUES(nom), sexe=VALUES(sexe), date_de_naissance=VALUES(date_de_naissance), date_de_deces=VALUES(date_de_deces), profession=VALUES(profession), nationalite=VALUES(nationalite)"
val = [(individu['id'], individu['prenom'], individu['nom'], individu['sexe'], individu['date_naissance'], individu['date_deces'], individu['profession'], individu['nationalite']) for individu in individus]
cursor.executemany(sql, val)

# Enregistrement des changements dans la base de données
conn.commit()

# Fermeture de la connexion à la base de données
conn.close()