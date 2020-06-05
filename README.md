# FootballApp

###### Les données <span style="color:red">sont filtrés</span> pour supprimer les lignes qui ne sont pas correctement formatés.


# Context
Ce projet est destiné à découvrir le traitement Big Data et l'utilisation de Spark en Python dans le cadre d'une formation scolaire.

# Description du projet

Ce projet à pour but de : 

* calculer des données statistiques à partir d'un fichier .csv correctement formatés
* enregistrer ces données calculées sous la forme d'un fichier au format .parquet
* associer ces données avec les données du fichier .csv pour en faire une seul dataFrame
* sauvegarder ce dataFrame dans un autre fichier au format .parquet partitionné par années puis par mois

Projet réalisé sous <span style="color:green">Python 3.6</span>.

# Stockage des données

* Le fichier .csv source doit être fournis dans un dossier "resource" et doit porter le nom df_matches.csv. Il doit contenir des formatées de la même façon que celui disponible sur le site : ​https://www.data.gouv.fr/fr/datasets/histoire-de-lequipe-de-france-de-football/ 
* Après l'exécution du programme, les fichiers générés se trouveront dans un dossier "output"

# Prérequis

* Apache Spark
* Python 3.x
* pyspark pour Python
* setuptools pour Python

# Lancer le programme
Pour générer l'archive .egg à partir du package :
```python setup.py bdist_egg```

Pour lancer cet archive via spark en local :
```spark-submit --master local --py-files dist\```<span style="color:blue">LeNomDuFichierExecutable</span>```.egg launch.py```

Pour lancer les tests :
```python -m esgi.exo.FootballAppTest.FootballAppTest```