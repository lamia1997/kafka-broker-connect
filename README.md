# kafka-broker-connect
Ce projet premet en premier lieu à mettre des données Csv dans une base de données postgres qui se trouve en local puis connecter cette base à Kafka dans confluent grace à Kafka connect.
Etapes:
1-lancer le code insert.py sur le fichier csv World cup Player
2-lancer le Docker compose
3-mettre le dossier confluentinc-kafka-connect-jdbc-10.6.0 dans le path /usr/share/confluent-hub-components du conteneur connect
4-importer le fichier config.json sur Connect dans confluent
5-Creer un stream sur la base ksql pour suivre le topic creer a partir de la base de donnees postgres.