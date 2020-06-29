**Relazione progetto Industrial Informatics**

Cannizzaro Michele, Grasso Michele

**Introduzione**

Il "Reference Architectural Model for Industrie 4.0", più noto come RAMI 4.0, raccomanda a partire da Aprile 2015 lo standard IEC 62541 Open Platform Communication Unified Architecture (OPC UA) come unico standard per il livello di comunicazione al fine di garantire l'interoperabilità di dispositivi utilizzati nel sistema di produzione (CIM), quali PLC. A tutti i prodotti etichettati come industrie 4.0, dalla categoria più bassa a quella più alta (in ordine: "Basic", "Ready", "Full"), è richiesto l'indirizzamento via TCP/UDP o IP e l'integrazione con l'information model OPC UA. Alla luce di questo, il progetto che segue è stato realizzato al fine di conoscere in modo più approfondito ed utilizzare nella pratica l'Information Model OPC UA.

**Scenario**

L'intero progetto si basa sulla realizzazione di un Client Gateway OPC UA, il quale dopo aver ottenuto i valori desiderati da uno o più Server OPC UA, mediante Polling o sottoscrizione con Monitored Items, li invia al servizio di Messaging di Apache Kafka. Il sottoscrittore o i sottoscrittori al Kafka topic, ricevuti i valori provenienti dal Broker ne fanno l'upload sul cloud in due forme differenti: sotto forma di blobs su Microsoft Azure Blob Storage e sotto forma di collezioni e documenti su database Cloud Firestore di Firebase. Infine, in quest'ultimo caso si alimenta una web app che realizza una dashboard dinamica scritta in React e disponibile sul cloud tramite servizio di hosting offerto da Firebase.

![Imgur](https://i.imgur.com/u0cUc0D.png)

Fig.1 Project Structure


**Scelte implementative**

Il linguaggio che si è usato in modo predominante è Python.

Il progetto è stato realizzato come repository su GitHub, consultabile all'indirizzo: [https://github.com/GrassoMichele/OPCUA-Client-Kafka-Gateway](https://github.com/GrassoMichele/OPCUA-Client-Kafka-Gateway).

Apache Kafka è stato implementato tramite la soluzione CloudKarafka ([https://www.cloudkarafka.com/](https://www.cloudkarafka.com/)). Inoltre per proteggere i dati trasmessi sul servizio di messaging si è operata una crittografia simmetrica, tramite il modulo python Fernet.

**app.py**

Il progetto si compone di un'applicazione principale ( **app.py** ) che rappresenta il Client Gateway OPC UA, produttore dei records di Apache Kafka e basato sullo stack di comunicazione FreeOpcUa in Python ([https://github.com/FreeOpcUa/python-opcua](https://github.com/FreeOpcUa/python-opcua)).

Inizialmente si effettua una lettura da un file di configurazione ( **config.json** ) contenente i "Servers" ai quali ci si vuole collegare fornendo per ciascuno di essi la possibilità di definire:

- L'indirizzo del server
- Le sottoscrizioni del server
- Il tipo di autenticazione ("anonymous", "username/password", "certificate")
- I nodi da monitorare

La generica **sottoscrizione** è caratterizzata da:

- subId: Identificatore univoco della sottoscrizione all'interno del server
- publishingInterval: (in millisecondi)
- lifetimeCount
- maxKeepAliveCount
- maxNotificationsPerPublish
- publishingEnabled: "True", "False"
- priority

Per ciascun nodo è possibile definire i seguenti campi:

- nodeId
- Publishing Mode
  - type: "polling", "monitoredItem"

Se il type è **polling** è possibile settare:

- maxAge
- timestampsToReturn: "Neither", "Both", "Server", "Source"
- pollingRate: (in millisecondi)

Se invece, il type è **monitoredItem** è possibile settare:

- subscriptionId
- samplingInterval: (in millisecondi)
- monitoringMode: "Disabled", "Sampling", "Reporting"
- queueSize
- queueDiscardOldest: "True", "False"

- filter:
  - Trigger: "Status", "SatusValue", "StatusValueTimestamp"
  - DeadbandType: "None", "Absolute", "Percent"
  - DeadbandValue: (Double)

Infine, è necessario definire l'indirizzo del KafkaServer, sempre all'interno dello stesso file di configurazione. Nel caso in esame sono stati utilizzati i tre servers (Broker) offerti dal "Developer Duck Plan" (Free Plan) del servizio CloudKarafka.

Nota: Si è scelto di gestire tutte le funzioni che verranno trattate nel seguito all'interno del package **utility** : moduli **opcua\_custom\_lib** e **kafka\_custom\_lib**.

Effettuata la lettura del suddetto file, si procede alla configurazione delle sottoscrizioni e dei nodi da monitorare (nelle varianti polling e monitored item) per ciascun server mediante la funzione _read\_nodes\_from\_json_ la quale verifica la correttezza dei dati immessi e ritorna due liste:

- nodes\_to\_handle: contiene per ciascun server una tupla di liste costituita dai nodi da leggere mediante polling e monitored item, secondo il seguente ordine (nodes\_to\_read\_s, nodes\_to\_monitor\_s).
- subscriptions\_to\_handle: contiene le informazioni relative alle sottoscrizioni.

Si procede alla connessione del client con i vari server facendo uso della funzione _servers\_connection_. Essa tenta di stabilire la connessione con ciascun server (_client\_connection_): in un primo momento viene interrogato il discovery endpoint così da ottenere l'elenco degli endpoints disponibili (_best\_endpoint\_selection_), successivamente, tra questi, viene scelto quello con security level maggiore la cui security policy rientra tra quelle contemplate dallo stack di comunicazione (None, Basic128Rsa15, Basic256, Basic256Sha256). Fatta esclusione per il security level pari a "None", si procede alla connessione attraverso la creazione del secure channel facendo uso del certificato e della chiave privata del client ( **client\_certificate.pem** , **client\_key.pem** ).

Si crea dunque la sessione e si procede con l'attivazione della stessa effettuando l'autenticazione dell'utente (_client\_auth_). Le tipologie di autenticazione permesse sono: Anonymous, Username/Password e Certificate. Nel caso in cui l'autenticazione dovesse fallire, si è deciso di tentare un accesso con autenticazione: Anonymous. Se anche questo dovesse fallire si utilizza una security policy pari a None.

![Imgur](https://imgur.com/MO0aKyY.png)

Fig.2 Best Endpoint Selection and Server Connection

Terminata l'operazione di connessione ai servers viene avviato un thread che gestisce la loro riconnessione in caso di caduta (_check\_servers\_status_). Il funzionamento si basa sull'interrogazione del ServerStatus node (ns=0;i=2259) di ciascun server, la cui irraggiungibilità viene classificata come assenza del server. La riconnessione prevede la creazione delle sottoscrizioni e dei monitored items dei nodi relativi al particolare server (_sub\_and\_monitored\_items\_service_).

Infine, vengono rimossi i nodi non presenti nell'Address Space (_removing\_invalid\_nodes_).

La funzione _servers\_connection_ ritorna clients\_list ovvero una lista di oggetti client, i quali contengono le informazioni sulle connessioni ai servers.

Effettuata la creazione del canale sicuro, della sessione e la sua successiva attivazione si procede con l'interrogazione dei nodi nelle due modalità e ciò avviene mediante le funzioni:

- _sub\_and\_monitored\_items\_service_: per ciascun server attivo vengono create le sottoscrizioni (con i parametri specificati nel file **config.json** ) solo se vi sono nodi da monitorare che le richiedono. In caso di successo, si procede con la creazione e la gestione dei monitored items associati, tramite la funzione _create\_monitored\_items_ la quale a sua volta richiama _sub\_monitored\_items\_creation_. Nel caso in cui un server dovesse rinegoziare alcuni dei parametri dei monitored items appena creati questo viene comunicato all'utente. Le notifiche per tutti monitored Item relativi ad una stessa sottoscrizione vengono gestite dal metodo _datachange\_notification_ della classe SubHandler come indicato dalla documentazione del SDK. Alla ricezione della notifica segue la pubblicazione della stessa sotto forma di record sulla coda Kafka con topic " **opcua-nodes**" mediante il metodo publish\_message presente nel package utility, modulo **kafka\_custom\_lib**.

**Nota** : All'inizio dell'esecuzione di app.py viene generata un'istanza del Kafka producer mediante funzione _connect\_kafka\_producer_ dello stesso modulo.

L'integrazione con Apache Kafka è avvenuta per mezzo della libreria kafka-python ([https://pypi.org/project/kafka-python/](https://pypi.org/project/kafka-python/)).

- _Polling\_service_: crea un numero di thread pari ai diversi pollingRate dei nodi richiesti nel **config.json** , indipendentemente dal particolare server. Ciascun thread esegue la _polling\_function_ la quale interroga il server di competenza sullo specifico nodo e procede con l'invio delle informazioni, una volta ricevute, sulla coda Kafka con topic " **opcua-nodes**" realizzando così un record.

**Nota** : il record prodotto su Kafka (sia nel caso di polling che di monitored item) è un json ottenuto a partire da un dizionario le cui chiavi sono: "server", "node", "value", "status", "sourceTimestamp", "serverTimestamp".

![Imgur](https://imgur.com/crVmQSM.png)

Fig.3 Subscription Creation

![Imgur](https://imgur.com/8hP30zQ.png)


Fig.4 Receiving Nodes Updates

![Imgur](https://imgur.com/FirgzDs.png)


Fig.5 **app.py** Termination



**Consumers**

Sono state realizzate due possibili implementazioni di Consumer :

- kafkaConsumer\_Azure
- kafkaConsumer\_Firebase

Entrambe le implementazioni prevedono per il consumer una politica auto\_offset\_reset='latest' ovvero riceveranno solo i record prodotti dal momento in cui si sono sottoscritti al topic e quindi al loro avvio. Si consiglia dunque di avviare prima il consumer e dopo il producer ( **app.py** ).

**kafkaConsumer\_Azure**

Si trova all'interno della directory del progetto "kafka-consumer-azure".

Dispone di un file di configurazione " **config\_kafkaConsumer.json**" dal quale prende l'indirizzo del broker Kafka a cui si sottoscrive oltre alla stringa di connessione per accedere al Blob Storage di Azure.

Avviata l'applicazione si crea (nel caso in cui non fosse presente) il Container avente per nome il topic della coda Kafka.

Viene lanciato un thread che, dopo aver creato un'istanza del KafkaConsumer, si mette in ascolto dei records relativi al topic di interesse e per ciascuno di essi va a creare un blob. Questo contiene al suo interno il record.

I blob vengono organizzati in maniera gerarchica ovvero:

- ServerName Folder
  - Node Folder
    - Timestamp blob:

Es:

{"server": "MicheleGrassoPC:48010", "node": "ns=2;s=Demo.Dynamic.Scalar.Byte", "value": "218", "status": "Good", "sourceTimestamp": "2020-06-24 13:26:51.596364", "serverTimestamp": "2020-06-24 13:26:51.597363"}

Tale implementazione mediante blob permette una facile consultazione dei dati e si presta potenzialmente per Big Data analysis.

![Imgur](https://imgur.com/2vF5GHS.png)

Fig.6 **KafkaConsumer\_Azure.py**

![Imgur](https://imgur.com/515dtQl.png)
![Imgur](https://imgur.com/S2RcLoI.png)
![Imgur](https://imgur.com/ez0BgAX.png)
![Imgur](https://imgur.com/HYiefRj.png)

Fig.7 Azure Blob Storage Portal

**kafkaConsumer\_Firebase**

Si trova all'interno della directory del progetto "kafka-consumer-firebase".

Dispone di un file di configurazione "config\_kafkaConsumer.json" da cui ricava l'indirizzo del broker Kafka e del topic di interesse.

È presente un file " **opcua-client-kafka-gateway-firebase-adminsdk-hn72v-4c7137a285.json**" da cui ricava le informazioni necessarie per il servizio Cloud Firestore di Firebase.

L'applicazione opera in maniera analoga a quella esposta precedentemente. La differenza sostanziale la si ha nella scrittura su database. Si utilizza una Collezione "OpcUaNodes" e per ogni record prelevato da Kafka viene creato un documento con id univoco e avente per campi: "value", "node", "server", "sourcetimestamp", "servertimestamp", "status".

![Imgur](https://imgur.com/jCTQOjq.png)

Fig.8 **KafkaConsumer\_Firebase.py**

**Nota** : Il broker Kafka e il coordinator Zookeeper sono stati entrambi installati in locale.

![Imgur](https://imgur.com/UWfxeOE.png)

Fig.9 CloudFirestore

**Dashboard**

La Dashboard è una Web App scritta in React che legge dinamicamente i documenti generati dal kafkaConsumer\_Firebase ed ospitata sul cloud mediante servizio hosting di Firebase e consultabile all'indirizzo "[https://opcua-client-kafka-gateway.web.app/](https://opcua-client-kafka-gateway.web.app/)"

![Imgur](https://imgur.com/5TRYKAx.png)

Fig.10 Dashboard

**Prerequisiti**

- Python 3.8.3
- Librerie di utilità: è stato realizzato uno script autoImportRequiremts.py il quale installa automaticamente i requirements contenuti all'interno del file requirements.txt
- Broker Kafka e Zookeeper

**How to Use**

Per poter avviare correttamente l'applicazione è necessario:

1. Avviare Zookeper (Su windows: zookeeper-server-start.bat ../../config/zookeeper.properties)
2. Avviare Kafka (Su windows: kafka-server-start.bat ../../config/server.properties)
3. Avviare i consumer
4. Avviare app.py
5. Consultare la Dashboard.

**Built With**

- FreeOpcUa sdk
- Apache Kafka
- kafka-python