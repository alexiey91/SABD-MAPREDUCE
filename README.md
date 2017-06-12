# Progetto SABD HADOOP

# Requisiti del progetto

Lo scopo del progetto è di rispondere, utilizzando il framework Apache Hadoop, ad alcune query riguardanti il dataset **MovieLens** (http://movielens.org), in particolare la versione 20M. 
Tale dataset contiene 20 milioni di valutazioni (su scala da 0 a 5 stelle) effettuate da 138000 utenti e riguardanti 27000 film, appartenenti a diversi generi cinematografici.
Il dataset e memorizzato in 6 file di testo, nel formato comma-separated value (csv). In particolare, i file di interesse per il progetto sono: *movies.csv* e *ratings.csv*. Il primo contiene informazioni sui film; ogni riga del file (eccetto la prima di intestazione) ha il formato:
**movieId,title,genres** dove:

• **movieId** è l’ID del film; 

• **title** è il titolo del film; 

• **genres** è una lista (i cui elementi sono separati da |) dei generi attribuiti al film; 
i valori possibili sono:
*Action, Adventure, Animation, Children’s, Comedy, Crime, Documentary,
Drama, Fantasy, Film-Noir, Horror, Musical, Mystery, Romance, Sci-Fi,
Thriller, War, Western, (no genres listed)*.

Il file *ratings.csv* contiene le valutazioni dei film; ogni riga del file (eccetto la prima di intestazione)
ha il formato:
**userId,movieId,rating,timestamp**
dove:

• **userId** è l’ID dell’utente che ha inserito la valutazione del film; 

• **movieId** è l’ID del film; 

• **rating** è la valutazione del film, su una scala a 5 stelle, con incremento di mezza stella (da 0.5 a 5.0 stelle).

• **timestamp** è la data della valutazione, rappresentata in secondi a partire dalla mezzanotte UTC del 1 Gennaio 1970.

Le query a cui rispondere sono:
1. Individuare i film con una valutazione maggiore o uguale a 4.0 e valutati a partire dal 1 Gennaio 2000.
2. Calcolare la valutazione media e la sua deviazione standard per ciascun genere di film.
3. Trovare i 10 film che hanno ottenuto la più alta valutazione nell’ultimo anno del dataset (dal 1 Aprile 
2014 al 31 Marzo 2015) e confrontare, laddove possibile, la loro posizione nella classifica rispetto a
quella conseguita nell’anno precedente (dal 1 Aprile 2013 al 31 Marzo 2014).

Si chiede inoltre di valutare sperimentalmente i tempi di processamento delle 3 query sulla piattaforma
di riferimento usata per la realizzazione del progetto. Tale piattaforma puo essere un nodo standalone oppure 
in alternativa e possibile utilizzare un servizio Cloud per Hadoop (ad es. Amazon EMR o Google Dataproc), 
utilizzando i rispettivi grant a disposizione.
Infine, si chiede di realizzare la fase di data ingestion per:

• importare i dati di input in *HDFS*, eventualmente trasformando la rappresentazione dei dati in un altro
formato (e.g., *Avro*, *Parquet*, ...), usando un framework a scelta (e.g., *Flume*, *Kite*, ...);

• esportare i dati di output da *HDFS* ad un sistema di storage a scelta (e.g., *HBase*, ...).

# Installazione Ambiente
Per prima cosa bisogna aver preinstallato **Docker**.
Scaricare l'immagine *Docker di Hadoop file system*:
	
	 $ docker pull matnar/hadoop

Una volta installata l'immagine bisogna configurare l'ambiente di sviluppo.
 - E' possibile interagire con il nodo master scambiando i file attraverso il volume all'interno della directory */data*.   

		docker network create --driver bridge hadoop_network

		docker run -t -i -p 50075:50075 -p 50061:50060 -d --network=hadoop_network --name=slave1 matnar/hadoop
		docker run -t -i -p 50076:50075 -p 50062:50060 -d --network=hadoop_network --name=slave2 matnar/hadoop
		docker run -t -i -p 50077:50075 -p 50063:50060 -d --network=hadoop_network --name=slave3 matnar/hadoop
		docker run -t -i -p 50070:50070 -p 50060:50060 -p 50030:50030 -p 8088:8088 -p 19888:19888 --network=hadoop_network --name=master -v $PWD/hddata:/data matnar/hadoop

Prima di eseguire il cluster con *Hadoop*, bisogna prima configurarlo attraverso il comando 
 
 	   hdfs namenode -format  (N.B da utilizzare soltanto la prima volta)
	   $HADOOP_HOME/sbin/start-dfs.sh
	   $HADOOP_HOME/sbin/start-yarn.sh.

#### Installazione di Apache Flume
1. Per prima cosa bisogna aver preinstallato una **jdk** (la versione jdk 1.8 è consigliata).
2. Scaricare *Apache-flume-1.7.0-bin.tar.gz*.
3. Creare una directory all'interno dell'immagine *Docker*. 
    
  	  	mkdir usr/local/hadoop/Flume

4. Estrarre il file *.tar.gz* all'interno della nuova directory.
5. All'interno della directory bisogna aggiornare il file *.bashrc*.
 
 		 vim ~/.bashrc
 
    Inseriamo i seguenti Path.
 
	   export FLUME_HOME=/usr/local/hadoop/Flume
 	   export FLUME_CONF_DIR=$FLUME_HOME/conf
 	   export FLUME_CLASSPATH=$FLUME_CONF_DIR
 	   export PATH=$PATH:$FLUME_HOME/bin

6. Infine eseguiamo il comando  
			
	        source ~/.bashrc

7. Modifichiamo i file di configurazione presenti nel file *conf*. Rinominiamo il file **flume-env.sh.template** in **flume-env-sh** e aggiungiamo il path della directory di **Java**.
#### Installazione Hbase
1. Dowload *hbase-1.2.5.bin.tar.gz* dal sito ufficiale.
2. Estrarre tale file all'interno dell'immagine *Docker di Hadoop* nel percorso */usr/local/hbase* (una volta creata la cartella hbase)
3. Modificare il file di configurazione *hbase/conf/hbase-env.sh* inserendo il percorso della jdk installata all'interno dell'immagine *Docker*.
4. Aggiornare il file *.bashrc* aggiungendo:
  
   	 	export HBASE_HOME=/usr/lib/hbase
   	 	export PATH=$PATH:$HBASE_HOME/bin
   	 	source ~/.bashrc

5. Infine modificare il file /usr/local/hbase/conf/hbase-site.xml
 
 <pre><code>&lt;?xml version="1.0"?&gt;
 &lt;?xml-stylesheet type="text/xsl"ref="configuration.xsl&gt;
  &lt;configuration&gt;&lt;property&gt;
    &lt;name&gt;hbase.rootdir&lt;/name&gt;
    &lt;value&gt;file:///home/hduser/HBASE/hbase&lt;/value&gt;
  &lt;/property&gt;
  &lt;property&gt;
      &lt;name&gt;hbase.zookeeper.property.dataDir&lt;/name&gt;
      &lt;value&gt;/home/hduser/HBASE/zookeeper&lt;/value&gt;
    &lt;/property&gt;
    &lt;/configuration&gt;</code></pre>
    

#### Installare Apache Pig
1. Scaricare dal sito ufficiale la versione *pig-0.16.0.tar.gz*.
2. Creare la directory ed estrarre al suo interno *Apache Pig /usr/lib/pig*
3. Aggiorniamo il file *~/.bashrc* inserendo:
  	
	        export PIG_HOME=/usr/lib/pig
  	        export PATH=$PATH:$PIG_HOME/bin
  	        source /.bashrc

#### Esecuzione del Progetto
 Copiare il file *hadoopQuery.jar* all'iterno della directory */data*.
 Per eseguire una qualsiasi query occorre per prima cosa caricare i file .csv forniti da *MovieLens*.

1. Utilizzando le Api fornite da hadoop si esegue il seguente comando:
    
    	   hdfs dfs -put nomefile hdfs:///directory_di_destinazione 

2. Se invece si volesse caricare i dati all'iterno di *Hadoop* sfruttando il servizio di *Flume* occorrerà:
 
   2.1 Create due directory *SpoolDir* e *SpoolMovie* dove inserire rispettivamente i file *rating.csv* e *movie.csv*.
   
   2.2 Prelevare i file di configurazione di *Flume* presenti nel repository (*RatingFlume.conf e Movies.conf*) e caricarli 		all'iterno della directory *usr/local/hadoo/Flume/conf*
   
   2.3 Eseguire il comando di esecuzione di *Flume* per effettuare l'import di tali file all'interno di hdfs
   
  		 /usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f /usr/local/hadoop/Flume/conf/RatingFlume.conf -Dflume.root.looger=DEBUG,console -Xmx2g     
         /usr/local/hadoop/Flume/bin/flume-ng agent -n agent -c /usr/local/hadoop/Flume/conf -f  usr/local/hadoop/Flume/conf/MoviesFlume.conf -Dflume.root.looger=DEBUG,console -Xmx2g 

- Infine per eseguire il codice del progetto per ottenere i risultati delle query occore eseguire :
 
	   $hadoop jar hadoopQuery.jar query.tipo_query hdfs:///directory_file_rating.csv hdfs:/// directory_file_movie.csv hdfs:///directory_output > file_tempi_query.txt;

-  Nel caso in cui fosse stata scelta la query con il salvataggio dei dati all'interno del database Hbase occererà prima aver avviato il database *Hbase*.

#### Esecuzione Apache Pig
- Per prima cosa prelevare gli script.pig presenti all'interno del repository, copiarli all'interno della directory */usr/lib/pig/scripts*.

- Per poter eseguire correttamente la *query2.pig* occorre scaricare ed estrarre all'iterno di *usr/lib/pig* la libreria  
			
		datafu-hourglass-incubating-1.3.1

- Modificare le prime due righe di codice presenti all'iterno dei tre script stostituendo le directory di *Hadoop* contenenti i file *rating.csv* e *movie.csv*.

	 	rating = LOAD 'hdfs://master:54310/Ratings' USING PigStorage(',') as (userid:int, movieid:int, rating:double, timestamp:int );
        movies = LOAD 'hdfs://master:54310/Movies' USING PigStorage(',') as (movieid:int , title:chararray, genres: chararray);

- Infine eseguire il comando base di *Apache Pig* per eseguire il codice

		$ pig -x mapreduce scripts/query_scelta.pig > result.query.txt

#### Eseguire il progetto
Una volta configurato tutto l'ambiente di sviluppo, come illustrato precedentemente, per calcolare tutte e tre le query è possibile sfruttare uno script bash da eseguire all'iterno della directory data/ dell'immagine docker di hadoop:
		
 	chmod 777 scriptProgetto.sh
	./scriptProgetto.sh

Questo script esegue tutte e 3 le query richieste salvandole all'interno del database Hbase.

Nel caso si volesse eseguire tutte e 3 le query salavando i risultati all'interno di *Hadoop File System* basterà eseguire il seguente script:
	
	chmod 777 scriptProgettoHdfs.sh
	./scriptProgettoHdfs.sh



