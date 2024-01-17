# py-rep

Una collezione di strumenti (librerie, notebooks) per generare reports sulle cripto-attività.

Il software è cresciuto in modo disordinato e non è pensato per un uso professionale, è un *work in progress*, ma può tornare utile per analizzare le transazioni e calcolare bilanci, giacenze fiscali e plusvalenze (secondo il metodo LIFO).
Lo metto quindi a disposizione di tutti: suggerimenti e aggiunte sono benvenuti!

## Disclaimer:
Questo software è fornito "così com'è", senza alcuna garanzia esplicita o implicita riguardo alla sua accuratezza, affidabilità o idoneità a uno scopo specifico. L'utilizzo di questo software è a proprio rischio. L'autore non si assume alcuna responsabilità per danni diretti, indiretti, accidentali, speciali, derivanti dall'uso o dall'incapacità di utilizzare il software.

Si tenga presente che questo software potrebbe non essere stato sottoposto a test estensivi e potrebbe contenere errori. 

L'autore si riserva il diritto di apportare modifiche al software in qualsiasi momento, senza preavviso. Continuando a utilizzare questo software, l'utente accetta di sollevare l'autore da qualsiasi responsabilità derivante dal suo utilizzo.

## Overview

### inserimento di transazioni e bilanci

Si inizia dalla cartella `recipients`. Ogni recipient è un contenitore di cripto-attività (wallet, exchange) di cui si deve possedere l'elenco delle transazioni in forma di file csv o excel. I formati degli elenchi delle transazioni sono diversi, quindi bisogna formattare queste ultime secondo uno schema comune ed inserirle in database. Contestualmente vengono calcolati e inseriti a database i bilanci relativi alle transazioni esaminate.
Le tabelle vengono manipolate con `pandas` e inserite in un database `sqlite` attraverso le librerie presenti in `/src`.

Prendere a modello i notebook usati per i wallets di bitcoin e ethereum e per gli exchanges kraken e the rock trading. Vedi README in cartella.

### cartella setup

Dopo aver inserito le transazioni in db, preparare i database attraverso i notebook della cartella `setup`. 

Questa è la parte più importante di py-rep. Vedere README in cartella.

### cartella apps

Notebooks di esempio per osservare transazioni e bilanci e calcolare giacenze e plusvalenze

### cartella data
qui vegono conservati i dati generati dalla cartella setup: transazioni, bilanci, prezzi e il database delle transazioni viste come aperture e chiusure di "posizioni".




 







