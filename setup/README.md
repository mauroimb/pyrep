# Setup
Una volta inserite in database le transazioni provenienti da exchanges e wallets, si devono eseguire in successione i notebooks della cartella "setup" per manipolarne i dati.
Il dettaglio delle operazioni svolte si trova nei notebook stessi.

È usata, tra le altre, la libreria `Prices` che legge i prezzi dalle API pubbliche di due servizi online e li conserva nel database `sqlite` *prices.db*. Vedi README nella cartella "/src/prices"

1. **setup**.
   Si parte dal notebook `setup.ipynb` che crea i database `sqlite` e un file *json* con l'elenco degli asset detenuti
2. **setup_giacenza**
   crea il database dei prezzi degli asset detenuti il 1° gennaio di ogni anno di interesse, conservato in un file excel (è il vecciho metodo, bisogna aggiornarlo e usare il database creato da *prices*) e lo riempie attraverso Prices.
3. **labels**
   ricerca ed elimina le transazioni "duplicate" e crea il database `db1.db`
4. **valorization**
   assegna un "valore" ad ogni transazione, considerando il prezzo giornaliero dell'asset più liquido.
5. **build_positions**
   Creazione di un dataframe di 'posizioni' a partire dalle transazioni. Ogni transazione di acquisto di assetA per assetB genera una posizione aperta in assetA, con relativa quantità e prezzo (in euro) e ne chiude una o più in assetB, calcolando la plus o minusvalenza realizzata.

# Formati delle tabelle


### transazioni (table `recipient_txs` in db.db)

| id | timestamp | type | info | in | in_iso | out | out_iso | fee | fee_iso |
|----|-----------|------|------|----|--------|-----|---------|-----|---------|
| text | integer | text | text | real>0 | text | real>0| text | real>0 | text |
| id della transazione, unique value   | unix timestamp in ms          | 'ext'/'trade'/'other' | informazioni sulla tx     | quantità di valuta in entrata   |   codice ISO (o stringa rappresentativa dell'asset)     | quantità di valuta in uscita     |         | commissioni    |         |

### bilanci (table `recipient_bal` in db.db)

| id | timestamp | iso1 | iso2 | etc... |
|----|-----------|------|------|----|
| text | integer | real>0 | real>0 | real>0 |
| id della transazione  | unix timestamp in ms          | bilancio1| bilancio2 | bilancio etc |   



### transazioni (table `txs` in db1.db)

sono incluse solo le transazioni scelte nel file `label.ipynb`

| id                                 | timestamp            | type                  | info                  | in                            | in_iso                                            | out                          | out_iso | fee         | fee_iso | label1          | label2               | label3                          |
| ---------------------------------- | -------------------- | --------------------- | --------------------- | ----------------------------- | ------------------------------------------------- | ---------------------------- | ------- | ----------- | ------- | --------------- | -------------------- | ------------------------------- |
| text                               | integer              | text                  | text                  | real>0                        | text                                              | real>0                       | text    | real>0      | text    | text            | text                 | text                            |
| id della transazione, unique value | unix timestamp in ms | 'ext'/'trade'/'other' | informazioni sulla tx | quantità di valuta in entrata | codice ISO (o stringa rappresentativa dell'asset) | quantità di valuta in uscita |         | commissioni |         | 'internal'/null | recipient accoppiato | id della transazione accoppiata |

### valorized (table `valorized` in db1.db)

Come sopra ma con una colonna in più, "value" che rappresenta il valore (in euro) scambiato nella transazione

### positions

| open_id                                    | open_t         | open_amount                  | open_price                | coin                  | amount             | close_id                                                 | close_t                    | close_price        | close_amount                                              | cgain       |
| ------------------------------------------ | -------------- | ---------------------------- | ------------------------- | --------------------- | ------------------ | -------------------------------------------------------- | -------------------------- | ------------------ | --------------------------------------------------------- | ----------- |
| id della transazione che apre la posizione | unix timestamp | quantità di asset acquistato | prezzo di apertura (real) | iso code asset (text) | quantità rimanente | id della transazione che chiude la posizione (se chiusa) | unix timestamp di chiusura | prezzo di chiusura | dovrebbe essere uguale ad amount se la posizione è chiusa | plusvalenza |



