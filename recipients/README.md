## recipients

In ogni cartella vanno conservati i csv e/o gli excel delle transazioni del rispettivo wallet.

Nel file `process.ipynb` si svolge:
 - import dei dati con  `pandas`
 - trasformazione delle colonne del dataframe in modo che rispettino uno standard comune. Le colonne devono essere quelle ottenute con src.Database.txs_columns_list e se ne verifica la correttezza dando in argomento il dataframe risultante alla funzione `arrange` di *tools*. `txs = arrange(~customFunction~(pd.read_csv(...)))`
 - 'timestamp' è un *integer* che rappresenta il timestamp unix in millisecondi (funzione `formatTime` di *tools*)
 - `balances  = makeBalances()` per calcolare i bilanci delle criptoattività scambiate, dopo ogni transazione.
  
e infine 
```
db = Database('db.db')
db.insert_balances(name, balances)
db.insert_txs(name, df)

```
- per aprire il db e inserire transazioni e bilanci

la funzione` m = maxi(txs, balances, out = True)` concatena i dataframe di transazioni e bilanci e se *out = True* crea un file excel per osservare l'evoluzione del portafoglio. 

Usare come modello i notebook di esempio, per inserire i propri dati.
`
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
