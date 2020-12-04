# README

This is a git repository from the Twente University [Gitlab](https://git.snt.utwente.nl/) server. To clone this repository do:

```bash
git clone git@git.snt.utwente.nl:flokstra/wdc-data-converter.git
```

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the 
Postgres connection package psycopg2

```bash
pip3 install psycopg2
```

To connect to the database copy the <B>database.ini.tmpl</B> to <B>database.ini</B> and fill
in your connection parameters. The *.ini files are not uploaded to the 
git repository for obvious security reasons.

## Data
The data used by this project can be found on the site:

<http://webdatacommons.org/largescaleproductcorpus/>

A small 11 line sample file is (compress to .gz for use):

<http://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/samples/sample_offersenglish.json>

The big (16M+) English repository used for testing is:

<http://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/data/offers_english.json.gz>

The .gitignore file for this projects prevents .gz file being added to the
repository.

## Run

When the database connection is specified and the data is downloaded you can 
run the wdc2pg.py python program. 

```bash
python3 wdc2pg.py
```

To change the input json file or the basename (TABLEBASE} of
the generated Postgres table change the parameter of the <I>convert_json</I>() function
call at the bottom of the file.

The program creates two tables in the database. The first {TABLEBASE}_key contains the url, node_id pairs which are key to the offer and an autoincrement 'key'.
The other {TABLEBASE}_offer table contains this offer 'key' together  with the cluster_id and the 15 most used offer properties as listed in the WDC document Figure 1.
The attributes from the top15 are automatically generated from the TOP15PROPERTIES list in the wdc2pg.py file. When properties are added or removed they will
automatically be added to the table and converted by the python program.

The program is not very fast. The conversion of the complete 16M English offer will take approx 5 hours on my MacBook.
