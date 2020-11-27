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

To change the input json file or the name of
the generated Postgres table change the parameter of the <I>convert_json</I>() function
call at the bottom of the file.
