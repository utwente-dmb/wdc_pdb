This is a git repository from the utwente Gitlab server. To clone this
repository do:

git clone git@git.snt.utwente.nl:flokstra/wdc-data-converter.git

To use the python Postgresql connection package do:

pip3 install psycopg2

The data used by the package can be found on the site:

http://webdatacommons.org/largescaleproductcorpus/

A small 11 line sample file is (compress to .gz for use):

http://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/samples/sample_offersenglish.json

The big english repository used for testing is:

http://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/data/offers_english.json.gz

The .gitignore file for this projects prevents .gz file being added to the
repository.

To connect to the database copy the database.ini.tmpl to database.ini and fill
in your connection parameters. The *.ini files are not uploaded to the 
git repository for obvious security reasons.
