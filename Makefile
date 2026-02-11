ingest:
	python projects/sfmta_parking_citations/src/ingest.py

clean:
	python projects/sfmta_parking_citations/src/clean.py

mart:
	python projects/sfmta_parking_citations/src/build_marts.py