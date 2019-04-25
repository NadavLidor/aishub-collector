#!/usr/bin/python
# -*- coding: utf-8 -*-

import json, sys, tempfile, os, bz2
import urllib.request
import pymysql.cursors
from pymysql.converters import escape_string

# Read settings from json file
with open('settings.json', 'r') as jsonf:
	SETTINGS = json.load(jsonf)

# Open database connection
db = pymysql.connect(host=SETTINGS['database']['host'],
	user=SETTINGS['database']['user'],
	password=SETTINGS['database']['password'],
	db=SETTINGS['database']['db'],
	charset='utf8mb4',
	cursorclass=pymysql.cursors.DictCursor)

# Retrieve data from AIShub to temp file
datafile = tempfile.mkstemp(prefix='aishub-collector-')
os.write(datafile[0], urllib.request.urlopen(SETTINGS['aishub']['url']).read())
os.close(datafile[0])

# Read data using gunzip2 as json object
with bz2.open(datafile[1], mode="rt", encoding="utf-8") as dataf:
	data = json.load(dataf)

success_count = 0
failed_count = 0
filtered_count = 0
total_count = 0

for this_ship in data[1]:
	sql = (
		'INSERT INTO `temp` SET `mmsi`=%s, `time`=%s, `longitude`=%s, `latitude`=%s, '
	 	'`cog`=%s, `sog`=%s, `heading`=%s, `navstat`=%s, '
		'`imo`=%s, `name`=%s, `callsign`=%s, `type`=%s, '
		'`a`=%s, `b`=%s, `c`=%s, `d`=%s, '
		'`draught`=%s, `dest`=%s, `eta`=%s'
		)

	total_count += 1

	if SETTINGS['data_filtering']['clear_empty_imo'] and this_ship['IMO'] == 0:
		filtered_count += 1
		continue

	try:
		with db.cursor() as cursor:
			cursor.execute(sql, (
				this_ship['MMSI'], this_ship['TIME'][:19], float(this_ship['LONGITUDE']), float(this_ship['LATITUDE']),
				float(this_ship['COG']), float(this_ship['SOG']), int(this_ship['HEADING']), int(this_ship['NAVSTAT']),
				this_ship['IMO'], this_ship['NAME'], this_ship['CALLSIGN'], int(this_ship['TYPE']),
				int(this_ship['A']), int(this_ship['B']), int(this_ship['C']), int(this_ship['D']),
				this_ship['DRAUGHT'], this_ship['DEST'], this_ship['ETA']
			))
		success_count += 1

		# Commit every 100 lines
		if ((success_count % 100) == 0):
			db.commit()
	except pymysql.Error as error:
		failed_count += 1
		code, message = error.args
		if SETTINGS['logging']['collector_error']:
			print (code, message)

# Print summary
if SETTINGS['logging']['collector_summary']:
	print ("committed: %d rows" % success_count)
	print ("failed to commit: %d rows" % (failed_count))
	print ("filtered out: %d rows" % (filtered_count))
	print ("total rows: %d" % total_count)

# Final commit
db.commit()
db.close()

# Remove temporary file
os.unlink(datafile[1])