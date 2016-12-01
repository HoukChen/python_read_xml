#coding=utf-8 
#!/usr/bin/python 

import xml.sax
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

class MapHandler(xml.sax.ContentHandler):

	def __init__(self,conn):
		self.conn = conn
		self.cur = conn.cursor()
		self.Current_Tag = ''
		# attributes for nodes
		self.Node_ID = ''
		self.Node_Name = ''
		self.Node_Poitype = ''
		self.Node_Lon = ''
		self.Node_Lat = ''
		
		# attributes for ways
		self.Way_ID = ''
		self.Way_Name = ''
		self.Way_Nodes = []

		# attributes for relations
		self.Relation_ID = ''
		self.Relation_Name = ''
		self.Relation_Nodes = []
		self.Relation_Ways = []

	def clear_nodes(self):
		self.Node_ID = ''
		self.Node_Name = ''
		self.Node_Poitype = ''
		self.Node_Lon = ''
		self.Node_Lat = ''

	def clear_ways(self):
		self.Way_ID = ''
		self.Way_Name = ''
		self.Way_Nodes = []

	def clear_relations(self):
		self.Relation_ID = ''
		self.Relation_Name = ''
		self.Relation_Nodes = []
		self.Relation_Ways = []


	def startElement(self, tag, attributes):
		if tag == 'node':
			self.Current_Tag = 'node'
			self.Node_ID = attributes['id']
			self.Node_Lon = attributes['lon']
			self.Node_Lat = attributes['lat']

		if tag == 'way':
			self.Current_Tag = 'way'
			self.Way_ID = attributes['id']

		if tag == 'relation':
			self.Current_Tag = 'relation'
			self.Relation_ID = attributes['id']

		if (self.Current_Tag == 'node') and (tag == 'tag') and (attributes.has_key('k')):
			if attributes['k'] == 'name':# or attributes['k'] == 'name:en':
				self.Node_Name = attributes['v']
			if attributes['k'] == 'poitype':
				self.Node_Poitype = attributes['v']

		if (self.Current_Tag == 'way') and (tag == 'nd') and (attributes.has_key('ref')):
			self.Way_Nodes.append(attributes['ref'])
		if (self.Current_Tag == 'way') and (tag == 'tag') and (attributes.has_key('k')):
			if attributes['k'] == 'name':
				self.Way_Name = attributes['v']

		if (self.Current_Tag == 'relation') and (tag == 'member') and (attributes.has_key('type')):
			if attributes['type'] == 'way':
				self.Relation_Ways.append(attributes['ref'])
			if attributes['type'] == 'node':
				self.Relation_Nodes.append(attributes['ref'])
		if (self.Current_Tag == 'relation') and (tag == 'tag') and (attributes.has_key('k')):
			if attributes['k'] == 'name':
				self.Relation_Name = attributes['v']

	def endElement(self,tag):
		if tag == 'node':
			self.cur.execute("insert into Node values\
				('"+self.Node_ID+"','"+self.Node_Name+"','"+self.Node_Poitype+"','"+self.Node_Lon+"','"+self.Node_Lat+"')")
			#self.conn.commit()
			print 'Node_ID',self.Node_ID
			self.clear_nodes()
		
		if tag == 'way':
			self.cur.execute("insert into Way values\
				('"+self.Way_ID+"','"+self.Way_Name+"')")
			for element in self.Way_Nodes:
				self.cur.execute("insert into Way_Node values\
					('"+self.Way_ID+"','"+element+"')")
			#self.conn.commit()
			print 'Way_ID',self.Way_ID
			self.clear_ways()

		if tag == 'relation':
			self.cur.execute("insert into Relation values\
				('"+self.Relation_ID+"','"+self.Relation_Name+"')")
			#self.conn.commit()
			for element in self.Relation_Nodes:
				self.cur.execute("insert into Relation_Node values\
					('"+self.Relation_ID+"','"+element+"')")
			for element in self.Relation_Ways:
				self.cur.execute("insert into Relation_Way values\
					('"+self.Relation_ID+"','"+element+"')")
			print 'Relation_ID',self.Relation_ID
			self.clear_relations()

if __name__ == "__main__": 
	conn = MySQLdb.connect(
		host = 'localhost',
		port = 3306,
		user = 'root',
		passwd = '',
		db = 'mapdata',
		charset='utf8'
		)

	cur = conn.cursor()
	cur.execute("create table Node(\
		Node_ID varchar(20), \
		Node_Name varchar(100), \
		Node_Poitype varchar(50), \
		Node_Lon double, \
		Node_Lat double, \
		primary key (Node_ID))")
	cur.execute("create table Way(\
		Way_ID varchar(20), \
		Way_Name varchar(100), \
		primary key (Way_ID))")
	cur.execute("create table Relation(\
		Relation_ID varchar(20), \
		Relation_Name varchar(100), \
		primary key (Relation_ID))")
	cur.execute("create table Way_Node(\
		Way_ID varchar(20), \
		Node_ID varchar(20))")
	cur.execute("create table Relation_Node(\
		Relation_ID varchar(20), \
		Node_ID varchar(20))")
	cur.execute("create table Relation_Way(\
		Relation_ID varchar(20), \
		Way_ID varchar(20))")
	cur.close()
	# create a XMLReader 
	parser = xml.sax.make_parser() 
	# turn off namepsaces 
	parser.setFeature(xml.sax.handler.feature_namespaces, 0) 

	# rewrite ContextHandler 
	Handler = MapHandler(conn) 
	parser.setContentHandler(Handler) 
	parser.parse("shanghai_dump.osm")
	
	conn.commit()
	conn.close()
