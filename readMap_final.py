#coding=utf-8 
#!/usr/bin/python 

import xml.sax
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

class MapHandler(xml.sax.ContentHandler):

	def __init__(self,conn):

		self.Current_Tag = ''

		# attributes for MySQL
		self.conn = conn
		self.cur = conn.cursor()

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

		# attributes for saving contents
		self.has_tag = False
		self.content = ''

	def clear_nodes(self):
		self.Node_ID = ''
		self.Node_Name = ''
		self.Node_Poitype = ''
		self.Node_Lon = ''
		self.Node_Lat = ''
		self.content = ''
		self.has_tag = False


	def clear_ways(self):
		self.Way_ID = ''
		self.Way_Name = ''
		self.Way_Nodes = []
		self.content = ''
		self.has_tag = False

	def clear_relations(self):
		self.Relation_ID = ''
		self.Relation_Name = ''
		self.Relation_Nodes = []
		self.Relation_Ways = []
		self.content = ''
		self.has_tag = False


	def startElement(self, tag, attributes):
		if tag == 'node':
			# node info
			self.Current_Tag = 'node'
			self.Node_ID = attributes['id']
			self.Node_Lon = attributes['lon']
			self.Node_Lat = attributes['lat']

			# node content
			self.content += "<node id=\""
			self.content += attributes["id"]
			self.content += "\" version=\""
			self.content += attributes["version"]
			self.content += "\" timestamp=\""
			self.content += attributes["timestamp"]
			self.content += "\" changeset=\""
			self.content += attributes["changeset"]
			self.content += "\" lat=\""
			self.content += attributes["lat"]
			self.content += "\" lon=\""
			self.content += attributes["lon"]
			self.content += "\">\n"
			'''
			for index in range(len(attributes.keys())):
				self.content += attributes.keys()[index]
				self.content += "=\""
				self.content += attributes.values()[index]
				self.content += "\" "
			'''
			 
		if tag == 'way':
			# way info
			self.Current_Tag = 'way'
			self.Way_ID = attributes['id']

			# way content
			self.content += "<way id=\""
			self.content += attributes["id"]
			self.content += "\" version=\""
			self.content += attributes["version"]
			self.content += "\" timestamp=\""
			self.content += attributes["timestamp"]
			self.content += "\" changeset=\""
			self.content += attributes["changeset"]
			self.content += "\">\n" 
			'''
			for index in range(len(attributes.keys())):
				self.content += attributes.keys()[index]
				self.content += "=\""
				self.content += attributes.values()[index]
				self.content += "\" "
			'''

		if tag == 'relation':
			# relation info
			self.Current_Tag = 'relation'
			self.Relation_ID = attributes['id']

			# relation content
			self.content += "<relation id=\""
			self.content += attributes["id"]
			self.content += "\" version=\""
			self.content += attributes["version"]
			self.content += "\" timestamp=\""
			self.content += attributes["timestamp"]
			self.content += "\" changeset=\""
			self.content += attributes["changeset"]
			self.content += "\">\n" 
			'''
			for index in range(len(attributes.keys())):
				self.content += attributes.keys()[index]
				self.content += "=\""
				self.content += attributes.values()[index]
				self.content += "\" "
			'''

		# for table info
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

		# for content
		if tag == "member":
			self.content += "\t<member type=\""
			self.content += attributes["type"]
			self.content += "\" ref=\""
			self.content += attributes["ref"]
			self.content += "\" role=\""
			self.content += attributes["role"]
			self.content += "\"/>\n"
		if tag=="nd" or tag=="tag":
			self.has_tag = True
			self.content += "\t<"
			self.content += tag
			for index in range(len(attributes.keys())):
				self.content += " "
				self.content += attributes.keys()[index]
				self.content += "=\""
				self.content += attributes.values()[index]
				self.content += "\""
			self.content += "/>\n" 


	def endElement(self,tag):
		if tag == "node":
			# for node info
			self.cur.execute("insert into Node values\
				('"+self.Node_ID+"','"+self.Node_Name+"','"+self.Node_Poitype+"','"+self.Node_Lon+"','"+self.Node_Lat+"')")
			
			# for node content
			if self.has_tag:
				self.content += "</node>"
			else:
				self.content = self.content[0:len(self.content)-2] + "/>"
			self.cur.execute("insert into Node_Info values\
				('"+self.Node_ID+"','"+self.content+"')")
			
			print "node: " + self.Node_ID
			self.clear_nodes()

		if tag == "way":
			# for way info
			self.cur.execute("insert into Way values\
				('"+self.Way_ID+"','"+self.Way_Name+"')")
			for element in self.Way_Nodes:
				self.cur.execute("insert into Way_Node values\
					('"+self.Way_ID+"','"+element+"')")

			# for way content
			if self.has_tag:
				self.content += "</way>"
			else:
				self.content = self.content[0:len(self.content)-2] + "/>"
			self.cur.execute("insert into Way_Info values\
				('"+self.Way_ID+"','"+self.content+"')")
			
			print "way: " + self.Way_ID
			self.clear_ways()

		if tag == "relation":
			# for relation info
			self.cur.execute("insert into Relation values\
				('"+self.Relation_ID+"','"+self.Relation_Name+"')")
			#self.conn.commit()
			for element in self.Relation_Nodes:
				self.cur.execute("insert into Relation_Node values\
					('"+self.Relation_ID+"','"+element+"')")
			for element in self.Relation_Ways:
				self.cur.execute("insert into Relation_Way values\
					('"+self.Relation_ID+"','"+element+"')")

			# for relation content
			if self.has_tag:
				self.content += "</relation>"
			else:
				self.content = self.content[0:len(self.content)-2] + "/>"
			self.cur.execute("insert into Relation_Info values\
				('"+self.Relation_ID+"','"+self.content+"')")

			print "relation: " + self.Relation_ID
			self.clear_relations()

if __name__ == "__main__":

	conn = MySQLdb.connect(
		host = 'localhost',
		port = 3306,
		user = 'root',
		passwd = 'abcd112358',
		db = 'MapData',
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
	cur.execute("create table Node_Info(\
		Node_ID varchar(20), \
		Info text,\
		primary key (Node_ID))")
	cur.execute("create table Way_Info(\
		Way_ID varchar(20), \
		Info text,\
		primary key (Way_ID))")
	cur.execute("create table Relation_Info(\
		Relation_ID varchar(20), \
		Info text,\
		primary key (Relation_ID))")
	
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

	cur = conn.cursor()
	cur.execute("ALTER TABLE `node` ADD `range` INT NOT NULL")
	cur.execute("UPDATE node n SET n.range=FLOOR((n.Node_Lon-119.5)*20)+FLOOR((n.Node_Lat-30.5)*2000)")
	cur.execute("ALTER TABLE way ADD `type` BOOLEAN DEFAULT 0")
	cur.execute("UPDATE way w SET w.type= 1 WHERE w.Way_Name LIKE '%路' OR w.Way_Name LIKE '%道' OR w.Way_Name LIKE '%line'")
	conn.close()