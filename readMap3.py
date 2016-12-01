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
		self.has_tag = False
		self.content = ''
		self.ID = ''

	def startElement(self, tag, attributes):
		if tag == 'node':
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
			'''
			for index in range(len(attributes.keys())):
				self.content += attributes.keys()[index]
				self.content += "=\""
				self.content += attributes.values()[index]
				self.content += "\" "
			'''
			self.content += "\">\n" 
			self.Current_Tag = 'node'
			self.ID = attributes["id"]
	
		if tag == 'way':
			self.content += "<way id=\""
			self.content += attributes["id"]
			self.content += "\" version=\""
			self.content += attributes["version"]
			self.content += "\" timestamp=\""
			self.content += attributes["timestamp"]
			self.content += "\" changeset=\""
			self.content += attributes["changeset"]
			'''
			for index in range(len(attributes.keys())):
				self.content += attributes.keys()[index]
				self.content += "=\""
				self.content += attributes.values()[index]
				self.content += "\" "
			'''
			self.content += "\">\n" 
			self.Current_Tag = 'way'
			self.ID = attributes["id"]

		if tag == 'relation':
			self.content += "<relation id=\""
			self.content += attributes["id"]
			self.content += "\" version=\""
			self.content += attributes["version"]
			self.content += "\" timestamp=\""
			self.content += attributes["timestamp"]
			self.content += "\" changeset=\""
			self.content += attributes["changeset"]
			'''
			for index in range(len(attributes.keys())):
				self.content += attributes.keys()[index]
				self.content += "=\""
				self.content += attributes.values()[index]
				self.content += "\" "
			'''
			self.content += "\">\n" 
			self.Current_Tag = 'relation'
			self.ID = attributes["id"]

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
			if self.has_tag:
				self.content += "</node>"
			else:
				self.content = self.content[0:len(self.content)-2] + "/>"
			self.cur.execute("insert into Node_Info values\
				('"+self.ID+"','"+self.content+"')")
			print "node: " + self.ID
			self.content = ''
			self.has_tag = False
			self.ID = ''

		if tag == "way":
			if self.has_tag:
				self.content += "</way>"
			else:
				self.content = self.content[0:len(self.content)-2] + "/>"
			self.cur.execute("insert into Way_Info values\
				('"+self.ID+"','"+self.content+"')")
			print "way: " + self.ID
			self.content = ''
			self.has_tag = False
			self.ID = ''

		if tag == "relation":
			if self.has_tag:
				self.content += "</relation>"
			else:
				self.content = self.content[0:len(self.content)-2] + "/>"
			print "relation: " + self.ID
			self.cur.execute("insert into Relation_Info values\
				('"+self.ID+"','"+self.content+"')")
			self.content = ''
			self.has_tag = False
			self.ID = ''

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
	conn.close()