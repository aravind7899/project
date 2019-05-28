from flask import Flask,request,Response
from flask_cors import CORS
import json
import urlparse
from cs_reloaded import CsOps
cs=CsOps()
cs.connect(['192.168.21.114'])
app= Flask(__name__)
CORS(app)
def convertRow(Row):
	string =  ''.join(Row)
	return str(string)
@app.route('/keyspaces')
def getAllKeyspaces():
	ks=cs.showKeyspaces()
	st='{"result":"success","data":['+','.join(str(i) for i in ks)+"]}"
	resp = Response(st, status=200)
	return resp
@app.route('/<keyspace>/create_table',methods=['GET','POST'])
def createtable(keyspace):
	cs.useKeyspace(keyspace)
	data=json.loads(str(request.data))
	st=cs.createTable(data)
	resp = Response(st, status=200)
	return resp
@app.route('/<keyspace>/tables')
def getAllTables(keyspace):
	ks=cs.showTables(keyspace)
	st='{"result":"success","data":['+','.join(str(i) for i in ks)+"]}"
	resp = Response(st, status=200)
	return resp
@app.route('/<keyspace>/<table_name>/primary_key')
def getprkeys(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	st='{"result":"success","data":{"Partition key":'+','.join(j for j in cs.getPartitionkey(t))+'],"Clustering keys":['+','.join(j for j in cs.getClusteringkeys(t))+"]}}"
	resp=Response(st,status=200)
	return resp
@app.route('/<keyspace>/<table_name>/<column>/create_index')
def createindex(keyspace,table_name,column):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	c=str(column)
	st=cs.createIndex(t,c)
	resp=Response(st,status=200)
	return resp
@app.route('/<keyspace>/<table_name>/<column>/drop_index')
def dropindex(keyspace,table_name,column):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	c=str(column)
	st=cs.dropIndex(t,c)
	resp=Response(st,status=200)
	return resp
@app.route('/<keyspace>/<table_name>/')
def Columns(keyspace,table_name):
	ks=str(keyspace)
	t=str(table_name)
	l=cs.getColumns(ks,t)
	st='</br>'.join(j for j in l)
	resp=Response(st,status=200)
	return resp
@app.route('/<keyspace>/<table_name>/drop')
def droptable(keyspace,table_name):
	cs.useKeyspace(str(keyspace))
	st=cs.dropTable(str(table_name))
	resp=Response(st,status=200)
	return resp
@app.route('/<keyspace>/<table_name>/truncate',methods=['DELETE'])
def truncate(keyspace,table_name):
	cs.useKeyspace(str(keyspace))
	st=cs.truncateTable(str(keyspace),str(table_name))
	js=cs.displayDataJSON(str(table_name))
	resp=Response(js,status=200)
	return resp
@app.route('/<keyspace>/<table_name>/update',methods=['PUT'])
def updateData(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	d=dict(urlparse.parse_qsl(request.query_string))
	data=json.loads(str(request.data))
	js=cs.updateData(t,d,data)
	if js=="Updated successfully!!":
		jst='{"result":"success","data":'+cs.displayDataJSON(t)+',"message":'+js+"}"
		resp = Response(jst, status=200)
		return resp
	else:
		jst='{"result":"failure","message":"'+js+'"}'
		resp = Response(jst, status=200)
		return resp
@app.route('/<keyspace>/<table_name>/display_all',methods=['GET'])
def getAllDetails(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	js=cs.displayDataJSON(t)
	if cs.getRowCount(t)==0:
		jst='{"result":"success","data":[]}'
		resp = Response(jst, status=200)
		return resp
	else:
		jst='{"result":"success","data":'+js+"}"
		resp = Response(jst, status=200)
		return resp
@app.route('/<keyspace>/<table_name>/display/',methods=['GET'])
def getwithcondition(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	d=dict(urlparse.parse_qsl(request.query_string))
	js=cs.displayDataJSON(t,d)
	if cs.getRowCount(t,d)==0:
		jst='{"result":"success","data":[]}'
		resp = Response(jst, status=200)
		return resp
	else:
		jst='{"result":"success","data":'+js+"}"
		resp = Response(jst, status=200)
		return resp
@app.route('/<keyspace>/<table_name>/insert/<filename>')
def insertFileData(keyspace,table_name,filename):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	f=str(filename)
	js=cs.insertData(t,f)
	resp = Response(js, status=200)
	return resp
@app.route('/<keyspace>/<table_name>/insert',methods=['GET','POST'])
def insertData(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	data=json.loads(str(request.data))
	js=cs.insertData(t,data)
	resp = Response(js, status=200)
	return resp
@app.route('/<keyspace>/<table_name>/delete',methods=['DELETE'])
def deleteData(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	d=dict(urlparse.parse_qsl(request.query_string))
	j=cs.displayDataJSON(t,d)
	js=cs.deleteData(t,d)
	if js=="Deleted successfully!!":
		jst='{"result":"success","data":'+j+',"message":"'+js+'"}'
		resp = Response(jst, status=200)
		return resp
	else:
		jst='{"result":"failure","message":"'+js+'"}'
		resp = Response(jst, status=200)
		return resp
if __name__ == '__main__':
	app.run(host="172.28.84.65",port=8081)
