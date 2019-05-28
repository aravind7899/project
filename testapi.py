from flask import Flask,request,Response,jsonify
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
	st={"result":"success","data": ks}
	return jsonify(st)
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
	st={"result":"success","data": ks}
	return jsonify(st)
@app.route('/<keyspace>/<table_name>/primary_key')
def getprkeys(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	st={"result":"success","data":{"Partition key":cs.getPartitionkey(t),"Clustering keys":cs.getClusteringkeys(t)}}
	return jsonify(st)
@app.route('/<keyspace>/<table_name>/<column>/create_index')
def createindex(keyspace,table_name,column):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	c=str(column)
	st={"result":"success","message":cs.createIndex(t,c)}
	return jsonify(st)
@app.route('/<keyspace>/<table_name>/<column>/drop_index')
def dropindex(keyspace,table_name,column):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	c=str(column)
	st={"result":"success","message":cs.dropIndex(t,c)}
	return jsonify(st)
@app.route('/<keyspace>/<table_name>/')
def Columns(keyspace,table_name):
	ks=str(keyspace)
	t=str(table_name)
	l=cs.getColumns(ks,t)
	st={"result":"success","data": l}
	return jsonify(st)
@app.route('/<keyspace>/<table_name>/drop')
def droptable(keyspace,table_name):
	cs.useKeyspace(str(keyspace))
	st={"result":"success","message":cs.dropTable(str(table_name))}
	return jsonify(st)
@app.route('/<keyspace>/<table_name>/truncate',methods=['DELETE'])
def truncate(keyspace,table_name):
	cs.useKeyspace(str(keyspace))
	js=cs.displayDataJSON(str(table_name))
	st={"result":"success","data":json.loads(js),"message":cs.truncateTable(str(table_name))}
	return jsonify(st)
@app.route('/<keyspace>/<table_name>/update',methods=['PUT'])
def updateData(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	d=dict(urlparse.parse_qsl(request.query_string))
	data=json.loads(str(request.data))
	js=cs.updateData(t,d,data)
	if js=="Updated successfully!!":
		jst={"result":"success","data":json.loads(cs.displayDataJSON(t)),"message":js}
		return jsonify(jst)
	else:
		jst={"result":"failure","message":js}
		return jsonify(jst)
@app.route('/<keyspace>/<table_name>/display_all',methods=['GET'])
def getAllDetails(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	js=cs.displayDataJSON(t)
	if cs.getRowCount(t)==0:
		jst={"result":"success","data":[]}
		return jsonify(jst)
	else:
		jst={"result":"success","data":json.loads(js)}
		return jsonify(jst)
@app.route('/<keyspace>/<table_name>/display/',methods=['GET'])
def getwithcondition(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	d=dict(urlparse.parse_qsl(request.query_string))
	js=cs.displayDataJSON(t,d)
	if cs.getRowCount(t,d)==0:
		jst={"result":"success","data":[]}
		return jsonify(jst)
	else:
		jst={"result":"success","data":json.loads(js)}
		return jsonify(jst)
@app.route('/<keyspace>/<table_name>/insert/<filename>')
def insertFileData(keyspace,table_name,filename):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	f=str(filename)
	st=cs.insertData(t,f)
	resp=Response(st,status=200)
	return resp
@app.route('/<keyspace>/<table_name>/insert',methods=['GET','POST'])
def insertData(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	data=json.loads(str(request.data))
	js=cs.insertData(t,data)
	if js=="Inserted data successfully!!":
		st={"result":"success","message":js}
		return jsonify(st)
	else:
		st={"result":"failure","message":js}
		return jsonify(st)
@app.route('/<keyspace>/<table_name>/delete',methods=['DELETE'])
def deleteData(keyspace,table_name):
	cs.useKeyspace(keyspace)
	t=str(table_name)
	d=dict(urlparse.parse_qsl(request.query_string))
	j=cs.displayDataJSON(t,d)
	js=cs.deleteData(t,d)
	if js=="Deleted successfully!!":
		jst={"result":"success","data":json.loads(j),"message":js}
		return jsonify(jst)
	else:
		jst={"result":"failure","message":js}
		return jsonify(jst)
if __name__ == '__main__':
	app.run(host="172.28.84.65",port=8081)
