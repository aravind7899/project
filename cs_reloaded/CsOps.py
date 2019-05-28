from cassandra.cluster import Cluster
from pythonlangutil.overload import Overload,signature
import json
import ast
class CsOps:
  def __init__(self):
    self.d1=[u'system_schema',u'system_auth',u'system',u'system_distributed',u'system_traces']
    self.l_str=['ascii','date','inet','text','varchar','timestamp','time']
  @Overload
  @signature("list")
  def connect(self,ipaddress):
    self.cl=Cluster(ipaddress)
    self.s=self.cl.connect()
    return
  @connect.overload
  @signature()
  def connect(self):
    self.connect(['127.0.0.1'])
    return
  def getContent(self,filename):
    f=str(filename)
    data=open(f,"r").read()
    data=data.replace("true","True")
    data=data.replace("false","False")
    data=data.replace("null","None")
    file=open(f,"w")
    file.write(data)
    file=open(f,"w")
    file1=open(f,"r").read()
    return ast.literal_eval(file1)
  def getRContent(self,filename):
    f=str(filename)
    data=open(f,"r").read()
    data=data.replace("True","true")
    data=data.replace("False","false")
    data=data.replace("None","null")
    file=open(f,"w")
    file.write(data)
    file=open(f,"w")
    return
  def createKeyspace(self,keyspace,replication_strategy,replication_factor):
    try:
      if replication_strategy=="SimpleStrategy" or replication_strategy=="OldNetworkTopologyStrategy":
        self.s.execute("CREATE keyspace "+keyspace+" WITH replication={'class':"+"'"+replication_strategy+"','replication_factor':"+str(replication_factor)+"}")
        return "Keyspace created successfully!!"
      if replication_strategy=="NetworkTopologyStrategy":
        self.s.execute("CREATE keyspace "+keyspace+" WITH replication={'class':"+"'"+replication_strategy+"','datacentre1':"+str(replication_factor)+"}")
        return "Keyspace created successfully!!"
    except Exception as e:
      return str(e)
  def useKeyspace(self,keyspace):
    try:
      self.s.set_keyspace(keyspace)
      self.ks=keyspace
      return
    except Exception as e:
      print e
  def showKeyspaces(self):
    try:
      d2=list(self.cl.metadata.keyspaces.keys())
      return [e for e in d2 if e not in self.d1]
    except Exception as e:
      return list(e)
  def showTables(self,keyspace):
    return self.cl.metadata.keyspaces[keyspace].tables.keys()
  def alterKeyspace(self,keyspace,replication_strategy,replication_factor):
    try:
      if replication_strategy=="SimpleStrategy" or replication_strategy=="OldNetworkTopologyStrategy":
        self.s.execute("alter keyspace "+keyspace+"WITH replication={'class':"+"'"+replication_strategy+"','replication_factor':"+replication_factor+"}")
      if replication_strategy=="NetworkTopologyStrategy":
        self.s.execute("alter keyspace "+keyspace+"WITH replication={'class':"+"'"+replication_strategy+"','datacentre1':"+replication_factor+"}")
      return "Keyspace altered!!"
    except Exception as e:
      return str(e)
  def dropKeyspace(self,keyspace):
    try:
      self.s.execute("drop keyspace "+keyspace)
      return "Keyspace dropped!!"
    except Exception as e:
      return str(e)
  @Overload
  @signature("str")
  def getPartitionkey(self,table_name):
    try:
      return [str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='partition_key' ALLOW FILTERING"))]
    except Exception as e:
      return list(e)
  @getPartitionkey.overload
  @signature("str","str")
  def getPartitionkey(self,keyspace,table_name):
    try:
      return [str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+keyspace+"' and table_name='"+table_name+"' and kind='partition_key' ALLOW FILTERING"))]
    except Exception as e:
      return list(e)
  @Overload
  @signature("str","str")
  def getPrimarykey(self,keyspace,table_name):
    l1=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+keyspace+"' and table_name='"+table_name+"' and kind='partition_key' ALLOW FILTERING"))]
    l2=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+keyspace+"' and table_name='"+table_name+"' and kind='clustering' ALLOW FILTERING"))]
    return l1+l2
  @getPrimarykey.overload
  @signature("str")
  def getPrimarykey(self,table_name):
    l1=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='partition_key' ALLOW FILTERING"))]
    l2=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='clustering' ALLOW FILTERING"))]
    return l1+l2
  @Overload
  @signature("str","str")
  def getColumns(self,keyspace,table_name):
    try:
      return [str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+keyspace+"' and table_name='"+table_name+"'"))]
    except Exception as e:
      return list(e)
  @getColumns.overload
  @signature("str")
  def getColumns(self,table_name):
    try:
      return [str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"'"))]
    except Exception as e:
      return list(e)
  @Overload
  @signature("str","str")
  def getClusteringkeys(self,keyspace,table_name):
    try:
      return [str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+keyspace+"' and table_name='"+table_name+"' and kind='clustering' ALLOW FILTERING"))]
    except Exception as e:
      return list(e)
  @getClusteringkeys.overload
  @signature("str")
  def getClusteringkeys(self,table_name):
    try:
      return [str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='clustering' ALLOW FILTERING"))]
    except Exception as e:
      return list(e)
  @Overload
  @signature("str","str")
  def createIndex(self,table_name,column):
    try:
      list1=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='partition_key' ALLOW FILTERING"))]
      q_c=self.s.execute("select count(*) from system_schema.indexes where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and options={'target':'"+column+"'} ALLOW FILTERING")[0][0]
      if column in list1:
        return "Index cannot be created!!"
      else:
        if q_c>1:
          return "Index already exists!!"
        else:
          self.s.execute("create index if not exists on "+table_name+" ("+column+")")
          return "Index created!!"
    except Exception as e:
      return str(e)
  @createIndex.overload
  @signature("str","str","str")
  def createIndex(self,keyspace,table_name,column):
    self.useKeyspace(keyspace)
    return self.createIndex(table_name,column)
  @Overload
  @signature("str","str")
  def dropIndex(self,table_name,column):
    try:
      index=table_name+"_"+column+"_"+"idx"
      q_c=self.s.execute("select count(*) from system_schema.indexes where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and options={'target':'"+column+"'} ALLOW FILTERING")[0][0]
      if q_c==1:
        self.s.execute("drop index "+index)
        return "Index dropped!!"
      else:
        return "Index does not exists!!"
    except Exception as e:
      return str(e)
  @dropIndex.overload
  @signature("str","str","str")
  def dropIndex(self,keyspace,table_name,column):
    self.useKeypspace(keyspace)
    return self.dropIndex(table_name,column)
  @Overload
  @signature("str","dict","list","list")
  def createTable(self,table_name,column_dt,partition_key,clustering_keys):
    try:
      query="create table "+table_name+" ("
      for key,value in column_dt.iteritems():
        query+=key+" "+value+","
      pk=len(partition_key)
      ck=len(clustering_keys)
      if pk==1:
        if ck>0:
          if ck==1 and clustering_keys[0]=='':
            query+="PRIMARY KEY ("+partition_key[0]+"))"
            self.s.execute(query)
          else:
            query+="PRIMARY KEY ("+partition_key[0]+","
            for i in xrange(ck-1):
              query+=clustering_keys[i]+","
            query+=clustering_keys[-1]+"))"
            self.s.execute(query)
        else:
          query+="PRIMARY KEY ("+partition_key[0]+"))"
          self.s.execute(query)
      else:
        if ck>0:
          if ck==1 and clustering_keys[0]=='':
            for i in xrange(pk-1):
              query+="PRIMARY KEY (("+partition_key[i]+","
            query+=partition_key[-1]+"))"
            self.s.execute(query)
          else:      
            for i in xrange(pk-1):
              query+="PRIMARY KEY (("+partition_key[i]+","
            query+=partition_key[-1]+"),"
            for j in xrange(ck-1):
              query+=clustering_keys[j]+","
            query+=clustering_keys[-1]+"))"
            self.s.execute(query)
        else:
          for i in xrange(pk-1):
            query+="PRIMARY KEY (("+partition_key[i]+","
          query+=partition_key[-1]+"))"
          self.s.execute(query)
      return "Table created!!"
    except Exception as e:
      return str(e)
  @createTable.overload
  @signature("str","str","dict","list","list")
  def createTable(self,keyspace,table_name,column_dt,partition_key,clustering_keys):
    self.useKeyspace(keyspace)
    return self.createTable(table_name,column_dt,partition_key,clustering_keys)
  @createTable.overload
  @signature("dict")
  def createTable(self,details):
    try:
      query="create table "+details["table_name"]+" ("
      c_dt=details["columns"]
      for key,value in c_dt.iteritems():
        query+=key+" "+value+","
      partition_key=details["primary_key"]["partition_key"]
      clustering_keys=details["primary_key"]["clustering_keys"]
      pk=len(partition_key)
      ck=len(clustering_keys)
      if pk==1:
        if ck>0:
          query+="PRIMARY KEY ("+partition_key[0]+","
          if ck==1 and clustering_keys[0]=='':
            query+="PRIMARY KEY ("+partition_key[0]+"))"
            self.s.execute(query)
          else:
            for i in xrange(ck-1):
              query+=clustering_keys[i]+","
            query+=clustering_keys[-1]+"))"
            self.s.execute(query)
        else:
          query+="PRIMARY KEY ("+partition_key[0]+"))"
          self.s.execute(query)
      else:
        if ck>0:
          if ck==1 and clustering_keys[0]=='':
            for i in xrange(pk-1):
              query+="PRIMARY KEY (("+partition_key[i]+","
            query+=partition_key[-1]+"))"
            self.s.execute(query)
          else:      
            for i in xrange(pk-1):
              query+="PRIMARY KEY (("+partition_key[i]+","
            query+=partition_key[-1]+"),"
            for j in xrange(ck-1):
              query+=clustering_keys[j]+","
            query+=clustering_keys[-1]+"))"
            self.s.execute(query)
        else:
          for i in xrange(pk-1):
            query+="PRIMARY KEY (("+partition_key[i]+","
          query+=partition_key[-1]+"))"
          self.s.execute(query)
      return "Table created!!"
    except Exception as e:
      return str(e)
  @createTable.overload
  @signature("str","dict")
  def createTable(self,keyspace,details):
    self.useKeyspace(keyspace)
    return self.createTable(details)
  @Overload
  @signature("str","str","str")
  def addColumn(self,table_name,column,datatype):
    try:
      self.s.execute("alter table "+table_name+" add "+column+" "+datatype)
      return "Column added to the table!!"
    except Exception as e:
      return str(e)
  @addColumn.overload
  @signature("str","str","str","str")
  def addColumn(self,keyspace,table_name,column,datatype):
    self.useKeyspace(keyspace)
    return self.addColumn(table_name,column,datatype)
  @Overload
  @signature("str","str","str")
  def renameColumn(self,table_name,old_column,new_column):
    try:
      list1=self.getPartitionkey(table_name)
      list2=self.getClusteringkeys(table_name)
      list4=list1+list2
      if old_column not in list4:
        self.s.execute("alter table "+table_name+" rename "+old_column+" to "+new_column)
        return "Column renamed from "+old_column+"to "+new_column+" !!"
      else:
        return "Column cannot be renamed!!"
    except Exception as e:
      return str(e)
  @renameColumn.overload
  @signature("str","str","str","str")
  def renameColumn(self,keyspace,table_name,old_column,new_column):
    self.useKeyspace(keyspace)
    return self.renameColumn(table_name,old_column,new_column)
  @Overload
  @signature("str","str")
  def dropColumn(self,table_name,column):
    try:
      self.s.execute("alter table "+table_name+" drop "+column)
      return "Column dropped!!"
    except Exception as e:
      return str(e)
  @dropColumn.overload
  @signature("str","str","str")
  def dropColumn(self,keyspace,table_name,column):
    self.useKeyspace(keyspace)
    return self.dropColumn(table_name,column)
  @Overload
  @signature("str","str","str")
  def changeDatatype(self,table_name,column,new_datatype):
    try:
      q_d=dict(self.s.execute("select column_name,type from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"'"))
      dt=q_d[column]
      l=[]
      l.append(column)
      if self.getRowCount(table_name,l)==0:
        self.s.execute("alter table "+table_name+" alter "+column+" type "+new_datatype)
        return "Datatype changed from "+dt+" to "+new_datatype+" !!"
      else:
        return "Datatype cannot be changed!!"
    except Exception as e:
      return str(e)
  @changeDatatype.overload
  @signature("str","str","str","str")
  def changeDatatype(self,keyspace,table_name,column,new_datatype):
    self.useKeyspace(keyspace)
    return self.changeDatatype(table_name,column,new_datatype)
  @Overload
  @signature("str")
  def dropTable(self,table_name):
    try:
      self.s.execute("drop table "+table_name)
      return "Table dropped!!"
    except Exception as e:
      return str(e)
  @dropTable.overload
  @signature("str","str")
  def dropTable(self,keyspace,table_name):
    self.useKeyspace(keyspace)
    return self.dropTable(table_name)
  @Overload
  @signature("str")
  def truncateTable(self,table_name):
    try:
      self.s.execute("truncate "+table_name)
      return "Table truncated!!"
    except Exception as e:
      return str(e)
  @truncateTable.overload
  @signature("str","str")
  def truncateTable(self,keyspace,table_name):
    self.useKeyspace(keyspace)
    return self.truncateTable(table_name)
  @Overload
  @signature("str","dict")
  def insertData(self,table_name,column_value):
    try:
      q1=self.s.execute("select count(*) from "+table_name)
      query=self.s.execute("INSERT INTO "+table_name+" JSON '"+json.dumps(column_value)+"'")
      q2=self.s.execute("select count(*) from "+table_name)
      if q2[0][0]-q1[0][0]==0:
        return "Already inserted!!"
      else:
        return "Inserted data successfully!!"
    except Exception as e:
      return str(e)
  @insertData.overload
  @signature("str","str","dict")
  def insertData(self,keyspace,table_name,column_value):
    self.useKeyspace(keyspace)
    return self.insertData(table_name,column_value)
  @insertData.overload
  @signature("str","str")
  def insertData(self,table_name,filename):
    filename=str(filename)
    g=self.getContent(filename)
    if type(g) is dict:
      js=self.insertData(table_name,g)
      self.getRContent(filename)
      return js
    else:
      j,j1,j2=0,0,0
      for i in g:
        st=self.insertData(table_name,i)
        if st=="Inserted data successfully!!":
          j+=1
        elif st=="Already inserted!!":
          j1+=1
        else:
          j2+=1  
      if j+j1==len(g):
        if j1==0:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!!"
        elif j==0 and j1==1:
          js= str(j1)+" row already inserted!!"
        elif j==0 and j1>1:
          js=str(j1)+" rows already inserted!!"
        elif j1==1 and j!=0:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!! "+str(j1)+" row already inserted!!"
        else:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!! "+str(j1)+" rows already inserted!!"
      if j+j1+j2==len(g) and j2>0:
        if j1==0 and j!=0 and j2==1:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!! "+str(j2)+" row not inserted!!"
        elif j1==0 and j!=0 and j2>1:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!! "+str(j2)+" rows not inserted!!" 
        elif j==0 and j1==1 and j2==1:
          js= str(j1)+" rows already inserted!! "+str(j2)+" row not inserted!!"
        elif j==0 and j1==1 and j2>1:
          js= str(j1)+" rows already inserted!! "+str(j2)+" rows not inserted!!"
        elif j==0 and j1>1 and j2==1:
          js=str(j1)+" rows already inserted!! "+str(j2)+" row not inserted!!"
        elif j==0 and j1>1 and j2>1:
          js=str(j1)+" rows already inserted!! "+str(j2)+" row not inserted!!"
        elif j1==1 and j!=0 and j2==1:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!! "+str(j1)+" row already inserted!!"+str(j2)+" row not inserted!!"
        elif j1==1 and j!=0 and j2>1:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!! "+str(j1)+" row already inserted!!"+str(j2)+" row not inserted!!"
        elif j1>1 and j!=0 and j2==1:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!! "+str(j1)+" rows already inserted!!"+str(j2)+" row not inserted!!"
        elif j1>1 and j!=0 and j2>1:
          js=str(j)+" of "+str(len(g))+" rows inserted successfully!! "+str(j1)+" rows already inserted!!"+str(j2)+" rows not inserted!!"
        else:
          if j2==1:
            js=str(j2)+" row not inserted!!"
          else:
            js=str(j2)+" rows not inserted!!"
      self.getRContent(filename)
      return js
  @insertData.overload
  @signature("str","str","str")
  def insertData(self,keyspace,table_name,filename):
    self.useKeyspace(keyspace)
    return self.insertData(table_name,filename)
  @Overload
  @signature("str")
  def getRowCount(self,table_name):
    return self.s.execute("select count(*) from  "+table_name)[0][0]
  @getRowCount.overload
  @signature("str","str")
  def getRowCount(self,keyspace,table_name):
    self.useKeyspace(keyspace)
    return self.getRowCount(table_name)
  @getRowCount.overload
  @signature("str","dict")
  def getRowCount(self,table_name,condition):
    query="select count(*) from "+table_name+" where "+self.display_condition(table_name,condition)
    return self.s.execute(query)[0][0]
  @getRowCount.overload
  @signature("str","str","dict")
  def getRowCount(self,keyspace,table_name,condition):
    self.useKeyspace(keyspace)
    return self.getRowCount(table_name,condition)
  @getRowCount.overload
  @signature("str","list")
  def getRowCount(self,table_name,column):
    query="select count("+str(column[0])+") from "+table_name
    return self.s.execute(query)[0][0]
  @getRowCount.overload
  @signature("str","str","list")
  def getRowCount(self,keyspace,table_name,column):
    self.useKeyspace(keyspace)
    return self.getRowCount(table_name,column)
  @getRowCount.overload
  @signature("str","list","dict")
  def getRowCount(self,table_name,column,condition):
    query="select count("+str(column[0])+") from "+table_name+" where "+self.display_condition(table_name,condition)
    return self.s.execute(query)[0][0]
  @getRowCount.overload
  @signature("str","str","str","dict")
  def getRowCount(self,keyspace,table_name,column,condition):
    self.useKeyspace(keyspace)
    return self.getRowCount(table_name,column,condition)
  @Overload
  @signature("str")
  def displayData(self,table_name):
    try:
      l1=[]
      q=self.s.execute("select * from "+table_name)
      for i in q:
        l1.append(list(i))
      return l1
    except Exception as e:
      return list(e)
  @displayData.overload
  @signature("str","str")
  def displayData(self,keyspace,table_name):
    self.useKeyspace(keyspace)
    return self.displayData(table_name)
  @displayData.overload
  @signature("str","list")
  def displayData(self,table_name,columns):
    try:
      l1=[]
      query="select "
      for i in columns[:-1]:
        query+=i+","
      query+=columns[-1]+" from "+table_name
      for i in self.s.execute(query):
        l1.append(list(i))
      return l1
    except Exception as e:
      return list(e)
  @displayData.overload
  @signature("str","str","list")
  def displayData(self,keyspace,table_name,columns):
    self.useKeyspace(keyspace)
    return self.displayData(table_name,columns)
  def display_condition(self,table_name,condition):
    q_d=dict(self.s.execute("select column_name,type from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"'"))
    list1=self.getPartitionkey(table_name)
    list2=self.getClusteringkeys(table_name)
    list3=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='regular' ALLOW FILTERING"))]
    list4=list1+list2
    l=list(condition.keys())
    query=""
    if set(list4)==set(l):
        if len(l)==1:
          if str(q_d[l[0]]) in self.l_str:
            query+=l[0]+"='"+str(condition[l[0]])+"'"
          else:
            query+=l[0]+"="+str(condition[l[0]])
        else:
          for i in l[:-1]:
            if str(q_d[i]) in self.l_str:
              query+=i+"='"+str(condition[i])+"' and "
            else:
              query+=i+"="+str(condition[i])+" and "
          if str(q_d[l[-1]]) in self.l_str:
            query+=l[-1]+"='"+str(condition[l[-1]])+"'"
          else:
            query+=l[-1]+"="+str(condition[l[-1]])
        query1=self.s.execute(query)
    elif set(list1)==set(l) and len(list2)==1:
      if len(l)==1:
        if str(q_d[l[0]]) in self.l_str:
          query+=l[0]+"='"+str(condition[l[0]])+"'"
        else:
          query+=l[0]+"="+str(condition[l[0]])
      else:
        for i in l[:-1]:
          if str(q_d[i]) in self.l_str:
            query+=i+"='"+str(condition[i])+"' and "
          else:
            query+=i+"="+str(condition[i])+" and "
        if str(q_d[l[-1]]) in self.l_str:
          query+=l[-1]+"='"+str(condition[l[-1]])+"' order by "+list2[0]
        else:
          query+=l[-1]+"="+str(condition[l[-1]])+" order by "+list2[0]
    else:
      if len(l)==1:
        if str(q_d[l[0]]) in self.l_str:
          query+=l[0]+"='"+str(condition[l[0]])+"' ALLOW FILTERING"
        else:
          query+=l[0]+"="+str(condition[l[0]])+" ALLOW FILTERING"
      else:
        for i in l[:-1]:
          if str(q_d[i]) in self.l_str:
            query+=i+"='"+str(condition[i])+"' and "
          else:
            query+=i+"="+str(condition[i])+" and "
        if str(q_d[l[-1]]) in self.l_str:
          query+=l[-1]+"='"+str(condition[l[-1]])+"' ALLOW FILTERING"
        else:
          query+=l[-1]+"="+str(condition[l[-1]])+" ALLOW FILTERING"
    return query
  @displayData.overload
  @signature("str","dict")
  def displayData(self,table_name,condition):
    try:
      l1=[]
      query="select * from "+table_name+" where "+self.display_condition(table_name,condition)
      query1=self.s.execute(query)
      for i in query1:
        l1.append(list(i))
      return l1
    except Exception as e:
      return list(e)
  @displayData.overload
  @signature("str","str","dict")
  def displayData(self,keyspace,table_name,condition):
    self.useKeyspace(keyspace)
    return self.displayData(table_name,condition)
  @displayData.overload
  @signature("str","list","dict")
  def displayData(self,table_name,columns,condition):
    try:
      l1=[]
      query="select "
      for i in columns[:-1]:
        query+=i+","
      query+=columns[-1]+" from "+table_name+" where "+self.display_condition(table_name,condition)
      query1=self.s.execute(query)
      for i in query1:
        l1.append(list(i))
      return l1
    except Exception as e:
      return list(e)
  @displayData.overload
  @signature("str","str","list","dict")
  def displayData(self,keyspace,table_name,columns,condition):
    self.useKeyspace(keyspace)
    return self.displayData(table_name,columns,condition)
  @Overload
  @signature("str")
  def displayDataJSON(self,table_name):
    try:
      l1=[]
      query=self.s.execute("select JSON * from "+table_name)
      for i in query:
        l1.append(str(''.join(i)))
      q = self.getRowCount(table_name)
      if q==1:
        js=','.join(j for j in l1)
      elif q>1:
        js="["+','.join(j for j in l1)+"]"
      else:
        js=""
      return js
    except Exception as e:
      return str(e)
  @displayDataJSON.overload
  @signature("str","str")
  def displayDataJSON(self,keyspace,table_name):
    self.useKeyspace(keyspace)
    return self.displayDataJSON(table_name)
  @displayDataJSON.overload
  @signature("str","list")
  def displayDataJSON(self,table_name,columns):
    try:
      l1=[]
      query="select JSON "
      for i in columns[:-1]:
        query+=i+","
      query+=columns[-1]+" from "+table_name
      for i in self.s.execute(query):
        l1.append(str(''.join(i)))
      q=self.getRowCount(table_name)
      if q==1:
        js=','.join(j for j in l1)
      elif q>1:
        js="["+','.join(j for j in l1)+"]"
      else:
        js=""
      return js
    except Exception as e:
      return str(e)
  @displayDataJSON.overload
  @signature("str","str","list")
  def displayDataJSON(self,keyspace,table_name,columns):
    self.useKeyspace(keyspace)
    return self.displayDataJSON(table_name,columns)
  @displayDataJSON.overload
  @signature("str","dict")
  def displayDataJSON(self,table_name,condition):
    try:
      l1=[]
      query="select JSON * from "+table_name+" where "+self.display_condition(table_name,condition)
      query1=self.s.execute(query)
      for i in query1:
        l1.append(str(''.join(i)))
      q=self.getRowCount(table_name,condition)
      if q==1:
        js=','.join(j for j in l1)
      elif q>1:
        js="["+','.join(j for j in l1)+"]"
      else:
        js=""
      return js
    except Exception as e:
      return str(e)
  @displayDataJSON.overload
  @signature("str","str","dict")
  def displayDataJSON(self,keyspace,table_name,condition):
    self.useKeyspace(keyspace)
    return self.displayDataJSON(table_name,condition)
  @displayDataJSON.overload
  @signature("str","list","dict")
  def displayDataJSON(self,table_name,columns,condition):
    try:
      l1=[]
      query="select JSON "
      for i in columns[:-1]:
        query+=i+","
      query+=columns[-1]+" from "+table_name+" where "+self.display_condition(table_name,condition)
      query1=self.s.execute(query)
      for i in query1:
        l1.append(str(''.join(i)))
      q=self.getRowCount(table_name,condition)
      if q==1:
        js=','.join(j for j in l1)
      elif q>1:
        js="["+','.join(j for j in l1)+"]"
      else:
        js=""
      return js
    except Exception as e:
      return str(e)
  @displayDataJSON.overload
  @signature("str","str","list","dict")
  def displayDataJSON(self,keyspace,table_name,columns,condition):
    self.useKeyspace(keyspace)
    return self.displayDataJSON(table_name,columns,condition)
  @Overload
  @signature("str","dict","dict")
  def updateData(self,table_name,column_value,condition):
    try:
      q_d=dict(self.s.execute("select column_name,type from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"'"))
      list1=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='partition_key' ALLOW FILTERING"))]
      list2=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='clustering' ALLOW FILTERING"))]
      list3=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='regular' ALLOW FILTERING"))]
      list4=list1+list2
      l2=[]
      l1=list(column_value.keys())
      l=list(condition.keys())
      query="update "+table_name+" set "
      if l1 not in list4:
        if len(l1)==1:
          if str(q_d[l1[0]]) in self.l_str:
            query+=l1[0]+"='"+str(column_value[l1[0]])+"' where "
          else:
            query+=l1[0]+"="+str(column_value[l1[0]])+" where "
        else:
          for i in l1[:-1]:
            if str(q_d[i]) in self.l_str:
              query+=i+"='"+str(column_value[i])+"',"
            else:
              query+=i+"="+str(column_value[i])+","
          if str(q_d[l1[-1]]) in self.l_str:
            query+=l1[-1]+"='"+str(column_value[l1[-1]])+"' where "
          else:
            query+=l1[-1]+"="+str(column_value[l1[-1]])+" where "
      else:
        return "Update not poosible!!Primary key found in SET!"
      if set(l)==set(list4):
        if len(l)==1:
          if str(q_d[l[0]]) in self.l_str:
            query+=l[0]+"='"+condition[l[0]]+"'"
          else:
            query+=l[0]+"="+str(condition[l[0]])
        else:
          for i in l[:-1]:
            if str(q_d[i]) in self.l_str:
              query+=i+"='"+condition[i]+"' and "
            else:
              query+=i+"="+str(condition[i])+" and "
          if str(q_d[l[-1]]) in self.l_str:
            query+=str(l[-1])+" = '"+str(condition[l[-1]])+"'"
          else:
            query+=str(l[-1])+" = "+str(condition[l[-1]])
          q=self.s.execute(query)
          return "Updated successfully!!"
      else:
        return "Update not poosible!!"
    except Exception as e:
      return str(e)
  @updateData.overload
  @signature("str","str","dict","dict")
  def updateData(self,keyspace,table_name,column_value,condition):
    self.useKeyspace(keyspace)
    return self.updateData(table_name,column_value,condition)
  @Overload
  @signature("str","dict")
  def deleteData(self,table_name,condition):
    try:
      q_d=dict(self.s.execute("select column_name,type from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"'"))
      list1=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='partition_key' ALLOW FILTERING"))]
      list2=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='clustering' ALLOW FILTERING"))]
      list3=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='regular' ALLOW FILTERING"))]
      list4=list1+list2
      l=list(condition.keys())
      query="delete from "+table_name+" where "
      q="select count(*) from "+table_name+" where "
      if set(list4)==set(l):
        if len(l)==1:
          if str(q_d[l[0]]) in self.l_str:
            query+=l[0]+"='"+str(condition[l[0]])+"'"
            q+=l[0]+"='"+str(condition[l[0]])+"'"
          else:
            query+=l[0]+"="+str(condition[l[0]])
            q+=l[0]+"="+str(condition[l[0]])
        else:
          for i in l[:-1]:
            if str(q_d[i]) in self.l_str:
              query+=i+"='"+str(condition[i])+"' and "
              q+=i+"='"+str(condition[i])+"' and "
            else:
              query+=i+"="+str(condition[i])+" and "
              q+=i+"="+str(condition[i])+" and "
          if str(q_d[l[-1]]) in self.l_str:
            query+=l[-1]+"='"+str(condition[l[-1]])+"'"
            q+=l[-1]+"='"+str(condition[l[-1]])+"'"
          else:
            query+=l[-1]+"="+str(condition[l[-1]])
            q+=l[-1]+"="+str(condition[l[-1]])
        q1 = self.s.execute(q)
        query1=self.s.execute(query)
      elif set(list1)==set(l):
        if len(l)==1:
          if str(q_d[l[0]]) in self.l_str:
            query+=l[0]+"='"+str(condition[l[0]])+"'"
            q+=l[0]+"='"+str(condition[l[0]])+"'"
          else:
            query+=l[0]+"="+str(condition[l[0]])
            q+=l[0]+"="+str(condition[l[0]])
        else:
          for i in l[:-1]:
            if str(q_d[i]) in self.l_str:
              query+=i+"='"+str(condition[i])+"' and "
              q+=i+"='"+str(condition[i])+"' and "
            else:
              query+=i+"="+str(condition[i])+" and "
              q+=i+"="+str(condition[i])+" and "
          if str(q_d[l[-1]]) in self.l_str:
            query+=l[-1]+"='"+str(condition[l[-1]])+"'"
            q+=l[-1]+"='"+str(condition[l[-1]])+"' ALLOW FILTERING"
          else:
            query+=l[-1]+"="+str(condition[l[-1]])
            q+=l[-1]+"="+str(condition[l[-1]])+" ALLOW FILTERING"
        q1 = self.s.execute(q)
        query1=self.s.execute(query)
      elif set(list1).intersection(l)!=set():
        return "Deletion not possible without all partition key columns!!"
      else:
        return "Deletion not possible!!"
      if q1[0][0]>0:
        return "Deleted successfully!!"
      else:
        return "Row does not exists!!"
    except Exception as e:
      return str(e)
  @deleteData.overload
  @signature("str","str","dict")
  def deleteData(self,keyspace,table_name,condition):
    self.useKeyspace(keyspace)
    return self.deleteData(table_name,condition)
  @deleteData.overload
  @signature("str","list","dict")
  def deleteData(self,table_name,columns,condition):
    try:
      q_d=dict(self.s.execute("select column_name,type from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"'"))
      list1=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='partition_key' ALLOW FILTERING"))]
      list2=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='clustering' ALLOW FILTERING"))]
      list3=[str(j[0]) for j in list(self.s.execute("select column_name from system_schema.columns where keyspace_name='"+self.ks+"' and table_name='"+table_name+"' and kind='regular' ALLOW FILTERING"))]
      list4=list1+list2
      l=list(condition.keys())
      query="delete "
      if set(columns)==set(list3):
        if len(columns)==1:
          query+=columns[-1]+" from "+table_name+" where "
        else:
          for i in columns[:-1]:
            query+=i+","
          query+=columns[-1]+" from "+table_name+" where "
      q="select count(*) from "+table_name+" where "
      if set(list4)==set(l):
        if len(l)==1:
          if str(q_d[l[0]]) in self.l_str:
            query+=l[0]+"='"+str(condition[l[0]])+"'"
            q+=l[0]+"='"+str(condition[l[0]])+"'"
          else:
            query+=l[0]+"="+str(condition[l[0]])
            q+=l[0]+"="+str(condition[l[0]])
        else:
          for i in l[:-1]:
            if str(q_d[i]) in self.l_str:
              query+=i+"='"+str(condition[i])+"' and "
              q+=i+"='"+str(condition[i])+"' and "
            else:
              query+=i+"="+str(condition[i])+" and "
              q+=i+"="+str(condition[i])+" and "
          if str(q_d[l[-1]]) in self.l_str:
            query+=l[-1]+"='"+str(condition[l[-1]])+"'"
            q+=l[-1]+"='"+str(condition[l[-1]])+"'"
          else:
            query+=l[-1]+"="+str(condition[l[-1]])
            q+=l[-1]+"="+str(condition[l[-1]])
        q1 = self.s.execute(q)
        query1=self.s.execute(query)
      elif set(list1)==set(l):
        if len(l)==1:
          if str(q_d[l1[0]]) in self.l_str:
            query+=l[0]+"='"+str(condition[l[0]])+"'"
            q+=l[0]+"='"+str(condition[l[0]])+"'"
          else:
            query+=l[0]+"="+str(condition[l[0]])
            q+=l[0]+"="+str(condition[l[0]])
        else:
          for i in l[:-1]:
            if str(q_d[i]) in self.l_str:
              query+=i+"='"+str(condition[i])+"' and "
              q+=i+"='"+str(condition[i])+"' and "
            else:
              query+=i+"="+str(condition[i])+" and "
              q+=i+"="+str(condition[i])+" and "
          if str(q_d[l[-1]]) in self.l_str:
            query+=l[-1]+"='"+str(condition[l[-1]])+"'"
            q+=l[-1]+"='"+str(condition[l[-1]])+"' ALLOW FILTERING"
          else:
            query+=l[-1]+"="+str(condition[l[-1]])
            q+=l[-1]+"="+str(condition[l[-1]])+" ALLOW FILTERING"
        q1 = self.s.execute(q)
        query1=self.s.execute(query)
      elif set(list1).intersection(l)!=set():
        return "Deletion not possible without all partition key columns!!"
      else:
        return "Deletion not possible!!"
      if q1[0][0]>0:
        return "Deleted successfully!!"
      else:
        return "Row does not exists!!"
    except Exception as e:
      return str(e)
  @deleteData.overload
  @signature("str","str","list","dict")
  def deleteData(self,keyspace,table_name,columns,condition):
    self.useKeyspace(keyspace)
    return self.deleteData(table_name,columns,condition)
