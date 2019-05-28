from cs_reloaded import CsOps
import sys
cs=CsOps()
ipaddress=raw_input("Enter ip address to connect:")
l=[]
l.append(ipaddress)
cs.connect(l)
while(True):
  d=cs.showKeyspaces()
  if len(d)>0:
    print "the keyspaces that are already created are:\n**********"
    for i in d:
      print i
    s=raw_input("\n**********\nEnter the choice:\nc to create keyspace\nu to use keyspace\nd to drop keyspace\nX for exit\n")
    if s=="c":
      ks=raw_input("Enter keyspace:")
      rps=raw_input("Select replication placement strategy:\n1 for SimpleStrategy \n2 for NetworkTopologyStrategy \n3 for OldNetworkTopologyStrategy\n")
      rps_d={1:'SimpleStrategy',2:'NetworkTopologyStrategy',3:'OldNetworkTopologyStrategy'}
      rf=raw_input("Enter the replication factor(for perfect running rf should be set to 3):")
      print(cs.createKeyspace(ks,str(rps_d[int(rps)]),int(rf)))
    elif s=="u":
      ks=raw_input("Enter keyspace:")
      cs.useKeyspace(ks)
      while(True):
        t=cs.showTables(ks)
        if len(t)>0 :
          print "\n**********\nThe existing tables are:"
          for i in t:
            print i
          s=raw_input("\n**********\nDo you want to create a table or perform operations on existing table(c/p)[X for exit,B to get back]:")
          if s=="c":
            tn=raw_input("Enter a table name:")
            print "Columm names and their datatypes of table are:"
            cn=raw_input("Enter Columm names:").split(',')
            dt=raw_input("Enter datatypes for each column:").split(',')
            cn_dt=dict()
            for i in range(len(cn)):
              cn_dt[cn[i]]=dt[i]
            pk=list(raw_input("Enter column or columns which is partition key:").split(','))
            ck=list(raw_input("Enter column/s which is clustering key:").split(','))
            print cs.createTable(tn,cn_dt,pk,ck)
          elif s=='p':
            tn=raw_input("Enter table name you want to use:")
            if tn in t:
              print "\n**********\nColumns:"
              for i in cs.getColumns(tn):
                print i
              print "\nPrimary key:"
              print "\nPartition key:"
              for i in cs.getPartitionkey(tn):
                print i
              print "\nClustering keys:"
              for i in cs.getClusteringkeys(tn):
                print i
            else:
              print "Table does not exists!!"
              continue
            while(True):
              ch=raw_input("\n**********\nEnter the choice.\nI to insert data\nU to update data\nS to display data\nD to delete data\nAt to Alter table\nDt to Drop table\nTt to Truncate table\nX to exit\nB to get back\n")
              if ch=='I':
                cn_i=raw_input("Enter file name:")
                js=cs.insertData(tn,cn_i)
                print js
              elif ch=='U':
                cn_u=raw_input("Enter column names in which values are updated:").split(',')
                v_u=raw_input("Enter values that are to be updated:").split(',')
                c_c=raw_input("Enter conditional column:").split(',')
                c_v=raw_input("Enter conditional value:").split(',')
                cnu_vu=dict()
                for i in range(len(cn_u)):
                  cnu_vu[cn_u[i]]=v_u[i]
                cc_vc=dict()
                for i in range(len(c_c)):
                  cc_vc[c_c[i]]=c_v[i]
                print cs.updateData(tn,cnu_vu,cc_vc)
              elif ch=='S':
                while(True):
                  ch_s=raw_input("\n**********\nEnter choice to display data:\nDA to display whole data\nDS to display selected columns\nDA_C to display all colums with conditon\nDS_C to display selected cloumns with condition\nX to exit\nB to get back\n")
                  if ch_s=="DA":
                    l=cs.displayDataJSON(tn)
                    print l
                  elif ch_s=="DS":
                    cn_s=list(raw_input("Enter columns you want to display:").split(','))
                    l=cs.displayDataJSON(tn,cn_s)
                    print l
                  elif ch_s=="DA_C":
                    c_c=raw_input("Enter conditional columns:").split(',')
                    c_v=raw_input("Enter conditional values:").split(',')
                    cc_vc=dict()
                    for i in range(len(c_c)):
                      cc_vc[c_c[i]]=c_v[i]
                    l=cs.displayDataJSON(tn,cc_vc)
                    print l
                  elif ch_s=="DS_C":
                    cn_s=list(raw_input("Enter columns you want to display:").split(','))
                    c_c=raw_input("Enter conditional columns:").split(',')
                    c_v=raw_input("Enter conditional values:").split(',')
                    cc_vc=dict()
                    for i in range(len(c_c)):
                      cc_vc[c_c[i]]=c_v[i]
                    l=cs.displayDataJSON(tn,cn_s,cc_vc)
                    print l
                  elif ch_s=="X":
                    sys.exit(0)
                  elif ch_s=="B":
                    break
                  else:
                    print "Invalid choice"
              elif ch=='D':
                while(True):
                  ch_d=raw_input("\n**********\nEnter choice to delete data\nDS_R to delete selected columns data in a row\nDR to delete entire row\nB to get back\nX to exit\n")
                  if ch_d=="DS_R":
                    c_c=raw_input("Enter conditional columns:").split(',')
                    c_v=raw_input("Enter conditional values:").split(',')
                    cc_cv=dict()
                    for i in range(len(c_c)):
                      cc_cv[c_c[i]]=c_v[i]
                    cn_d=list(raw_input("Enter column names:").split(','))
                    print cs.deleteData(tn,cn_d,cc_cv)
                  elif ch_d=="DR":
                    c_c=raw_input("Enter conditional columns:").split(',')
                    c_v=raw_input("Enter conditional values:").split(',')
                    cc_cv=dict()
                    for i in range(len(c_c)):
                      cc_cv[c_c[i]]=c_v[i]
                    print cs.deleteData(tn,cc_cv)
                  elif ch_d=="B":
                    break
                  elif ch_d=="X":
                    sys.exit(0)
                  else:
                    print "Invalid Choice"
              elif ch=="At":
                while(True):
                  ch_a=raw_input("\n**********\nEnter choice to alter:\nAC to add column\nDC to drop column\nRC to rename column\nCD to change datatype of column(if column is newly)\nB to get back\nX to exit\n")
                  if ch_a=="AC":
                    c_a=raw_input("Enter column to be added:")
                    dt_a=raw_input("Enter datatype of column:")
                    print cs.addColumn(tn,c_a,dt_a)
                  elif ch_a=="DC":
                    c_a=raw_input("Enter column to be dropped:")
                    print cs.dropColumn(tn,c_a)
                  elif ch_a=="RC":
                    ocn=raw_input("Enter old column name:")
                    ncn=raw_input("Enter new column name:")
                    print cs.renameColumn(tn,ocn,ncn)
                  elif ch_a=="CD":
                    cn=raw_input("Enter the column name:")
                    dt=raw_input("Enter the datatype to be changed:")
                    print cs.changeDatatype(tn,cn,dt)
                  elif ch_a=="B":
                    break
                  elif ch_a=="X":
                    sys.exit(0)
                  else:
                    print "Invalid choice"
              elif ch=="Dt":
                print cs.dropTable(tn)
              elif ch=="Tt":
                print cs.truncateTable(tn)
              elif ch=="X":
                sys.exit(0)
              elif ch=="B":
                break
              else:
                print "Invalid choice"
          elif s=="B":
            break
          elif s=="X":
            sys.exit(0)
          else:
            print "Invalid choice"
        else:
          print "create a table!!"
          tn=raw_input("Enter a table name:")
          print "Columm names and their datatypes of table are:"
          cn=raw_input("Enter Columm names:").split(',')
          dt=raw_input("Enter datatypes for each column:").split(',')
          cn_dt=dict()
          for i in range(len(cn)):
            cn_dt[cn[i]]=dt[i]
          pk=list(raw_input("Enter column or columns which is partition key: ").split(','))
          ck=list(raw_input("Enter column/s which is clustering key: ").split(','))
          print cs.createTable(tn,cn_dt,pk,ck)
          continue
    elif s=="X":
      break
    elif s=="d":
      ks=raw_input("Enter keyspace:")
      print cs.dropKeyspace(ks)
    else:
      print "Invalid choice"
  else:
    print "there are no keyspaces.Create a keyspace first!"
    ks=raw_input("Enter keyspace:")
    rps=raw_input("Select replication placement strategy:\n1 for SimpleStrategy \n2 for NetworkTopologyStrategy \n3 for OldNetworkTopologyStrategy\n")
    rps_d={1:'SimpleStrategy',2:'NetworkTopologyStrategy',3:'OldNetworkTopologyStrategy'}
    rf=raw_input("Enter the replication factor(for perfect running rf should be set to 3):")
    cs.createKeyspace(ks,rps_d[rps],int(rf))
