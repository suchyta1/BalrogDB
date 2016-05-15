#!/usr/bin/env python

import sys
import argparse
import desdb
import suchyta_utils.db
import numpy as np

def SetupParser():
    parser = argparse.ArgumentParser()
    parser.add_argument( "-t", "--table", help="DB table name to download", required=True)
    parser.add_argument( "-n", "--indexname", help="base string for index name", default=None)
    parser.add_argument( "-d", "--drop", help="Drop instead of create", action="store_true")
    return parser

def ParseArgs(parser):
    args = parser.parse_args()
    if args.indexname is None:
        args.indexname = 'i_%s'%(args.table.lstrip("balrog_"))
    return args

def GetArgs():
    parser = SetupParser()
    args = ParseArgs(parser)
    return args


if __name__=='__main__': 

    args = GetArgs()
    tables = ['truth', 'sim', 'nosim']
    tnames = ['t','s','n']
    columns = ['balrog_index','tilename']
    cnames = ['b','t']

    cur = desdb.connect()
    truth = '%s_%s'%(args.table, tables[0])
    sim = '%s_%s'%(args.table, tables[1])


    # Add a primary key for balrog_index in the truth table
    docmd = False
    ptname = '%s_p'%(truth)
    cons = suchyta_utils.db.ConstraintDescribe(truth)
    found = (ptname.upper() in cons['constraint_name'])
    if args.drop and found:
        cmd = "ALTER TABLE %s DROP CONSTRAINT %s" %(truth,ptname)
        docmd = True
    elif (not args.drop) and (not found):
        cmd = "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (balrog_index)" %(truth,ptname)
        docmd = True
    if docmd:
        print cmd
        cur.quick(cmd)

    """
    docmd = False
    psname = '%s_p'%(sim)
    cons = suchyta_utils.db.ConstraintDescribe(sim)
    found = (psname.upper() in cons['constraint_name'])
    if args.drop and found:
        cmd = "ALTER TABLE %s DROP CONSTRAINT %s" %(sim,psname)
        docmd = True
    elif (not args.drop) and (not found):
        cmd = "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (balrog_index, number_sex)" %(sim,psname)
        docmd = True
    if docmd:
        print cmd
        cur.quick(cmd)
    """
    
    added = {}
    added['index'] = []
    added['table'] = []

    # Add indexes in each table on columns
    for i in range(len(tables)):
        tname = '%s_%s'%(args.table, tables[i])
        inds = suchyta_utils.db.IndexDescribe(tname)

        for j in range(len(columns)):
            iname = '%s_%s%s'%(args.indexname, tnames[i], cnames[j])
            found = (iname.upper() in inds['index_name'])
            docmd = False

            if args.drop and found:
                cmd = 'DROP INDEX %s'%(iname)
                docmd = True
            elif (not args.drop) and (not found) and (not ((j==0) and (i==0))):
                cmd = 'CREATE INDEX %s on %s (%s)'%(iname, tname, columns[j])
                docmd = True
                added['table'].append(tname)
                added['index'].append(iname)
            if docmd:
                print cmd
                cur.quick(cmd)
  

    # Add an index on joining the truth table and the sim table
    inds = suchyta_utils.db.IndexDescribe(truth)
    for j in range(len(columns)):
        iname = '%s_j%s'%(args.indexname,cnames[j])
        found = (iname.upper() in inds['index_name'])
        docmd = False

        if args.drop and found:
            cmd = "DROP INDEX %s" %(iname)
            docmd = True
        elif (not args.drop) and (not found):
            cmd = "CREATE BITMAP INDEX %s ON %s(%s.%s) FROM %s, %s WHERE %s.balrog_index=%s.balrog_index" %(iname, sim,truth,columns[j], truth,sim, sim,truth)
            docmd = True
            added['table'].append(truth)
            added['index'].append(iname)
        if docmd:
            print cmd
            cur.quick(cmd)
        
        """
        iname = '%s_js%s'%(args.indexname,cnames[j])
        found = (iname.upper() in inds['index_name'])
        docmd = False

        if args.drop and found:
            cmd = "DROP INDEX %s" %(iname)
            docmd = True
        elif (not args.drop) and (not found):
            cmd = "CREATE BITMAP INDEX %s ON %s(%s.%s) FROM %s, %s WHERE %s.balrog_index=%s.balrog_index" %(iname, truth,sim,columns[j], sim,truth, truth,sim)
            docmd = True
        if docmd:
            print cmd
            cur.quick(cmd)
        """
   
    cur.commit() 
    user = suchyta_utils.db.GetUser()
    utab = np.unique(added['table'])
    for i in range(len(utab)):
        cmd = """begin DBMS_STATS.GATHER_TABLE_STATS (ownname => '%s', tabname => '%s'); end;"""%(user.upper(),utab[i].upper())
        print cmd
        cur.quick(cmd)

    for i in range(len(added['index'])):
        cmd = """begin DBMS_STATS.GATHER_INDEX_STATS (ownname => '%s', indname => '%s'); end;"""%(user.upper(),added['index'][i].upper())
        print cmd
        cur.quick(cmd)
    cur.commit() 
