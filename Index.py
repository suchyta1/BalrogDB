#!/usr/bin/env python

import sys
import argparse
import desdb

def SetupParser():
    parser = argparse.ArgumentParser()
    parser.add_argument( "-t", "--table", help="DB table name to download", required=True)
    parser.add_argument( "-n", "--indexname", help="base string for index name", default=None)
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
    columns = ['tilename', 'balrog_index', 'tilename, balrog_index']
    cnames = ['t','b','tb']

    cur = desdb.connect()
    for i in range(len(tables)):
        for j in range(len(columns)):
            iname = '%s_%s%s'%(args.indexname, tnames[i], cnames[j])
            tname = '%s_%s'%(args.table, tables[i])
            cmd = 'CREATE INDEX %s on %s (%s)'%(iname, tname, columns[j])
            print cmd
            cur.quick(cmd)
    cur.commit() 
