#!/usr/bin/env python

import logging
import argparse
import numpy as np
import desdb
import os
import sys

import fitsio
import numpy.lib.recfunctions as rec
import suchyta_utils.db as db

import suchyta_utils.mpi as mpi
from mpi4py import MPI




def SetupParser():
    parser = argparse.ArgumentParser()
    parser.add_argument( "-t", "--tables", help="Table name to merge", required=True)
    parser.add_argument( "-ow", "--owners", required=True, help="Username of table owner")
    parser.add_argument( "-off", "--offsets", required=True, help="Username of table owner")
    parser.add_argument( "-u", "--user", default='suchyta1', help="Username of runner")
    parser.add_argument( "-o", "--out", required=True, help="Out table name")
    parser.add_argument( "-c", "--create", action='store_true', help="Create the partitioned output table")
    return parser


def ParseArgs(parser):
    args = parser.parse_args()
    args.tables = args.tables.split(',')
    args.owners = args.owners.split(',')
    args.offsets = args.offsets.split(',')
    return args

def GetArgs():
    parser = SetupParser()
    args = ParseArgs(parser)
    return args

def Recast(tstruct):
    recast = {'balrog_index': 'number(12)'}
    bands = ['g','r','i','z','y']
    for band in bands:
        recast['not_drawn_%s'%(band)] = 'number(5)'

    exclude = 'SYS_'
    bad = ['RUNNUM']
    tbands = ['det','g','r','i','z','y']
    for tb in tbands:
        bad.append('SLROK_%s'%(tb.upper()))

    strs = []
    sels = []
    for i in range(len(tstruct)):
        name, type, p1, p2, nullable, char_length = tstruct[i]
        if (name.upper().startswith(exclude)) or (name.upper() in bad):
            continue
        nl = name.lower()
        s = '%s'%(nl)

        if nl in recast.keys():
            r = recast[nl]
            s = '%s %s'%(s, recast[nl])
            sels.append('cast(%s as %s) %s'%(nl, recast[nl], nl))
        else:
            sels.append(s)
            s = '%s %s'%(s, type.lower())
            if type.lower()=='number':
                s = '%s(%i, %i)'%(s, int(p1), int(p2))
            if type.lower()=='varchar2':
                s = '%s(%i)'%(s, int(char_length))

        if nullable.lower()=='n':
            s = '%s not null'%(s)

        strs.append(s)
    return strs, sels

def Partition():
    vals = ['12', '23', '33', '43', '53', '61', '68', '75', '82', '90', '301', '317', '329', '341', '349', 'max']
    ps = []
    for i in range(len(vals)):
        if i<(len(vals)-1):
            ps.append('partition p_y1a1_s82_sim_%s values less than (%s)'%(vals[i], vals[i]))
        else:
            ps.append('partition p_y1a1_s82_sim_max values less than (maxvalue)')
    return ps


if __name__=='__main__': 
    args = GetArgs()
    cur = desdb.connect()

    tables = ['truth','sim','nosim']
    pbys = ['ra', 'alphawin_j2000_i', 'deltawin_j2000_i']


    if (MPI.COMM_WORLD.Get_rank()==0):
        offset = 0
        if (args.offsets[0]=='out'):
            t = '%s_truth'%(args.out)
            m = "select max(balrog_index) offset from %s"%(t)
            offset = cur.quick(m, array=True)['offset'][0] + 1


    for i in range(len(args.tables)):
        args.table = args.tables[i]
        args.owner = args.owners[i]

        if MPI.COMM_WORLD.Get_rank()==0:
            sel = "select unique(tilename) tilename from %s.%s_truth"%(args.owner, args.table)
            tiles = cur.quick(sel, array=True)['tilename']
        else:
            tiles = None
        tiles = mpi.Scatter(tiles)

        if MPI.COMM_WORLD.Get_rank()!=0:
            offset = None
        offset = mpi.Broadcast(offset)


        for tab,pby in zip(tables,pbys):
            table = '%s_%s'%(args.table,tab)
            out = '%s_%s'%(args.out,tab)
            tstruct = db.ColumnDescribe(table, user=args.owner)
            types, sels = Recast(tstruct)
        
            if (MPI.COMM_WORLD.Get_rank()==0) and (i==0):
                if args.create:
                    parts = Partition()
                    arr = cur.quick("select table_name from dba_tables where owner='%s'" %(args.user.upper()), array=True)
                    table_names = arr['table_name']
                    if out.upper() in table_names:
                        cur.quick('drop table %s'%(out))
                    cur.commit()
                    create = "create table %s (%s)\npartition by range(%s) (%s)"%(out, ', '.join(types), pby, ', '.join(parts))
                    cur.quick(create)

            sels = ', '.join(sels)
            sels_off = sels.replace('(balrog_index', '(balrog_index+%i'%(offset))

            if (args.create) and (i==0):
                MPI.COMM_WORLD.Barrier()

            for tile in tiles:
                sel = "select %s from %s.%s where tilename='%s'"%(sels_off, args.owner, table, tile)
                ins = 'insert into %s %s'%(out, sel)
                cur.quick(ins)
                cur.commit()


        if (MPI.COMM_WORLD.Get_rank()==0):
            print args.table, offset
            if (i < len(args.tables)-1):
                if (args.offsets[i+1]=='last'): 
                    m = "select max(balrog_index) offset from %s.%s_truth"%(args.owner,args.table)
                    offset = offset + int(cur.quick(m, array=True)['offset'][0]) + 1
                elif (args.offsets[i+1]=='out'):
                    m = "select max(balrog_index) offset from %s"%(out)
                    offset = offset + int(cur.quick(m, array=True)['offset'][0]) + 1
