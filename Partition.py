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
    parser.add_argument( "-t", "--table", help="Table name to merge", required=True)
    parser.add_argument( "-ow", "--owner", default='jelena', help="Username of table owner")
    parser.add_argument( "-u", "--user", default='suchyta1', help="Username of runner")
    parser.add_argument( "-o", "--out", required=True, help="Out table name")
    parser.add_argument( "-c", "--create", action='store_true', help="Create the partitioned output table")
    parser.add_argument( "-off", "--offset", action='store_true', help="Offset balrog_index")
    return parser


def ParseArgs(parser):
    args = parser.parse_args()
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
    strs = []
    sels = []
    for i in range(len(tstruct)):
        name, type, p1, p2, nullable, char_length = tstruct[i]
        if name.upper().startswith(exclude):
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


    if MPI.COMM_WORLD.Get_rank()==0:
        sel = "select unique(tilename) tilename from %s.%s_truth"%(args.owner, args.table)
        tiles = cur.quick(sel, array=True)['tilename']
    else:
        tiles = None
    tiles = mpi.Scatter(tiles)


    offset = 0
    for tab,pby in zip(tables,pbys):
        table = '%s_%s'%(args.table,tab)
        out = '%s_%s'%(args.out,tab)

        tstruct = db.ColumnDescribe(table, user=args.owner)
        types, sels = Recast(tstruct)
        parts = Partition()
    

        if (MPI.COMM_WORLD.Get_rank()==0):
            if args.create:
                arr = cur.quick("select table_name from dba_tables where owner='%s'" %(args.user.upper()), array=True)
                tables = arr['table_name']
                if out.upper() in tables:
                    cur.quick('drop table %s'%(out))
                cur.commit()
                create = "create table %s (%s)\npartition by range(%s) (%s)"%(out, ', '.join(types), pby, ', '.join(parts))
                cur.quick(create)

            elif (args.offset) and (tab=='truth'):
                m = "select max(balrog_index) offset from %s"%(out)
                offset = cur.quick(m, array=True)['offset'][0] + 1
        else:
            offset = None
        offset = mpi.Broadcast(offset)
        sels = ', '.join(sels)
        sels_off = sels.replace('(balrog_index', '(balrog_index+%i'%(offset))


        MPI.COMM_WORLD.Barrier()
        for tile in tiles:
            sel = "select %s from %s.%s where tilename='%s'"%(sels_off, args.owner, table, tile)
            ins = 'insert into %s %s'%(out, sel)
            cur.quick(ins)
            cur.commit()
        if (MPI.COMM_WORLD.Get_rank()==0):
            print table
