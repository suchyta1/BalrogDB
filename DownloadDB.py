#!/usr/bin/env python

import argparse
import numpy as np
import pandas as pd
import h5py
import desdb
import re

import suchyta_utils as es
import suchyta_utils.mpi as mpi
from mpi4py import MPI


def SetupParser():
    parser = argparse.ArgumentParser()
    parser.add_argument( "-t", "--table", help="DB table name to download", required=True)
    parser.add_argument( "-d", "--destable", help="DES table name", default='y1a1_coadd_objects')

    parser.add_argument( "-u", "--user", help="User who owns the DB table", default=None)
    parser.add_argument( "-c", "--chunkby", help="Get data in chunks by this column name", default='tilename')

    parser.add_argument( "-b", "--bands", help="Bands to get (if they exist in a table)", default='g,r,i,z,y,det')
    parser.add_argument( "-dc", "--descols", help="DES column names", default='all')
    parser.add_argument( "-sc", "--simcols", help="sim column names", default='all')
    parser.add_argument( "-tc", "--truthcols", help="truth column names", default='all')

    return parser

def ParseArgs(parser):
    args = parser.parse_args()
    args.bands = args.bands.split(',')
    if args.user is None:
        args.user = es.db.GetUser()

    args.truth = '%s_truth'%(args.table)
    args.utruth = '%s.%s'%(args.user, args.truth)

    args.sim = '%s_sim'%(args.table)
    args.usim = '%s.%s'%(args.user, args.sim)

    args.nosim = '%s_sim'%(args.table)
    args.unosim = '%s.%s'%(args.user, args.nosim)

    return args

def GetArgs():
    parser = SetupParser()
    args = ParseArgs(parser)
    return args

def AllOrFile(what, cat, user):
    cols = np.core.defchararray.upper( es.db.ColumnDescribe(cat, user=user)['column_name'] )
    if what!='all':
        newcols = np.loadtxt(what, dtype=np.str_)
        base = []
        comp = re.compile('_[grizy]$|_det$')
        for i in range(len(newcols)):
            s = comp.sub('', newcols[i].lower()).upper()
            if s not in base:
                base.append(s)
        newcols = []
        for band in args.bands:
            newcols = np.append( newcols, np.core.defchararray.add(base,'_%s'%(band.upper())) )
        cut = np.in1d(newcols, cols)
        cols = newcols[cut]
    cut = -( np.core.defchararray.find(cols, 'SYS_')!=-1 )
    return cols[cut]
    


if __name__=='__main__': 
    rank = MPI.COMM_WORLD.Get_rank()
    args = GetArgs()
    cur = desdb.connect()
   
    chunks = truthcols = simcols = descols = None
    if rank==0:
        chunks = cur.quick("select unique(%s) from %s"%(args.chunkby, args.utruth), array=True)['tilename']

        truthcols = AllOrFile(args.truthcols, args.truth, args.user)
       
        simcols = AllOrFile(args.simcols, args.sim, args.user)
        cut = -( np.in1d(simcols, truthcols) )
        simcols = simcols[cut]

        descols = AllOrFile(args.descols, args.destable, None)
        
        truthcols = ', '.join(np.core.defchararray.add('truth.',truthcols))
        simcols = ', '.join(np.core.defchararray.add('sim.',simcols))
        descols = ', '.join(np.core.defchararray.add('des.',descols))

    chunks = mpi.Scatter(chunks)
    truthcols, simcols, descols = mpi.Broadcast(truthcols, simcols, descols)

    for chunk in chunks:
        if type(chunk)==np.str_:
            chunk = "'%s'"%(chunk)

        q = "select %s from %s truth where truth.%s=%s"%(truthcols, args.utruth, args.chunkby, chunk)
        data = cur.quick(q, array=True)
        t = len(data)
        #print 'truth', len(data), es.system.GetCurrentMemoryUsage()

        q = "select %s, %s from %s sim, %s truth where truth.%s=%s and truth.balrog_index=sim.balrog_index"%(simcols, truthcols, args.usim, args.utruth, args.chunkby, chunk)
        data = cur.quick(q, array=True)
        s = len(data)
        #print 'sim', len(data), es.system.GetCurrentMemoryUsage()

        q = "select %s from %s des where des.%s=%s"%(descols, args.destable, args.chunkby, chunk)
        data = cur.quick(q, array=True)
        d = len(data)
        #print 'des', len(data), es.system.GetCurrentMemoryUsage()

        print chunk, t, s, d, es.system.GetMaxMemoryUsage()
