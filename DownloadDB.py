#!/usr/bin/env python

import argparse
import numpy as np
import desdb
import re
import Queue
import time
import os

import fitsio
import esutil

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

    parser.add_argument( "-ft", "--filetype", help="output file type", default='.fits', choices=['.fits', '.h5'])
    parser.add_argument( "-o", "--outdir", help="output directory", default=None)

    return parser

def ParseArgs(parser):
    args = parser.parse_args()
    args.bands = args.bands.split(',')
    if args.user is None:
        args.user = es.db.GetUser()
    if args.outdir is None:
        if args.filetype=='.fits':
            args.outdir = 'FITS'
        else:
            args.outdir = 'HDF%'

    args.file = []
    for f in ['truth','sim','nosim','des']:
        args.file.append( os.path.join(args.outdir, '%s-%s%s'%(args.table,f,args.filetype)) )


    args.truth = '%s_truth'%(args.table)
    args.utruth = '%s.%s'%(args.user, args.truth)

    args.sim = '%s_sim'%(args.table)
    args.usim = '%s.%s'%(args.user, args.sim)

    args.nosim = '%s_nosim'%(args.table)
    args.unosim = '%s.%s'%(args.user, args.nosim)

    args.cols = ColumnSelects(args)

    return args

def GetArgs():
    parser = SetupParser()
    args = ParseArgs(parser)
    return args


def AllOrFile(what, cat, user, bands):
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
        for band in bands:
            newcols = np.append( newcols, np.core.defchararray.add(base,'_%s'%(band.upper())) )
        cut = np.in1d(newcols, cols)
        cols = newcols[cut]
    cut = -( np.core.defchararray.find(cols, 'SYS_')!=-1 )
    return cols[cut]


def ColumnSelects(args):
    truthcols = AllOrFile(args.truthcols, args.truth, args.user, args.bands)
   
    simcols = AllOrFile(args.simcols, args.sim, args.user, args.bands)
    cut = -( np.in1d(simcols, truthcols) )
    simcols = simcols[cut]

    descols = AllOrFile(args.descols, args.destable, None, args.bands)
    
    truthcols = ', '.join(np.core.defchararray.add('truth.',truthcols))
    simcols = ', '.join(np.core.defchararray.add('sim.',simcols))
    descols = ', '.join(np.core.defchararray.add('des.',descols))

    return truthcols, simcols, descols



def Serve(chunks):
    rsize = MPI.COMM_WORLD.size - 1
    csize = len(chunks)
    sent = 0
    rdone = 0
    wait = [ [],[],[],[] ]

    while (rdone < rsize):
        rank, msg = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE)

        if msg==0:
            if (sent < csize):
                MPI.COMM_WORLD.send(chunks[sent], dest=rank)
                sent += 1
            else:
                MPI.COMM_WORLD.send(-1, dest=rank)
                rdone += 1

        else:
            for i in range(len(wait)):
                if msg==(i,0):
                    if rank not in wait[i]:
                        wait[i].append(rank)

                    if wait[i][0]==rank:
                        MPI.COMM_WORLD.send(1, dest=rank)
                    else:
                        MPI.COMM_WORLD.send(0, dest=rank)

                elif msg==(i,1):
                    del wait[i][0]


def Work(args, rank):
    cur = desdb.connect()
    while True:
        cmd = MPI.COMM_WORLD.sendrecv([rank, 0], dest=0, source=0)
        if cmd==-1:
            break
        else:
            GetData(args, cmd, args.cols[0], args.cols[1], args.cols[2], cur, rank)
            MPI.COMM_WORLD.send([rank, -1], dest=0)

def WaitOrWrite(num, rank, args, data):
    while True:
        msg = MPI.COMM_WORLD.sendrecv([rank,(num,0)], dest=0, source=0)
        if msg==1:
            WriteData(data,args,num)
            MPI.COMM_WORLD.send([rank,(num,1)], dest=0)
            break

def WriteData(data, args, num):
    time.sleep(1)
    file = args.file[num]

    if args.filetype=='.fits':
        if not os.path.exists(file):
            f = esutil.io.write(file, data)
        else:
            f = fitsio.FITS(file, 'rw')
            f[-1].append(data)


def GetData(args, chunk, truthcols, simcols, descols, cur, rank):
    if type(chunk)==np.str_:
        chunk = "'%s'"%(chunk)

    q = "select %s from %s truth where truth.%s=%s"%(truthcols, args.utruth, args.chunkby, chunk)
    data = cur.quick(q, array=True)
    t = len(data)
    WaitOrWrite(0, rank, args, data)

    q = "select %s, %s from %s sim, %s truth where truth.%s=%s and truth.balrog_index=sim.balrog_index"%(simcols, truthcols, args.usim, args.utruth, args.chunkby, chunk)
    data = cur.quick(q, array=True)
    s = len(data)
    WaitOrWrite(1, rank, args, data)

    q = "select %s, %s from %s sim, %s truth where truth.%s=%s and truth.balrog_index=sim.balrog_index"%(simcols, truthcols, args.unosim, args.utruth, args.chunkby, chunk)
    data = cur.quick(q, array=True)
    n = len(data)
    WaitOrWrite(2, rank, args, data)

    q = "select %s from %s des where des.%s=%s"%(descols, args.destable, args.chunkby, chunk)
    data = cur.quick(q, array=True)
    d = len(data)
    WaitOrWrite(3, rank, args, data)

    print chunk, t, n, s, d, es.system.GetMaxMemoryUsage()


def FileSetup(args):
    for file in args.file:
        if os.path.exists(file):
            os.remove(file)
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)


if __name__=='__main__': 
    rank = MPI.COMM_WORLD.Get_rank()
    args = GetArgs()
    #chunks = truthcols = simcols = descols = None
   
    if rank==0:
        cur = desdb.connect()
        chunks = cur.quick("select unique(%s) from %s"%(args.chunkby, args.utruth), array=True)['tilename']
        FileSetup(args)
        Serve(chunks)
    else:
        Work(args, rank)

    #chunks = mpi.Scatter(chunks)
    #cols = mpi.Broadcast(truthcols, simcols, descols)
