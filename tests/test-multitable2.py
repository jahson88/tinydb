
import sys
sys.path.append( f'D:\\github\\tinydb2' )

import re
from collections.abc import Mapping

import pytest

from tinydb import TinyDB, where, Query
from tinydb.middlewares import Middleware, CachingMiddleware
from tinydb.storages import MemoryStorage, JSONStorage, JSONFrameStorage, JSONMultiFrameMeta, JSONMultiTableLineStorage
from tinydb.table import Document

import json

from timeit import timeit


def save_data():
	path = str('db2-4.json')

	# Verify ids are unique when reopening the DB and inserting
	with TinyDB(path, storage=JSONFrameStorage) as db:
        
		db.table('table1').insert({'a': 1})
		db.table('table1').insert({'a': 9})
		#db.table('table2').insert({'a': 1})
		print( db.tables() )
		#print( db.table('table1').all() )

def save_data1():
	path = str('db2-1.json')

	# Verify ids are unique when reopening the DB and inserting   11229.0964569
	with TinyDB(path, storage=JSONFrameStorage) as db:
		for i in range(50000):
			db.table('table1').insert({'a': i, 'content': "this is test value,this is test value,this is test value,this is test value,this is test value,the value is %d"%(i) })


def save_data2():
	path = str('db4-2.json')

	# Verify ids are unique when reopening the DB and inserting 50000条  old 11229.0964569   new 4968.6888719   1W条 655.742566
	with TinyDB(path, storage=JSONFrameStorage) as db:
		for i in range(10000):
			db.insert({'a': i, 'content': "this is test value,this is test value,this is test value,this is test value,this is test value,the value is %d"%(i) })

def save_datawithline():
	path = str('db3-2.json')

	# Verify ids are unique when reopening the DB and inserting   10000    733.623031
	with TinyDB(path, storage=JSONMultiTableLineStorage) as db:
		for i in range( 10000 ):
			db.insert({'a': i, 'content': "this is test value,this is test value,this is test value,this is test value,this is test value,the value is %d"%(i) })

	#with TinyDB(path, storage=JSONMultiTableLineStorage) as db:
	#	print( db.all() )

def query_datawithline():
    path = str('db3-2.json')

    # Verify ids are unique when reopening the DB and inserting   10000    733.623031
    with TinyDB(path, storage=JSONMultiTableLineStorage) as db:
        print( db.get( where('a') == 1 )  )
        print( db.count( where('a') < 80 )  )


def testDoubleId():
    """
    重复的doc_id  解析后保存最后的一条记录值。
    {'1': 'test5', '2': 'test4'}
    """
    val = '{"1":"test1", "2":"test2","1":"test3","2":"test4","1":"test5"}'
    print( json.loads( val ))

def testMerge():
    """
    重复的doc_id  合并后保存最后的一条记录值。 与合并的先后顺序有关，最后的值保留
    {'1': 'test5', '2': 'test4'}
    """
    val1 = '{"1":"test1", "2":"test2"}'
    val2 = '{"1":"test3","2":"test4"}'
    val3 = '{"1":"test5","2":"test6","3":"test7"}'
    val = {}
    val.update( json.loads( val1 )  )
    val.update( json.loads( val2 )  )
    val.update( json.loads( val3 )  )
    print( val )   #{'1': 'test5', '2': 'test6', '3': 'test7'}
    val11 = {}
    val11.update( json.loads( val1 )  )
    val11.update( json.loads( val3 )  )
    val11.update( json.loads( val2 )  )
    print( val11 )  #{'1': 'test3', '2': 'test4', '3': 'test7'}
    val12 = {}
    val12.update( json.loads( val3 )  )
    val12.update( json.loads( val1 )  )
    val12.update( json.loads( val2 )  )
    print( val12 )  #{'1': 'test3', '2': 'test4', '3': 'test7'}

    val13 = {}
    val13.update( json.loads( val3 )  )
    val13.update( json.loads( val2 )  )
    val13.update( json.loads( val1 )  )
    print( val13 )  #{'1': 'test1', '2': 'test2', '3': 'test7'}

def testTableMerge():
    val1 = '{"tablename":{ "1": {"a": 1} } }'
    val2 = '{"tablename":{ "2": {"a": 1} } }'
    val3 = '{"tablename":{ "3": {"a": 9} } }'
    val ={}
    val.update( json.loads( val1) )
    val.update( json.loads( val2) )
    val.update( json.loads( val3) )
    print( val )
    val = json.loads( val1 ) 
    val["tablename"].update( json.loads( val2)["tablename"] )
    val["tablename"].update( json.loads( val3)["tablename"] )
    print( val )


def testMetaPack():
    meta = JSONMultiFrameMeta()
    meta.tables = ["aaaa", "bb", "cccccccc"]
    print( meta.calTableNamesLength() )
    '''
    data = meta.packHeadMeta()
    with open( "meta.m", "bw") as f:
        f.write( data)
    '''
def testMetaUnpack():
    with open( "meta.m", "br") as f:
        data = f.read( )

    meta = JSONMultiFrameMeta()
    meta.parse( data )
    print( meta.tables )
    '''
    data = meta.packHeadMeta()
    
    '''

if __name__ == "__main__":
	#print( timeit( "save_data2()", "from __main__ import save_data2",number=1 ) )
    #save_data()
    #print( timeit( "save_datawithline()", "from __main__ import save_datawithline",number=1 ) )
    #save_datawithline()
    #testMerge()
    #testMeta()
    #testMetaUnpack()
    #testTableMerge()
    query_datawithline()