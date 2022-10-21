"""
Contains the :class:`base class <tinydb.storages.Storage>` for storages and
implementations.



多表存储方案
1、每个表固定的存储块。需要记录每个块的最后位置。
多表交叉写，就是随机写现象。不能利用追加写的优势；

2、每个表一个文件。就会产生多个文件。

3、一个文件，每行数据添加表名信息。先判断是哪个表，利用字典对象的update方法。
可能产生更多的计算。待验证

{"tablename":{ "1": {"a": 1} } }
{"tablename":{ "2": {"a": 1} } }
{"tablename":{ "3": {"a": 9} } }
读取时合并
val["tablename"].update( json.loads( val2)["tablename"] )

val = json.loads( val2 )
#tables[val.keys[0] ].update( val.values[0] )

单条记录格式如下
{"T":"tablename","V":{ "1": {"a": 1} } }

val = json.loads( val2 )
tables[val["T"] ].update( val["V"] )


可以行方式追加写入。


"""

import io
import json
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

import struct
from functools import reduce


__all__ = ('Storage', 'JSONStorage', 'MemoryStorage')


def touch(path: str, create_dirs: bool):
    """
    Create a file if it doesn't exist yet.

    :param path: The file to create.
    :param create_dirs: Whether to create all missing parent directories.
    """
    if create_dirs:
        base_dir = os.path.dirname(path)

        # Check if we need to create missing parent directories
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

    # Create the file by opening it in 'a' mode which creates the file if it
    # does not exist yet but does not modify its contents
    with open(path, 'a'):
        pass

def initdb(path: str, create_dirs: bool, table: str ):
    """
    Create a file if it doesn't exist yet.

    :param path: The file to create.
    :param create_dirs: Whether to create all missing parent directories.
    """
    if create_dirs:
        base_dir = os.path.dirname(path)

        # Check if we need to create missing parent directories
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

    # Create the file by opening it in 'a' mode which creates the file if it
    # does not exist yet but does not modify its contents
    if os.path.exists( path ):
        return
    with open(path, 'w') as f:
        f.write( table )
        f.write( '\0'*( 500-len(table) ) )

class Storage(ABC):
    """
    The abstract base class for all Storages.

    A Storage (de)serializes the current state of the database and stores it in
    some place (memory, file on disk, ...).
    """

    # Using ABCMeta as metaclass allows instantiating only storages that have
    # implemented read and write

    @abstractmethod
    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        Read the current state.

        Any kind of deserialization should go here.

        Return ``None`` here to indicate that the storage is empty.
        """

        raise NotImplementedError('To be overridden!')

    @abstractmethod
    def read(self, table: str ) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        Read the current state.

        Any kind of deserialization should go here.

        Return ``None`` here to indicate that the storage is empty.
        """

        raise NotImplementedError('To be overridden!')


    @abstractmethod
    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        """
        Write the current state of the database to the storage.

        Any kind of serialization should go here.

        :param data: The current state of the database.
        """

        raise NotImplementedError('To be overridden!')

    @abstractmethod
    def write(self, table: str, data: Dict[str, Dict[str, Any]]) -> None:
        """
        Write the current state of the database to the storage.

        Any kind of serialization should go here.

        :param data: The current state of the database.
        """

        raise NotImplementedError('To be overridden!')

    def close(self) -> None:
        """
        Optional: Close open file handles, etc.
        """

        pass


class JSONStorage(Storage):
    """
    Store the data in a JSON file.
    """

    def __init__(self, path: str, create_dirs=False, encoding=None, access_mode='r+', **kwargs):
        """
        Create a new instance.

        Also creates the storage file, if it doesn't exist and the access mode is appropriate for writing.

        :param path: Where to store the JSON data.
        :param access_mode: mode in which the file is opened (r, r+, w, a, x, b, t, +, U)
        :type access_mode: str
        """

        super().__init__()

        self._mode = access_mode
        self.kwargs = kwargs

        # Create the file if it doesn't exist and creating is allowed by the
        # access mode
        if any([character in self._mode for character in ('+', 'w', 'a')]):  # any of the writing modes
            touch(path, create_dirs=create_dirs)

        # Open the file for reading/writing
        self._handle = open(path, mode=self._mode, encoding=encoding)

    def close(self) -> None:
        self._handle.close()

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        # Get the file size by moving the cursor to the file end and reading
        # its location
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        if not size:
            # File is empty, so we return ``None`` so TinyDB can properly
            # initialize the database
            return None
        else:
            # Return the cursor to the beginning of the file
            self._handle.seek(0)

            # Load the JSON contents of the file
            return json.load(self._handle)

    def write(self, data: Dict[str, Dict[str, Any]]):
        # Move the cursor to the beginning of the file just in case
        self._handle.seek(0, 2 )

        # Serialize the database state using the user-provided arguments
        serialized = json.dumps(data, **self.kwargs)

        # Write the serialized data to the file
        try:
            self._handle.write(serialized)
        except io.UnsupportedOperation:
            raise IOError('Cannot write to the database. Access mode is "{0}"'.format(self._mode))

        # Ensure the file has been written
        self._handle.flush()
        os.fsync(self._handle.fileno())

        # Remove data that is behind the new cursor in case the file has
        # gotten shorter
        #self._handle.truncate()


class JSONFrameStorage(Storage):
    """
    Store the data in a JSON file.
    """

    def __init__(self, path: str, table="_default", create_dirs=False, encoding=None, access_mode='r+', **kwargs):
        """
        Create a new instance.

        Also creates the storage file, if it doesn't exist and the access mode is appropriate for writing.

        :param path: Where to store the JSON data.
        :param access_mode: mode in which the file is opened (r, r+, w, a, x, b, t, +, U)
        :type access_mode: str
        """

        super().__init__()

        self._mode = access_mode
        self.kwargs = kwargs
        self.table = table
        #meta  head size  500b
        self.metaHeadSize = 500
        self.path = path

        # Create the file if it doesn't exist and creating is allowed by the
        # access mode
        if any([character in self._mode for character in ('+', 'w', 'a')]):  # any of the writing modes
            initdb(path, create_dirs, table )

        # Open the file for reading/writing
        self._handle = open(path, mode=self._mode, encoding=encoding)

    def close(self) -> None:
        self._handle.close()

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        # Get the file size by moving the cursor to the file end and reading
        # its location
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        if size <= self.metaHeadSize:
            # File is empty, so we return ``None`` so TinyDB can properly
            # initialize the database
            return None
        else:
            # Return the cursor to the beginning of the file
            self._handle.seek( self.metaHeadSize )

            # Load the JSON contents of the file, format  the data 
            sdata = self._handle.read()
            #print( "{\"" + self.table + "\": {"+ sdata[0:-1] + "}}" )
            return json.loads( "{\"" + self.table + "\": {"+ sdata[0:-1] + "}}" )

    def write(self, data: Dict[str, Dict[str, Any]]):
        # write one data
        # Move the cursor to the beginning of the file just in case
        self._handle.seek(0, os.SEEK_END )

        # Serialize the database state using the user-provided arguments
        serialized = json.dumps(data, **self.kwargs)
        #print( serialized[1:-1] )

        # Write the serialized data to the file
        try:
            self._handle.write(serialized[1:-1] )
            self._handle.write( "," )
        except io.UnsupportedOperation:
            raise IOError('Cannot write to the database. Access mode is "{0}"'.format(self._mode))

        # Ensure the file has been written
        self._handle.flush()
        os.fsync(self._handle.fileno())

        # Remove data that is behind the new cursor in case the file has
        # gotten shorter
        #self._handle.truncate()

    def snap(self, data: Dict[str, Dict[str, Any]]):
        #initdb( self.path+ ".0", create_dirs=True, self.table )
        with open( self.path+ ".0", 'bw') as f:
            f.write( self.table )
            f.write( '\0'*( 500-len(self.table) ) )
            # Move the cursor to the beginning of the file just in case
            f.seek( self.metaHeadSize )

            # Serialize the database state using the user-provided arguments
            serialized = json.dumps(data, **self.kwargs)

            # Write the serialized data to the file
            try:
                f.write(serialized)
            except io.UnsupportedOperation:
                raise IOError('Cannot write to the database. Access mode is "{0}"'.format(self._mode))

            # Ensure the file has been written
            f.flush()
            os.fsync( f.fileno())

        # Remove data that is behind the new cursor in case the file has
        # gotten shorter
        #self._handle.truncate()



class JSONMultiFrameStorage(Storage):
    """
    Store the data in a JSON file.
    """

    def __init__(self, path: str, tables=[], create_dirs=False, encoding=None, access_mode='r+', **kwargs):
        """
        Create a new instance.

        Also creates the storage file, if it doesn't exist and the access mode is appropriate for writing.

        :param path: Where to store the JSON data.
        :param access_mode: mode in which the file is opened (r, r+, w, a, x, b, t, +, U)
        :type access_mode: str
        """

        super().__init__()

        self._mode = access_mode
        self.kwargs = kwargs
        self.tables = tables.insert(0, "_default" )
        
        self.metaHeadSize = 500
        self.path = path

        # Create the file if it doesn't exist and creating is allowed by the
        # access mode
        if any([character in self._mode for character in ('+', 'w', 'a')]):  # any of the writing modes
            initdb(path, create_dirs, table )

        # Open the file for reading/writing
        self._handle = open(path, mode=self._mode, encoding=encoding)

    def close(self) -> None:
        self._handle.close()

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        # Get the file size by moving the cursor to the file end and reading
        # its location
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        if size <= self.metaHeadSize:
            # File is empty, so we return ``None`` so TinyDB can properly
            # initialize the database
            return None
        else:
            # Return the cursor to the beginning of the file
            self._handle.seek( self.metaHeadSize )

            # Load the JSON contents of the file, format  the data 
            sdata = self._handle.read()
            #print( "{\"" + self.table + "\": {"+ sdata[0:-1] + "}}" )
            return json.loads( "{\"" + self.table + "\": {"+ sdata[0:-1] + "}}" )

    def write(self, data: Dict[str, Dict[str, Any]]):
        # write one data
        # Move the cursor to the beginning of the file just in case
        self._handle.seek(0, os.SEEK_END )

        # Serialize the database state using the user-provided arguments
        serialized = json.dumps(data, **self.kwargs)
        #print( serialized[1:-1] )

        # Write the serialized data to the file
        try:
            self._handle.write(serialized[1:-1] )
            self._handle.write( "," )
        except io.UnsupportedOperation:
            raise IOError('Cannot write to the database. Access mode is "{0}"'.format(self._mode))

        # Ensure the file has been written
        self._handle.flush()
        os.fsync(self._handle.fileno())

        # Remove data that is behind the new cursor in case the file has
        # gotten shorter
        #self._handle.truncate()

    def snap(self, data: Dict[str, Dict[str, Any]]):
        #initdb( self.path+ ".0", create_dirs=True, self.table )
        with open( self.path+ ".0", 'bw') as f:
            f.write( self.table )
            f.write( '\0'*( 500-len(self.table) ) )
            # Move the cursor to the beginning of the file just in case
            f.seek( self.metaHeadSize )

            # Serialize the database state using the user-provided arguments
            serialized = json.dumps(data, **self.kwargs)

            # Write the serialized data to the file
            try:
                f.write(serialized)
            except io.UnsupportedOperation:
                raise IOError('Cannot write to the database. Access mode is "{0}"'.format(self._mode))

            # Ensure the file has been written
            f.flush()
            os.fsync( f.fileno())

        # Remove data that is behind the new cursor in case the file has
        # gotten shorter
        #self._handle.truncate()

class JSONMultiFrameMeta():
    """
    meta  head size  500b     version[byte]  namelist[length 20byte]  tablenames[] ， tablename length max 20b， list max 20个
    """
    def __init__(self):
        """
        Create a new instance.
        """
        self.version = 1  # 1byte
        self.tables=[]
        self.headSizeMax = 500  #byte
        self.tableNameLengthMax = 20 #byte
        self.tableCountMax = 20  #
    
    def parse( self, headbytes: bytes ):
        """
        解析头信息
        ？比如有3张表，剩余的17字节 0值，如何pack 或unpack
        ？名称  中 剩余的字节 \0，如何pack 或unpack

        """
        self.version = int( headbytes[0] )
        namelength_list = struct.unpack( "20B", headbytes[1:21])
        namefmt = ""
        for namelength in namelength_list:
            if namelength > 0:
                namefmt = namefmt + str(namelength) + "s"
            else:
                break   # 0s
        #print( namefmt )
        #print( headbytes[21: struct.calcsize(namefmt) + 21 ] )
        self.tables = struct.unpack( namefmt, headbytes[21: struct.calcsize(namefmt) + 21 ] )


    def packHeadMeta( self ):
        """
        解决问题
        >>> struct.unpack( '1B', b'\0' )
        (0,)
        >>> struct.unpack( '2s', b'aa\0' )
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        struct.error: unpack requires a buffer of 2 bytes
        >>> struct.unpack( '3s', b'aa\0' )
        (b'aa\x00',)
        >>> struct.unpack( '3s', b'aa\0' )[0]
        b'aa\x00'
        >>> d =  struct.unpack( '3s', b'aa\0' )
        >>> str( d[0])
        "b'aa\\x00'"
        >>> s = str( d[0])
        >>> s
        "b'aa\\x00'"
        >>> print( s )
        b'aa\x00'

        """
        namelength_list = []
        namefmt = ""
        sum = 0
        for name in self.tables:
            length = len( name )
            namelength_list.append( length  )
            namefmt = namefmt + f"{length}s"
            sum = sum + length 

        if sum + 21 > self.headSizeMax:
            raise Error( "tablenames length > " + self.headSizeMax )
       
        headfmt = "1b20B{0}s{1}s".format( sum, self.headSizeMax - 21 - sum )
        print( "headfmt=" + headfmt ) 
        headbytes = struct.pack( headfmt, self.version, *namelength_list, *b'\0'*(20- len(self.tables) ), bytes( "".join(self.tables), 'utf-8' ), b''*(500-21-sum) )
        

        return headbytes
        
    def calTableNamesLength(self):
        return reduce( lambda x,y : x + len( y) , [0, *self.tables] )


class JSONMultiTableLineStorage(Storage):
    """
    Store the data in a JSON file. 
    一行一条数据，格式：
    {"T": "tablename", "V": {"doc_id": object} } 
    """

    def __init__(self, path: str, tables=[], create_dirs=False, encoding=None, access_mode='r+', **kwargs):
        """
        Create a new instance.

        Also creates the storage file, if it doesn't exist and the access mode is appropriate for writing.

        :param path: Where to store the JSON data.
        :param access_mode: mode in which the file is opened (r, r+, w, a, x, b, t, +, U)
        :type access_mode: str
        """

        super().__init__()

        self._mode = access_mode
        self.kwargs = kwargs
        self.tables = tables.insert(0, "_default" )
        
        #self.metaHeadSize = 500
        self.path = path

        # Create the file if it doesn't exist and creating is allowed by the
        # access mode
        if any([character in self._mode for character in ('+', 'w', 'a')]):  # any of the writing modes
            touch(path, create_dirs=create_dirs)

        # Open the file for reading/writing
        self._handle = open(path, mode=self._mode, encoding=encoding)

    def close(self) -> None:
        self._handle.close()

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        # Get the file size by moving the cursor to the file end and reading
        # its location
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()
        data = {}

        if size <= 0:
            # File is empty, so we return ``None`` so TinyDB can properly
            # initialize the database
            return None
        else:
            # Return the cursor to the beginning of the file
            self._handle.seek( 0 )

            # Load the JSON contents of the file, format  the data 
            for line in self._handle.readlines():
                val = json.loads( line )
                if not data.get( val["T"] ):
                    data[val["T"] ] = {}
                data[val["T"] ].update( val["V"] )

            return data

    def write(self, data: Dict[str, Dict[str, Any]]):
        # write one data
        # Move the cursor to the beginning of the file just in case
        self._handle.seek(0, os.SEEK_END )

        # Serialize the database state using the user-provided arguments
        sdata = {"T": data["__T"] }
        del data[ "__T" ]
        sdata["V"] = data

        serialized = json.dumps(sdata, **self.kwargs)
        #print( serialized[1:-1] )

        # Write the serialized data to the file
        try:
            self._handle.write( serialized )
            self._handle.write( "\n" )
        except io.UnsupportedOperation:
            raise IOError('Cannot write to the database. Access mode is "{0}"'.format(self._mode))

        # Ensure the file has been written
        self._handle.flush()
        os.fsync(self._handle.fileno())

        # Remove data that is behind the new cursor in case the file has
        # gotten shorter
        #self._handle.truncate()



class MemoryStorage(Storage):
    """
    Store the data as JSON in memory.
    """

    def __init__(self):
        """
        Create a new instance.
        """

        super().__init__()
        self.memory = None

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        return self.memory

    def write(self, data: Dict[str, Dict[str, Any]]):
        self.memory = data
