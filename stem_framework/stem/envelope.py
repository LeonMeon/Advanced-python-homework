import array
import mmap
from asyncio import StreamReader, StreamWriter
from io import RawIOBase, BufferedReader
from json import JSONEncoder
from typing import Optional, Union, Any
from .meta import Meta

from dataclasses import is_dataclass, asdict

Binary = Union[bytes, bytearray, memoryview, array.array, mmap.mmap]

# 1 (2 p.) must allow to serialize Meta object to JSON format as encoder for module json
class MetaEncoder(JSONEncoder):
            
    def default(self, obj: Meta) -> Any:
        if is_dataclass(obj):
            return asdict(obj)
        else:
            return json.JSONEncoder.default(self, obj)          

# 2 format for transferring data via byte stream
class Envelope:
    
    _MAX_SIZE = 128*1024*1024 # 128 Mb

    def __init__(self, meta: Meta, data : Optional[Binary] = None):
        self.meta = meta
        self.data = data

    def __str__(self):
        return str(self.meta)
    
    # i (3 p.) create Envelope instance from stream.
    @staticmethod
    def read(input: BufferedReader) -> "Envelope":
        _beginning = input.read(2) #beginning of binary string
        assert b'#~' == _beginning        
        _type  = input.read(4) #envelope format type and version. For default DF02 is used
        #assert b'DF02'  == _type #other envelope types could use the same format.        
        _metaType = input.read(2) #  metadata encoding type
        
        metaLength = int.from_bytes(input.read(4)) # metadata length in bytes 
        dataLength = int.from_bytes(input.read(4)) # the data length in bytes
        
        meta = json.loads(input.read(metaLength))
        
        _end = input.read(2) # ending of binary string
        assert b'#~'   == _end
         
        #If data size less than Envelope._MAX_SIZE store data in the memory,
        #otherwise on disk using memory mapping
        if dataLength >= Envelope._MAX_SIZE:
            data = mmap.mmap(input.fileno(), dataLength, offset = input.tell())
        else:
            data = input.read(dataLength)            
            
        return Envelope(meta, data)

    # ii (3 p.) write Envelope instance to stream
    def write_to(self, output: RawIOBase):
        # as read function
        output.write(b'#~' + b'DF02' + b'..') #beginning + _type + _metaType(empty)
        
        meta = bytes(json.dumps(self.meta), 'utf8')
        
        output.write(len(meta).to_bytes(4)) # metadata length in bytes 
        output.write(len(self.data).to_bytes(4)) # the data length in bytes
        
        output.write(meta)
        output.write(self.data)

        output.write(b'~#') # ending
    
    # iii (1 p.) create Envelope instance from binary string
    @staticmethod
    def from_bytes(buffer: bytes) -> "Envelope":
        # and that's all ,isn't it?
        return Envelope.read(BytesIO(buffer)) 
    
    #convert Envelope instance to binary string.
    #Note: Use module struct for work with binary values
    # iv (1 p.)
    def to_bytes(self) -> bytes:
        _read = BytesIO()
        self.write_to(_read)
        # return to start
        _read.seek(0) 
        return _read.read()

    @staticmethod
    async def async_read(reader: StreamReader) -> "Envelope":
        pass  # TODO(Assignment 11)

    async def async_write_to(self, writer: StreamWriter):
        pass  # TODO(Assignment 11)