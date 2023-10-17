
"""
# == =================================================================================================== == #
# == PyRocker: Keyrock's python tools for DeFi and Market Making Data Analysis                           == #
# == =================================================================================================== == #
# == File: dataIO.py                                                                                     == #
# == Description: Data Processing for Input and Output operations                                        == #
# == =================================================================================================== == #
# == Maintainer: francisco@keyrock.eu                                                                    == #
# == Repository: https://github.com/KeyrockEU/PyRocker                                                   == #
# == =================================================================================================== == #
"""

# -- Load packages for this script
import sys
import time
import pickle
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime as dt

# -- ------------------------------------------------------------- UNIVERSAL TIMESTAMP FORMAT CONVERSION -- #
# -- ------------------------------------------------------------- ------------------------------------- -- #

def ts_conversion(data_to_convert=None, ts_factor:int = 1000, ts_format:str = "%Y-%m-%d %H:%M:%S.%f", 
                  conversion_type:str = 'unix_to_ts'):
    """
    Universal Timestamp conversion
    
    Parameters
    ----------
                  
    data_to_convert:None, (default=None)
        Data to be converted, either a float that represents unix timestamp, or string with known format

    ts_factor:int, (default=1000)
        Factor to divide by the float unix timestamp

    ts_format:str, (default="%Y-%m-%d %H:%M:%S.%f")
        Either the input or output string format

    conversion_type:str, (default='unix_to_str')
        The conversion type to be chosen from

    Returns
    -------

        Either a float that represents a unix timestamp (conversion_type='unix_to_str') or
        a string with format same as ts_format (conversion_type='str_to_unix')

    References
    ----------

    [1] https://docs.python.org/3/library/datetime.html

    """

    if conversion_type == 'unix_to_str':
        return dt.datetime.utcfromtimestamp(data_to_convert/ts_factor).strftime(ts_format)[:-3]
    
    elif conversion_type == 'str_to_unix':
        return dt.datetime.strptime(data_to_convert, ts_format).timestamp()

    elif conversion_type == 'timestamp_to_str':
        return str(data_to_convert)

# -- -------------------------------------------------------------- INPUT/OUTPUT FOR DATA READING/WRITING -- #
# -- -------------------------------------------------------------- ------------------------------------- -- #

def data_file(object_data:object = None, operation:str = '', in_format:str = '', out_format:str = '',
              file_name:str = '', file_dir:str = '', memorytime:bool = False) -> dict:
    """
    
    Read and write data from and into files of different formats

    Parameters
    ----------
    
    object_data:object (default: None)
        The object containing the data to be written, and for read operation is ignored.
    
    operation:str ('read', 'write', default: '')
        The I/O operation to be performed
    
    in_format:str ('pickle', 'parquet', default: '')
        Format of the input file in the 'read' operation, and optional for 'write' operation
    
    out_format:str = ('default: '')
        Format of the file to be written with for the 'write' option, for the 'read' option is the 
        format of the object to be returned.
    
    file_name:str = (default: '')
        Name of the file, without file path/route
    
    file_dir:str = (default: '')
        Absolute path direction where to write to or read from.
    
    memorytime:bool = (True, False, default: False)
        Optional measurement of time and memory resources used

    Returns
    -------

    r_data: Output data in the corresponding file as stated with file_name and

    References
    ----------

    [1] https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html#pandas.read_parquet
    [2] https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html

    """

    # present timestamp (converted from present unix time to datetime in seconds)
    ini_time = pd.to_datetime(time.time()*1e9)

    # Pandas Compatible formats --> For low data use
    in_p_opts = ['pd.dataframe', 'dataframe']

    # PySpark Compatible Formats --> For heavy data use
    in_s_opts = ['pyspark.dataframe', 'sql.dataframe']

    # -- (Pending) Cuda Compatible Formats --> For heavy algebraic & tensor computations using the GPU 
    # ------------ for processing, and keeping the compatibility with .parquet files back and forth.
    in_c_opts = ['cuda']

    # ---------------------------------------------------------------------------------------- WRITE FILE -- #

    if operation == 'write':

        # -- (Pending) specify/convert data types, to ensure cross-compatibility 
        # ------------ between pyarrow.parquet and pyspark.parquet

        # -- OUTPUT FORMAT -- #

        if out_format == 'parquet':

            # -- from python pandas.dataframe to pyarrow.parquet
            if (in_format.lower() in in_p_opts) | (isinstance(object_data, pd.DataFrame)):
                
                # Write file using pandas.to_parquet
                object_data = pa.Table.from_pandas(object_data)

            # -- from python dict to pyarrow.parquet
            elif in_format == 'dict':
                
                # -- (Pending) process of general conversion from this dict to 
                # ------------ any generic dict to generic schema

                # Store data 
                # pre_data = [i_values for i_values in object_data.values()]
                object_data = pa.Table.from_pydict(object_data)
            
            # -- from pyspark.pandas.dataframe to .parquet
            elif (in_format.lower() in in_s_opts):

                # -- (Pending) to add method for schema inference then pyspark object creation and 
                # ------------ finally create the file.
                
                pass
            
            # Write data as a .parquet using pyarrow
            pq.write_table(table=object_data, where=file_dir + file_name + '.parquet', 
                           compression='LZ4', flavor='spark')

            # -- (Pending) add lz4_raw codec for compression/decompression as lz4 is suggested to be avoided
            # ------------ by official doc
    
        # -- OUTPUT FORMAT -- #

        elif out_format == 'pickle':

            with open(file_dir + file_name + '.pickle', 'wb') as handle:
                pickle.dump(object_data, handle, protocol=pickle.HIGHEST_PROTOCOL)

        else: 
            print(f'out_format: ', out_format, ' is not a supported value')

        if memorytime:
                end_time = pd.to_datetime(time.time()*1e9)
                print(f'seconds to write the file: ', (end_time - ini_time).total_seconds())
                print(f'size of the written object in (Mbs): ', sys.getsizeof(object_data)/1e6)
        
        return {'message': 'done', 'fileName': file_name, 'fileDir': file_dir}
    
    # ----------------------------------------------------------------------------------------- READ FILE -- #

    elif operation == 'read':
        
        # -- INPUT FORMAT -- #

        if in_format == 'parquet':

            # -- Read pyarrow.parquet
            in_data = pq.read_table(source=file_dir + file_name + '.parquet')

            # -- (Pending) Read pyspark.parquet file as a PySpark.DataFrame object
            # ------------ in_data = spark.read.parquet(file_dir + file_name + '.parquet')

            # from pyarrow.parquet to pandas.DataFrame
            if out_format.lower() in in_p_opts:
                r_data = in_data.to_pandas()
            
            # -- from Parquet to Dict
            elif out_format.lower() == 'dict':
                r_data = in_data.to_pydict()
            
            # -- (Pending) read from pyarrow.parquet with lz4 / lz4_raw compression codec to spark formats
            # ------------ PySpark_Pandas.DataFrame or PySpark_SQL.DataFrame

            elif out_format.lower() in in_s_opts:
                pass

        # -- INPUT FORMAT -- #

        elif in_format == 'pickle':

            with open(file_dir + file_name + '.pickle', 'rb') as handle:
                r_data = pickle.load(handle)
        
        else: 
            print(f'in_format: ', in_format, ' is not a supported value')

        if memorytime:
            end_time = pd.to_datetime(time.time()*1e9)
            print(f'seconds to read file: ', (end_time - ini_time).total_seconds())
            print(f'size of the read object in (Mbs): ', sys.getsizeof(object_data)/1e6)

        return r_data
