
# -- import packages for this script
import pandas as pd
import json
from dataclasses import dataclass
from abc import ABCMeta

# -- Load other scripts
from Processing import dataIO as io

# -- ---------------------------------------------------------------------------------------------------- -- #
# -- --------------------------------------------------------------------------- ABSTRACT CLASS: PROTOCOL -- #
# -- ---------------------------------------------------------------------------------------------------- -- #

class AbstractProtocol(metaclass=ABCMeta):

    """ Class to store, organize, describe, transform and plot a Blockchain's related data. """
    
    def __init__(self, class_id:str = None, protocol_id:str = None, symbol_id:str = None,
                       network_id:str = None, data:dict = None, logs:bool = None):
        
        """ Instantiation of the Protocol Class """
        
        self.class_id = class_id        # str(16): The class id for internal use
        self.network_id = network_id    # str(16): The blockchain name (network)
        self.protocol_id = protocol_id  # str(16): The protocol's name (lowercase, no spaces, as internal)
        self.symbol_id = symbol_id      # str(16): The symbol's id
        
        self.data = data                # dict: With any data that is being gathered and stored in self
        self.logs = logs                # None; Logs object to log the activity within the class


    # -- ------------------------------------------------------------------------------ GET DATA FROM API -- #
    # -- ------------------------------------------------------------------------------ ----------------- -- #
    
    def get_data(self, source:object = None, query_variables:dict = None, query_source:str = 'catalog',
                       query_content:str = '', **kwargs) -> dict:
        """
        An atomic call for data retrieval
        
        Uses the provided source and querying paramerters, will return a response with 'raw' content
        and response code and message. In case of error, content is None and the response code and message.

        Parameters
        ----------

        source: object, (default=None)
            The source object to be used for getting the data

        query: str, (default=None)
            With the route/filename, or, just the name of a pre-formulated query (Files/GraphQL/filename)
        
        query_variables: dict, (default=None)
            A dictionary with parameters as needed for the query

        query_source: str, (default='file')
            In order to specify the way of getting the query into the call, options are:
            'file' : specify the name of the file contained in Files/GraphQL/
            'raw' : provide the query as a string with tripple quotes

        Returns
        -------

        r_get_data : dict
            A dict with raw response from the endpoint calling, has the following structure:

            'response' = {'code': int, 'message': ''}
            'content' = {...}

        """

        # -- bitquery.io -- #

        if source.source_id == 'bitquery':
           
            response = source.get_endpoint(query_content=query_content, query_variables=query_variables,
                                           query_source=query_source)

            # -- TODO : Generic results parsing criteria

            try:
                content_response = json.loads(response.content)
                return content_response

            except:
                print(response)
                content_response = None               

        else:
            print(f'Source {source.source_id} not supported')
        

# -- ------------------------------------------------------------------------------- -------------------- -- #
# -- ------------------------------------------------------------------------------- CLASS: LIQUIDITYPOOL -- #
# -- ------------------------------------------------------------------------------- -------------------- -- #

@dataclass
class LiquidityPool(AbstractProtocol):
    """

    A subclass from the AbstractProtocol class template. The LiquidityPool class is used to store, 
    organize, describe, plot and transform DEX-based Liquidity Pool data.

    """
    
    # --------------------------------------------------------------------- Instantiate with class-params -- #
    # --------------------------------------------------------------------------------------------------- -- #

    def __init__(self, network_id:str = None, protocol_id:str = None, data:dict = None, logs:bool = None):
        
        """ Instantiation """

        self.network_id = network_id
        self.protocol_id = protocol_id

        self.data = data
        self.logs = logs

        super(LiquidityPool, self).__init__(class_id='LiquidityPool', network_id=network_id, 
                                            protocol_id=protocol_id, data=None, logs=None)

    # ----------------------------------------------------------------------------------- print(Tinybird) -- #
    # -- ------------------------------------------------------------------------------------------------ -- #
    
    def __str__(self):

        class_name = self.__class__.__name__
        message = "\n{}(class_id='{}', protocol_id='{}', ini_ts='{}', end_ts='{}')\n"

        return message.format(class_name, self.class_id, self.protocol_id)

    # ---------------------------------------------------------------------------------------- get_data() -- #
    # -- ------------------------------------------------------------------------------------------------ -- #
    
    def get_data(self, source:object = None, endpoint_params: dict = None, call_params:dict = None,
                       verbose:bool = True) -> list:
        """
        
        Calls for sync or async data retreival 

        Parameters
        ----------

        source: object, (default=None)
            with the source class to get the data from

        endpoint_params: dict, (default={})
            Dictionary with the parameters needed for each particular endpoint, these are going to be
            parsed stratight through as they are declared.
        
        call_params: dict, (default=None)
            with the parameters for the controlling of the call to the REST API according to the 
            provided connection
        
        verbose: bool, (default: True)
            To print helping messages in the console, for control and debugging.

        Returns
        -------

        response: list
            with the resulting responses from the call
        
        """

        if call_params['parallel']:
            
            pass
        
        else:
        
            # Single thread Non-batched API call

            # -- TODO : Yet to be decided a better value for batched calls
            batch_size = '8H'
            r_dextrades = []
            l_responses = []

            l_timestamps = list(pd.date_range(start=endpoint_params['query_variables']['ini_ts'],
                                              end=endpoint_params['query_variables']['end_ts'],
                                              freq=batch_size))
            if verbose: 
                print(f'Collecting data in batches of: {batch_size}')
            
            for i in range(len(l_timestamps)-1):

                endpoint_params['query_variables']['ini_ts'] = str(l_timestamps[i].isoformat())
                endpoint_params['query_variables']['end_ts'] = str(l_timestamps[i+1].isoformat())

                if verbose:
                    print(f'from: {str(l_timestamps[i])} to: {str(l_timestamps[i+1])}')

                data_response = super().get_data(source=call_params['source'], 
                                                 query_variables=endpoint_params['query_variables'],
                                                 query_content=call_params['query_content'],
                                                 query_source=call_params['query_source'])
                
                # concatenate responses in list
                l_responses.append(data_response)              

        return l_responses

    # ------------------------------------------------------------------------------- GET TOKEN TRANSFERS -- #
    # ------------------------------------------------------------------------------- ------------------- -- #

    def get_tokentransfers(self, source:object = None, 
                                 token_address:str = None,
                                 sender_address:str = None,
                                 receiver_address:str = None,
                                 ini_ts:str = None, end_ts:str = None, **kwargs) -> pd.DataFrame():
        """
        """

        p_ini_ts = pd.to_datetime(ini_ts).strftime('%Y-%m-%dT%H:%M:%S')
        p_end_ts = pd.to_datetime(end_ts).strftime('%Y-%m-%dT%H:%M:%S')

        query_variables = {"token_address": token_address,
                           "sender_address": sender_address,
                           "receiver_address": receiver_address, 
                           "ini_ts": p_ini_ts,
                           "end_ts": p_end_ts}
        
        query_source = kwargs['query_source'] if 'query_source' in list(kwargs.keys()) else None
        query_content = kwargs['query_content'] if 'query_content' in list(kwargs.keys()) else None

        e_params = {'query_variables': query_variables}
        c_params = {'source': source, 'query_source': query_source, 'query_content': query_content,
                    'parallel': False}

        data_responses = self.get_data(source=source, endpoint_params=e_params, call_params=c_params)
        r_formated_responses = self._post_process_response(l_responses=data_responses,
                                                            expected_fields=['ethereum', 'transfers'],
                                                            process='tokentransfers')

        return data_responses
    
    # ----------------------------------------------------------------------------------- GET TOP WALLETS -- #
    # ----------------------------------------------------------------------------------- --------------- -- #

    def get_topwallets(self, source:object = None, token_address:str = None,
                             ini_ts:str = None, end_ts:str = None, **kwargs) -> pd.DataFrame():
        """
        """

        p_ini_ts = pd.to_datetime(ini_ts).strftime('%Y-%m-%dT%H:%M:%S')
        p_end_ts = pd.to_datetime(end_ts).strftime('%Y-%m-%dT%H:%M:%S')

        query_variables = {"token_address": token_address, "limit": 20,
                           "ini_ts": p_ini_ts, "end_ts": p_end_ts}

        query_source = kwargs['query_source'] if 'query_source' in list(kwargs.keys()) else None
        query_content = kwargs['query_content'] if 'query_content' in list(kwargs.keys()) else None
        resample = kwargs['resample'] if 'resample' in list(kwargs.keys()) else None
        period = kwargs['period'] if 'period' in list(kwargs.keys()) else None

        e_params = {'query_variables': query_variables}
        c_params = {'source': source, 'query_source': query_source, 'query_content': query_content,
                    'parallel': False}
        
        data_responses = self.get_data(source=source, endpoint_params=e_params, call_params=c_params)

        r_formated_responses = self._post_process_response(l_responses=data_responses,
                                                           expected_fields=['ethereum', 'transfers'],
                                                           resample=resample, period=period,
                                                           process='topwallets')

        return r_formated_responses

    # ------------------------------------------------------------------------------------ GET DEX TRADES -- #
    # ------------------------------------------------------------------------------------ -------------- -- #

    def get_dextrades(self, source:object = None, base_token:str = None, quote_token:str = None,
                            ini_ts:str = None, end_ts:str = None, **kwargs) -> pd.DataFrame():

        """ 
        Get the trades information of a DEX, according to the query and its variables

        Paramaters
        ----------

        source:object, (default=None)
            The source object from where to get the data, the options should be classes that are present
            in the Sources.py script. Currently are:       
            'bitquery': from the REST API provided by bitquery.io
        
        base_token:str, (default=None)
            The token address for the base of the symbol
        
        quote_token:str, (default=None)
            The token address for the quote of the symbol
        
        ini_ts:str, (default=None)
            Initial timestamp for the query
        
        end_ts:str, (default=None)
            Final timestamp for the query
                
        query_source: str, (optional, default='raw')
            Source from where to get the query content parsed, the options are: 
            'file' : To indicate the file route
            'raw' : To parse the content of the query directly in the parameter
        
        query_content: str, (optional, default='')
            The actual content of the query, only usuable if query_source == 'raw'

        resample: bool, (optional, default=False)
            Whether to perform a resampling of the data or not
        
        period:str, (optional, default='1H')
            Period for the resampling of the data, depends on resample

        Returns
        -------

        r_data: either as a dataframe or dict, and also could be resampled, if 
        resample
               
        
        """

        # -- TODO : Add Single thread batched and Multithread batched calls to get_data

        p_ini_ts = pd.to_datetime(ini_ts).strftime('%Y-%m-%dT%H:%M:%S')
        p_end_ts = pd.to_datetime(end_ts).strftime('%Y-%m-%dT%H:%M:%S')

        # ERROR CHECK 1: Different timestamps
        if p_end_ts == p_ini_ts:
            raise ValueError(f'Initial and ending timestamps should be different: {p_ini_ts} != {p_end_ts}')

        query_variables = {"base_address": base_token, "quote_address": quote_token,
                           "protocol_id": self.protocol_id, "ini_ts": p_ini_ts, "end_ts": p_end_ts}

        query_source = kwargs['query_source'] if 'query_source' in list(kwargs.keys()) else None
        query_content = kwargs['query_content'] if 'query_content' in list(kwargs.keys()) else None
        
        resample = kwargs['resample'] if 'resample' in list(kwargs.keys()) else None
        period = kwargs['period'] if 'period' in list(kwargs.keys()) else None

        e_params = {'query_variables': query_variables}
        c_params = {'source': source, 'query_source': query_source, 'query_content': query_content,
                    'parallel': False, 'clerks': 1}
        
        data_responses = self.get_data(source = source, endpoint_params = e_params, call_params = c_params)
        
        r_formated_responses = self._post_process_response(l_responses=data_responses,
                                                            expected_fields=['ethereum', 'dexTrades'],
                                                            resample=resample, period=period,
                                                            process='dextrades')

        return pd.concat(r_formated_responses, axis=0)

    # -- ------------------------------------------------------------------------------------------------ -- #
    def _post_process_response(self, l_responses:list = None, expected_fields:object = None,
                                     resample:bool = False, period:str = '1H', **kwargs) -> list():
        """
        Post processing the response

        Parameters
        ----------

        l_responses: list, (default=None)
            With the responses to be iterated and processed 

        expected_fields: list, (default=None)
            With the name of the expected fields to be accessed 

        resample: bool, (default=False)
            Whether to perform a resampling to the fields

        period: str, (default='1H')
            In case resample == True, the period to be used for the resampling

        Returns
        -------
        
        r_responses: list
            With the post-processed responses according to the input parameters

        """
        
        # Init counter and return object
        i = 0
        r_responses = []

        process = kwargs['process'] if 'process' in list(kwargs.keys()) else None

        for i_response in l_responses:
            
            i += 1
            # Get the response data from the list
            response_data = i_response['data'][expected_fields[0]][expected_fields[1]]

            # Safe backup for empty response
            if response_data is None:
                print(f'for the current batch {i}, response_data delivered None')
                print('\n', i_response['errors'][0]['message'])

                # Continue with the next element in the input list
                continue
            
            # Show expected fields
            elif len(response_data) == 0:
                print(f'for the current batch {i}, the expected fields, \
                        [{expected_fields[0]}][{expected_fields[0]}] delivered empty response: \n')

                continue
            
            # Raw processing
            raw_sr = i_response['data'][expected_fields[0]][expected_fields[1]]
            
            if process == 'dextrades':
                r_responses.append(self._format_dextrades(raw_sr, resample, period))
            
            elif process == 'topwallets':
                r_responses.append(self._format_topwallets(raw_sr, resample, period))
        
        return r_responses

    # -- ------------------------------------------------------------------------------------------------ -- #
    def _format_topwallets(self, raw_responses:list = None, resample:bool = False,
                                period:str = '1H') -> pd.DataFrame():

        dc_data = {'sender_address': [], 'sender_annotation': [],
                   'receiver_address': [], 'receiver_annotation': [],
                   'currency': [], 'amount': [], 'count': [], 
                   'receiver_count': []}

        for i_response in raw_responses:
            # i_response = raw_responses[0]

            dc_data['sender_address'].append(i_response['sender']['address'])
            dc_data['sender_annotation'].append(i_response['sender']['annotation'])

            dc_data['receiver_address'].append(i_response['receiver']['address'])
            dc_data['receiver_annotation'].append(i_response['receiver']['annotation'])

            dc_data['currency'].append(i_response['currency']['symbol'])
            dc_data['amount'].append(i_response['amount'])
            dc_data['count'].append(i_response['count'])
            dc_data['receiver_count'].append(i_response['receiver_count'])

        return dc_data

    # -- ------------------------------------------------------------------------------------------------ -- #
    def _format_dextrades(self, raw_responses:list = None, resample:bool = False,
                                period:str = '1H') -> pd.DataFrame():
        """
        Format the response in order to provide a resampled version of it

        Parameters
        ----------
        
        raw_responses: list, (default=None)
            object containing the list of responses from a get_endpoint related process

        resample: bool, (default=False)
            Whether to perform a resampling to the fields

        period: str, (default='1H')
            In case resample == True, the period to be used for the resampling

        Returns
        -------

        r_data: pd.DataFrame
            With the resampled values, 

        """
        
        dc_data = {'block_ts': [], 'block_height': [], 'side': [], 'price': [], 
                   'gas': [], 'gas_price': [], 'gas_value': [],
                   'base_volume': [], 'quote_volume': [], 
                   'maker_address': [], 'taker_address': []}
       
        for i_trade in raw_responses:
            
            dc_data['block_ts'].append(i_trade['block']['timestamp']['iso8601'])
            dc_data['block_height'].append(i_trade['block']['height'])

            dc_data['side'].append(i_trade['side'])
            
            dc_data['price'].append(1/i_trade['price'] if i_trade['side'] == 'SELL' else i_trade['price'])
            
            dc_data['base_volume'].append(i_trade['baseAmount'])
            dc_data['quote_volume'].append(i_trade['quoteAmount'])

            dc_data['gas'].append(i_trade['transaction']['gas'])
            dc_data['gas_price'].append(i_trade['transaction']['gasPrice'])
            dc_data['gas_value'].append(i_trade['transaction']['gasValue'])

            dc_data['maker_address'].append(i_trade['maker']['address'])
            dc_data['taker_address'].append(i_trade['taker']['address'])

        r_data = pd.DataFrame(dc_data, index=pd.to_datetime(dc_data['block_ts']) )
        r_data.sort_index(ascending=True, inplace=True)
        
        # ini_ts = r_data.index[0]
        # end_ts = r_data.index[-1]
        
        if resample:
            
            resampled_df_data = pd.DataFrame()
            count_sells = r_data['side'][r_data['side']=='SELL'].resample(period).count()
            count_buys = r_data['side'][r_data['side']=='BUY'].resample(period).count()

            try:
                trades_count_imb = count_sells / (count_sells + count_buys)
            except ZeroDivisionError():
                trades_count_imb = 0

            resampled_df_data['average_price'] = r_data['price'].resample(period).mean()
            resampled_df_data['last_price'] = r_data['price'].resample(period).last()
            
            resampled_df_data['buy_trades_count'] = count_buys
            resampled_df_data['sell_trades_count'] = count_sells
            resampled_df_data['trades_count_imb'] = trades_count_imb
            resampled_df_data['base_volume'] = r_data['base_volume'].resample(period).sum()
            resampled_df_data['quote_volume'] = r_data['quote_volume'].resample(period).sum()

            resampled_df_data['mean_gas'] = r_data['gas'].resample(period).mean()
            resampled_df_data['mean_gas_price'] = r_data['gas_price'].resample(period).mean()
            resampled_df_data['mean_gas_value'] = r_data['gas_value'].resample(period).mean()

            # TODO : Validate response formats for unit testing
            
            # range_index = pd.date_range(start=ini_ts, end=end_ts, freq=period)
            # complete_df_data = pd.DataFrame(data=[0]*len(range_index), index=range_index)
            
            # r_data = complete_df_data.join(resampled_df_data).dropna(axis=0)
            # r_data = r_data[resampled_df_data.columns]

            r_data = resampled_df_data
            
        return r_data