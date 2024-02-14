
# -- Load packages for this script

import sys
import os
import json
import requests
import urllib
import io
import urllib3
import pandas as pd
import numpy as np
import datetime as dt

from dateutil.relativedelta import *
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass

urllib3.disable_warnings()

# -- --------------------------------------------------------------- ABSTRACT CLASS (CONNECTION TEMPLATE) -- #
# -- ---------------------------------------------------------------------------------------------------- -- #
# -- ---------------------------------------------------------------------------------------------------- -- #

class AbstractConnection(metaclass=ABCMeta):

    """ Absctract class for connectivity objects 
    
    Atomic class to be used either single or multithread requests to the respective instantiated classes.
    
    """

    def __init__(self, source_id:str = '', source_params:dict = {}, connector=None,
                       **kwargs):

        """ Instantiation of a Connection Abstract Class """

        self.source_id = source_id
        self.source_params = source_params
        self.connector = connector
    
    # -- ----------------------------------------------------------------------------------------------- -- #
    def get_connector(self, n_clerks:int = 1):

        """ """
        
        self.connector = None

        # single connector : n_clerks = 1
        if n_clerks == 1:
            self.connector = rest.create_connector(api_token=self.source_params['api_token'],
                                                   auth_type=self.source_params['auth_type'])
        
        # multiple connectors : n_clerks > 1 
        elif (n_clerks > 1) & (n_clerks <= 20):
            self.connector = []
            [self.connector.append(rest.create_connector(api_token=self.source_params['api_token'],
                                   auth_type=self.source_params['auth_type'])) for _ in range(n_clerks)]
        elif n_clerks > 20:
            raise ValueError(f' No of jobs {n_clerks} is not supported, maximum is 20')

    # -- ----------------------------------------------------------------------------------------------- -- #
    def get_endpoint(self, call_params, endpoint_params, **kwargs):
        
        """
        This should be the method to get the single thread or multi-thread response from 
        the corresponding endpoint, according to the provided source

        Parameters
        ----------

        endpoint_params: dict, (default=None)
            Parameters as defined in the endpoint, should be the exact same name and with the content
            and data type as requested by the endpoint. 

        call_params: dict, (default=None)
            Parameters to guide the way of the call is going to be performed.

        kwargs: 
            To reserve the posibility of parsing named arguments, which are going to be necessary
            in order to implement variations for different sources

        Returns
        -------
        
        """

        # call_params['parallel'] = False
        verbose = kwargs['verbose'] if 'verbose' in list(kwargs.keys()) else False

        # 1. Instantiate a CommsCenter
        Comms = CommsCenter(capacity=call_params['capacity'],
                            client_id=self.source_id, client_src=self,
                            service={'endpoint_params': endpoint_params,
                                     'call_params': call_params})
    
        # 2. Define an execution plan
        Comms.define_plan(plan='clerk_per_telegram')
        
        # 3. Execute plan
        resulting_data = Comms.execute_plan(verbose=verbose)

        # 4. Release resources
        Comms = None
        
        return resulting_data


# -------------------------------------------------------------------------------------- --------------- -- #
# -------------------------------------------------------------------------------------- CLASS: BITQUERY -- #
# -------------------------------------------------------------------------------------- --------------- -- #

class Bitquery(AbstractConnection):
    
    """ Class for Bitquery.io interaction 
    
    Attributes: 
        
        source_id
        api_token
        api_endpoint

    Public methods:
        
        get_endpoint

    Private methods:

        __init__ : Instantiation of the class.
        __str__  : Print the name of the class and its base parameters.
        _parse_file : Read from a local file a GraphQL query
        _parse_errors : Catch and format errors from API calls

    """

    # --------------------------------------------------------------------- Instantiate with class-params -- #
    # --------------------------------------------------------------------------------------------------- -- #

    def __init__(self, source_id:str = 'bitquery', 
                       api_token:str = None,
                       api_endpoint:str = 'https://graphql.bitquery.io'):

        self.api_token = api_token
        self.api_endpoint = api_endpoint
        
        abstract_params = {'api_token': self.api_token, 'api_endpoint': self.api_endpoint,
                           'auth_type': 'X-API-KEY'}

        super(Bitquery, self).__init__(source_id=source_id,
                                       source_params=abstract_params,
                                       connector=None)

        self.connector = None

    # -- ------------------------------------------------------------------------------------------------ -- #

    def __str__(self):

        class_name = self.__class__.__name__
        message = "\Bitquery(source_params='{}')\n"

        return message.format(class_name, self.source_params)
       
    # -- ---------------------------------------------------------- INTERNAL: ERROR PARSING FROM RESPONSE -- #
    # -- ---------------------------------------------------------- ------------------------------------- -- #

    def _parse_errors(self, error_data:dict) -> None:

        total_errors = len(error_data['errors'])
        print("\n---------------------------------------------- ")
        print(f"A total of {total_errors} errors were found in the response")
        print("---------------------------------------------- \n")

        for i in range(0, len(error_data['errors'])):
            print(f"Error {str(i)}: Code: {error_data['errors'][i]['code']} \
                                    Message: {error_data['errors'][i]['message']}")
        
    # -- -------------------------------------------------------------------------- GET BITQUERY ENDPOINT -- #
    # -- -------------------------------------------------------------------------- --------------------- -- #

    def get_endpoint(self, query_content:str = None, query_variables:dict = None,
                           query_source:str = 'bitquery_queries') -> dict:
        """
        Get the data from the endpoint according to the parameters specified.
        
        Parameters
        ----------

        query_content: str, (default=None)
            raw content for a GraphQL-based query, as it has been read from Queries.py

        query_source: str, (default='file')
            'bitquery_queries': query used as id that matches with Queries.py a file in Files/Bitquery/

        query_variables: dict, (default=None)
            To be used with the specific query

        Returns
        -------

        response: object
            as the result of the call to the endpoint, which is a JSON parsed to a dict

        References
        ----------

        [1] GraphQL-based querys can be tested here: https://graphql.bitquery.io/ide/        

        """
                   
        if query_source == 'bitquery_queries':
            query = query_content
        
        else:
            raise ValueError(f"{query_source} is not valid, supported value is 'bitquery_queries' ")

        headers = {'Content-Type': 'application/json'}
        
        if self.api_token is not None:
            headers['X-API-KEY'] = '{}'.format(self.api_token)
        else: 
            # Raise error
            raise ValueError('api_token is not present as an attribute in the source object')

            # -- TODO: Log the error 

        # Build the payload
        payload = {'query': query, 'variables': query_variables}
        
        # -- TODO : use the error parser 
        response = requests.request("POST", self.api_endpoint, headers=headers, data=json.dumps(payload))

        return response

