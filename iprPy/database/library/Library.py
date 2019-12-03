# Standard Python libraries
from pathlib import Path
import shutil
import tarfile

# http://www.numpy.org/
import numpy as np

# https://pandas.pydata.org/
import pandas as pd

# iprPy imports
from ...tools import aslist, iaslist
from .. import Database
from ... import load_record
from ...record import loaded as record_styles
from ... import libdir

class Library(Database):
    
    def __init__(self, host=None):
        """
        Initializes a connection to a local database.
        
        Parameters
        ----------
        host : str
            The host name (local directory path) for the database.
        """
        if host is None:
            host = libdir
        # Get absolute path to host
        host = Path(host).resolve()
        
        # Make the path if needed
        if not host.is_dir():
            host.mkdir(parents=True)
        
        # Pass host to Database initializer
        Database.__init__(self, host)
    
    def get_records(self, name=None, style=None, query=None, return_df=False,
                    **kwargs):
        """
        Produces a list of all matching records in the database.
        
        Parameters
        ----------
        name : str, optional
            The record name or id to limit the search by.
        style : str, optional
            The record style to limit the search by.
        query : str, optional
            A query str for identifying records.  Not supported by this style.
        return_df : bool, optional
            
        Returns
        ------
        list of iprPy.Records
            All records from the database matching the given parameters.
        """

        # Set default search parameters
        if style is None:
            style = list(record_styles.keys())
        else:
            style = aslist(style)
            for record_style in style:
                assert record_style in list(record_styles.keys()), f'unknown record style {record_style}'

        if query is not None:
            raise ValueError('query not supported by this style')
        
        df = []
        records = []
        # Iterate through all files matching style, name values
        for record_style in style:
            
            # Iterate over all names using glob
            if name is None:
                for record_file in Path(self.host, record_style).glob('*.json'):
                    record_name = record_file.stem
                    
                    # Load as an iprPy.Record object
                    record = load_record(record_style, record_name, record_file)
                    records.append(record)
                    df.append(record.todict(full=False, flat=True))
            else:
                # Iterate over given names
                for record_name in aslist(name):
                    record_file = Path(self.host, record_style, record_name+'.json')
                    if record_file.is_file():
                        
                        # Load as an iprPy.Record object
                        record = load_record(record_style, record_name, record_file)
                        records.append(record)
                        df.append(record.todict(full=False, flat=True))
        
        records = np.array(records)
        df = pd.DataFrame(df)
        
        if len(df) > 0:
            for key in kwargs:
                df = df[df[key].isin(aslist(kwargs[key]))]
        
        if return_df:
            return list(records[df.index.tolist()]), df.reset_index(drop=True)
        else:
            return list(records[df.index.tolist()])
    
    def get_records_df(self, name=None, style=None, query=None, full=True,
                       flat=False, **kwargs):
        """
        Produces a list of all matching records in the database.
        
        Parameters
        ----------
        name : str, optional
            The record name or id to limit the search by.
        style : str, optional
            The record style to limit the search by.
            
        Returns
        ------
        list of iprPy.Records
            All records from the database matching the given parameters.
        """
        
       # Set default search parameters
        if style is None:
            style = list(record_styles.keys())
        else:
            style = aslist(style)
            for record_style in style:
                assert record_style in list(record_styles.keys()), f'unknown record style {record_style}'

        if query is not None:
            raise ValueError('query not supported by this style')
        
        df = []
       # Iterate through all files matching style, name values
        for record_style in style:
            
            # Iterate over all names using glob
            if name is None:
                for record_file in Path(self.host, record_style).glob('*.json'):
                    record_name = record_file.stem

                    # Load as an iprPy.Record object
                    record = load_record(record_style, record_name, record_file)
                    df.append(record.todict(full=full, flat=flat))
            else:
                # Iterate over given names
                for record_name in aslist(name):
                    record_file = Path(self.host, record_style, record_name+'.json')
                    if record_file.is_file():
                        
                        # Load as an iprPy.Record object
                        record = load_record(record_style, record_name, record_file)
                        df.append(record.todict(full=full, flat=flat))
                    
        df = pd.DataFrame(df)
        
        if len(df) > 0:
            for key in kwargs:
                df = df[df[key].isin(aslist(kwargs[key]))]
        
        return df.reset_index(drop=True)

    def get_record(self, name=None, style=None, query=None, **kwargs):
        """
        Returns a single matching record from the database.
        
        Parameters
        ----------
        name : str, optional
            The record name or id to limit the search by.
        style : str, optional
            The record style to limit the search by.
            
        Returns
        ------
        iprPy.Record
            The single record from the database matching the given parameters.
        
        Raises
        ------
        ValueError
            If multiple or no matching records found.
        """
        
        # Get records
        record = self.get_records(name=name, style=style, query=None, **kwargs)
        
        # Verify that there is only one matching record
        if len(record) == 1:
            return record[0]
        elif len(record) == 0:
            raise ValueError(f'Cannot find matching record {name} ({style})')
        else:
            raise ValueError('Multiple matching records found')

    def get_tar(self, record=None, name=None, style=None, raw=False):
        """
        Retrives the tar archive associated with a record in the database.
        Issues an error if exactly one matching record is not found in the 
        database.
        
        Parameters
        ----------
        record : iprPy.Record, optional
            The record to retrive the associated tar archive for.
        name : str, optional
            .The name to use in uniquely identifying the record.
        style : str, optional
            .The style to use in uniquely identifying the record.
        raw : bool, optional
            If True, return the archive as raw binary content. If 
            False, return as an open tarfile. (Default is False)
        
        Returns
        -------
        tarfile or str
            The tar archive as an open tarfile if raw=False, or as a binary str if
            raw=True.
        
        Raises
        ------
        ValueError
            If style and/or name content given with record.
        """
        
        # Create Record object if not given
        if record is None:
            record = self.get_record(name=name, style=style)
        
        # Issue a ValueError for competing kwargs
        elif style is not None or name is not None:
            raise ValueError('kwargs style and name cannot be given with kwarg record')
        
        # Verify that record exists
        else:
            record = self.get_record(name=record.name, style=record.style)
        
        # Build path to record
        tar_path = Path(self.host, record.style, record.name+'.tar.gz')
        
        # Return content
        if raw is True:
            with open(tar_path, 'rb') as f:
                return f.read()
        else:
            return tarfile.open(tar_path)
            
    def build_refs(self, lib_directory=None, refresh=False, include=None):
        raise AttributeError('build_refs not allowed for Library style')
    
    def clean_records(self, run_directory=None, record_style=None, records=None):
        raise AttributeError('clean_records not allowed for Library style')
    
    def destroy_records(self, record_style=None):
        raise AttributeError('destroy_records not allowed for Library style')
    
    def prepare(self, run_directory, calculation, **kwargs):
        raise AttributeError('prepare not allowed for Library style')
    
    def runner(self, run_directory, orphan_directory=None, hold_directory=None):
        raise AttributeError('runner not allowed for Library style')