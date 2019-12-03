"""
iprPy.highthroughput.settings
-----------------------------

Collects functions that interact with the .iprPy settings file allowing
database and run_directory information to be stored and easily accessed.
"""
# Standard Python libraries
from pathlib import Path

# https://github.com/usnistgov/DataModelDict
from DataModelDict import DataModelDict as DM

from .. import rootdir
from ..tools import screen_input

__all__ = ['list_databases', 'set_database', 'unset_database',
           'list_run_directories', 'load_run_directory', 'set_run_directory', 
           'unset_run_directory']

settingsfile = Path(rootdir, '.iprPy')

def load_settings():
    """Loads the .iprPy settings file."""
    
    # Load settings file if it exists
    if settingsfile.is_file():
        with open(settingsfile, 'r') as f:
            settings = DM(f)
    
    # Create a new settings file if it doesn't exist
    else:
        settings = DM()
        settings['iprPy-defined-parameters'] = DM()
        save_settings(settings)
    
    return settings

def save_settings(settings):
    """Saves to the .iprPy settings file."""
    
    # Verify settings is appropriate
    keys = list(settings.keys())
    try:
        assert len(keys) == 1
        assert keys[0] == 'iprPy-defined-parameters'
    except:
        raise ValueError('Invalid settings')
    
    # Save
    with open(settingsfile, 'w') as f:
        settings.json(fp=f, indent=4)
    
def list_databases():
    """
    Gives a list of the pre-defined database names
    
    Returns
    -------
    list : The names associated with the pre-defined databases.
    """
    # Get information from settings file
    settings = load_settings()
    
    # Extract and return the database names
    databases = settings['iprPy-defined-parameters'].aslist('database')
    return [database['name'] for database in databases]

def set_database(name=None, style=None, host=None, **kwargs):
    """
    Allows for database information to be defined in the settings file. Screen
    prompts will be given to allow any necessary database parameters to be
    entered.
    
    Parameters
    ----------
    name : str, optional
        The name to assign to the database. If not given, the user will be
        prompted to enter one.
    style : str, optional
        The database style associated with the database. If not given, the
        user will be prompted to enter one.
    host : str, optional
        The database host (directory path or url) where the database is
        located. If not given, the user will be prompted to enter one.
    """
    
    # Get information from the settings file
    settings = load_settings()
    
    # Ask for name if not given
    if name is None:
        name = screen_input('Enter a name for the database:')
    
    # Load database if it exists
    try:
        database_settings = settings.find('database', yes={'name':name})
    
     #Create new database entry if it doesn't exist
    except:
        database_settings = DM()
        settings['iprPy-defined-parameters'].append('database',
                                                    database_settings)
        database_settings['name'] = name
    
    # Ask if existing database should be overwritten
    else: 
        print('Database', name, 'already defined.')
        option = screen_input('Overwrite? (yes or no):')
        if option in ['yes', 'y']:
            pass
        elif option in ['no', 'n']: 
            return None
        else: 
            raise ValueError('Invalid choice')
    
    # Ask for style if not given
    if style is None: 
        style = screen_input("Enter the database's style:")
    database_settings['style'] = style
    
    #Ask for host if not given
    if host is None:
        host = screen_input("Enter the database's host:")
    database_settings['host'] = str(host)
    
    # Ask for additional kwargs if not given
    if len(kwargs) == 0:
        print('Enter any other database parameters as key, value')
        print('Exit by leaving key blank')
        while True:
            key = screen_input('key:')
            if key == '': 
                break
            kwargs[key] = screen_input('value:')
    for key, value in kwargs.items():
        database_settings.append('params', DM([(key, value)]))
    
    save_settings(settings)

def unset_database(name=None):
    """
    Deletes the settings for a pre-defined database from the settings file.
    
    Parameters
    ----------
    name : str
        The name assigned to a pre-defined database.
    """
    # Get information from settings file
    settings = load_settings()
    
    databases = settings['iprPy-defined-parameters'].aslist('database')
    
    # Ask for name if not given
    if name is None:
        
        if len(databases) > 0:
            print('Select a database:')
            for i, database in enumerate(databases):
                print(i+1, database['name'])
            choice = screen_input(':')
            try:
                choice = int(choice)
            except:
                name = choice
            else:
                name = databases[choice-1]['name']
        else:
            print('No databases currently set')
            return None
    
    # Verify listed name exists
    else:
        try:
            settings.find('database', yes={'name':name})
        except:
            raise ValueError(f'Database {name} not found')
    
    print(f'Database {name} found')
    test = screen_input('Delete settings? (must type yes):')
    if test == 'yes':
        if len(databases) == 1:
            del(settings['iprPy-defined-parameters']['database'])
        else:
            new = DM()
            for database in databases:
                if database['name'] != name:
                    new.append('database', database)
            settings['iprPy-defined-parameters']['database'] = new['database']
            
        save_settings(settings)
        print(f'Settings for database {name} successfully deleted')

def list_run_directories():
    """
    Gives a list of the pre-defined run_directory names
    
    Returns
    -------
    list : The names associated with the pre-defined run_directories.
    """
    # Get information from settings file
    settings = load_settings()
    
    # Extract and return the run_directory names
    run_directories = settings['iprPy-defined-parameters'].aslist('run_directory')
    return [run_directory['name'] for run_directory in run_directories]

def load_run_directory(name=None):
    """
    Loads a pre-defined run_directory from the settings file.
    
    Parameters
    ----------
    name : str
        The name assigned to a pre-defined run_directory.
    
    Returns
    -------
    str
        The path to the identified run_directory.
    """
    # Get information from settings file
    settings = load_settings()
    
    run_directories = settings['iprPy-defined-parameters'].aslist('run_directory')
    
    # Ask for name if not given
    if name is None:
        if len(run_directories) > 0:
            print('Select a run_directory:')
            for i, run_directory in enumerate(run_directories):
                print(i+1, run_directory['name'])
            choice = screen_input(':')
            try:
                choice = int(choice)
            except:
                name = choice
            else:
                name = run_directories[choice-1]['name']
        else:
            print('No run_directories currently set')
            return None
    
    try:
        run_directory_settings = settings.find('run_directory', yes={'name':name})
    except:
        raise ValueError(f'run_directory {name} not found')
    
    # Extract parameters
    return Path(run_directory_settings['path'])

def set_run_directory(name=None, path=None):
    """
    Allows for run_directory information to be defined in the settings file.
    
    Parameters
    ----------
    name : str, optional
        The name to assign to the run_directory.  If not given, the user will
        be prompted to enter one.
    path : str, optional
        The directory path for the run_directory.  If not given, the user will
        be prompted to enter one.
    """
    
    # Get information from the settings file
    settings = load_settings()
    
    # Ask for name if not given
    if name is None:
        name = screen_input('Enter a name for the run_directory:')
        
    # Load run_directory if it exists
    try: 
        run_directory_settings = settings.find('run_directory',
                                               yes={'name':name})
    
    # Create new run_directory entry if it doesn't exist
    except: 
        run_directory_settings = DM()
        settings['iprPy-defined-parameters'].append('run_directory', run_directory_settings)
        run_directory_settings['name'] = name
    
    # Ask if existing run_directory should be overwritten    
    else: 
        print(f'run_directory {name} already defined.')
        option = screen_input('Overwrite? (yes or no):')
        if option in ['yes', 'y']:
            pass
        elif option in ['no', 'n']:
            return None
        else:
            raise ValueError('Invalid choice')
    
    # Ask for path if not given
    if path is None:
        path = screen_input("Enter the run_directory's path:")
    run_directory_settings['path'] = Path(path).resolve().as_posix()
    
    save_settings(settings)

def unset_run_directory(name=None):
    """
    Deletes the settings for a pre-defined run_directory from the settings
    file.
    
    Parameters
    ----------
    name : str
        The name assigned to a pre-defined run_directory.
    """
    
    # Get information from settings file
    settings = load_settings()
    run_directories = settings['iprPy-defined-parameters'].aslist('run_directory')
    
    # Ask for name if not given
    if name is None:
        if len(run_directories) > 0:
            print('Select a run_directory:')
            for i, run_directory in enumerate(run_directories):
                print(i+1, run_directory['name'])
            choice = screen_input(':')
            try:
                choice = int(choice)
            except:
                name = choice
            else:
                name = run_directories[choice-1]['name']
        else:
            print('No run_directories currently set')
            return None
    
    # Verify listed name exists
    else:
        try:
            settings.find('run_directory', yes={'name':name})
        except:
            raise ValueError(f'run_directory {name} not found')
    
    print(f'run_directory {name} found')
    test = screen_input('Delete settings? (must type yes):')
    if test == 'yes':
        if len(run_directories) == 1:
            del(settings['iprPy-defined-parameters']['run_directory'])
        else:
            new = DM()
            for run_directory in run_directories:
                if run_directory['name'] != name:
                    new.append('run_directory', run_directory)
            settings['iprPy-defined-parameters']['run_directory'] = new['run_directory']
        
        save_settings(settings)
        print(f'Settings for run_directory {name} successfully deleted')