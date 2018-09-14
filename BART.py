import zipfile
import xlrd 
import os
import glob
import psycopg2

def unzip_all(current_directory, new_directory):
    '''
    Goes through directories and subdirectories and unzips files.
    '''
    for filename in glob.glob(os.path.join(current_directory, '*.zip')):
        f = zipfile.ZipFile(filename, 'r')
        f.extractall(new_directory)
        f.close()
    return 


def normalize_sheet_name(name):
    '''
    Given the name of an excel sheet, it returns the normalized name 
    '''
    name = name.lower()
    # if name == 'Weekday OD' or name == 'Wkdy Adj OD':
    #     return 'unknown'

    if name.startswith('w'):
        return 'Weekday'
    
    elif name.startswith('sa'):
        return 'Saturday'

    elif name.startswith('su'): 
        return 'Sunday'
    
    else:
        return 'unknown' 

def normalize_month(month):
    '''
    Given a specific month, it will return the number associated with that 
    month.
    '''
    month_dict = {
        'january': 1,
        'february': 2,
        'march': 3,
        'april': 4,
        'may': 5,
        'june': 6,
        'july': 7,
        'august': 8,
        'september': 9,
        'october': 10,
        'november': 11,
        'december': 12
    }

    m = month.lower()
    return month_dict[m]

def get_month_year_from_name(file):
    '''
    Given a file name, returns the month and year.
    '''
    # fix for 2011 2008 --> make sure the names are consistent 
    # add conditions to this 

    # 2008 and 2009: 
   
    if file.startswith('R'):
        n = name.split('_') 
        fullname= n[1].split('.')
        month_year = fullname[0]
        year = month_year[-4:]
        month = month_year[:-4].strip()
        return (month, year)
    
    else: 
        file_parts = file.split()
        return (normalize_month(file_parts[0]), file_parts[1])

def load_xls(file): 
    '''
    Given an excel file, loads it and returns a list containing START, TERM, 
    EXITS, DAY_TYPE, MONTH, YEAR.
    '''    
    content = xlrd.open_workbook(file)
    sheets = content.sheets()
    month, year = get_month_year_from_name(file)

    # (START, TERM, EXITS, DAY_TYPE, MONTH, YEAR)
    file_data = []
    for sheet in sheets:
        sheet_name = normalize_sheet_name(sheet.name())

        if (sheet_name == 'unknown'):
            continue;

        ## FIX THIS 
        for row in sheet.nrows():
            for col in sheet.nrows():
                file_data.append((month, year, sheet_name, col, row, float(sheet[row][col])))
                # int() of each cell?

    return file_data


def load_excel_files(tmpDir):
    '''
    Given a direcory, it will load all excel files in the directory by walking 
    it and returns a list with the file paths.
    '''

    # [
    #   (START, TERM, EXITS, DAY_TYPE, MONTH, YEAR),
    # ]
    all_data = []
    for root, dirs, files in os.walk(tmpDir):
        for file in files:
            some_path = os.path.join(root, file)
            file_data = load_xls(some_path)
            all_data += file_data

    return all_data

def create_table(schema, table, SQLConn):
    """
    Given a schema, table and SQLConn, creates table.
    """
    try:
        SQLCursor = SQLConn.cursor()
        SQLCursor.execute("""
          CREATE TABLE %s.%s
          (
              mon int
              , yr int
              , daytype varchar(15)
              , start varchar(2)
              , term varchar(2)
              , riders float
          );""" % (schema, table))
        SQLConn.commit()

        return True
    except:
        return False

def save_data_as_csv(all_data, tmpDir):
    """
    Given data and a directory, it will save data as a CSV.
    """
    csv_file_name = tmpDir + "/toLoad.csv"
    with open(csv_file_name, 'w') as csv_file:
        # Header
        csv_file.write("mon,yr,daytype,start,term,riders")

        for tuple in all_data:
            csv_line = ",".join(list(tuple))
            csv_file.write(csv_line)

    return csv_file_name

def load_csv(csv_file_name, schema, table, SQLConn):
    """
    Given a CSV file name, schema, table and SQLConn, it will load the CSV
    to Postgres.
    """
    SQLCursor = SQLConn.cursor()
    SQLCursor.execute("""COPY %s.%s FROM '%s' CSV;"""
          % (schema, table, csv_file_name))
    SQLConn.commit()

    return


def ProcessBart(tmpDir, dataDir, SQLConn=None, schema='cls', table='bart'):
    """
    Given a the current and previous directory, SQLConn, the name of the schema 
    and table, it will unzip the files, load the excel files and save them as CSV
    and load the CSV to Postgres.
    """
    if (create_table(schema, table, SQLConn) == False):
        print("Table already exists")
        # return

    unzip_all(dataDir, tmpDir)
    all_data = load_excel_files(tmpDir)

    csv_file_name = save_data_as_csv(all_data)
    load_csv(csv_file_name, schema, table, SQLConn)

## Testing

LCLconnR = psycopg2.connect("dbname='donya' user='donya' host='localhost' password=''")
ProcessBart('./tmp/', './data/', SQLConn=LCLconnR, schema='cls', table='bart')
