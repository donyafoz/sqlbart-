import zipfile
import shutil
import xlrd 
import os
import glob
import psycopg2
import re

# issue 1: we should unzip recursively (not sure if needed).

def empty_directory(folder):
    '''
    Empties a given directory.
    '''
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)


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
    """
    Given a file name, returns the month and year.
    """
   
    filename = file.split('/')[-1]
    if filename.startswith('R'):
        n = filename.split('_') 
        fullname= n[1].split('.')
        month_year = fullname[0]
        year = month_year[-4:]
        month = month_year[:-4].strip()
        return (normalize_month(month), year)
    
    else: 
        regex = r"(\w+)\s?(\d{4})"
        file_parts = re.findall(regex, filename)[0]
        return (normalize_month(file_parts[0]), file_parts[1])

def load_xls(file): 
    """
    Given an excel file, loads it and returns a list containing START, TERM, 
    EXITS, DAY_TYPE, MONTH, YEAR.
    """    
    content = xlrd.open_workbook(file)
    sheets = content.sheets()
    month, year = get_month_year_from_name(file)

    # (MONTH, YEAR, DAY_TYPE, START, TERM, RIDERS)
    file_data = []
    for sheet in sheets:
        sheet_name = normalize_sheet_name(sheet.name)

        if (sheet_name == 'unknown'):
            continue 

        ## FIX THIS
        col_number = sheet.ncols
        row_number = sheet.nrows

        for i in range(1, row_number):
            if sheet.cell_value(i, 0) == 'Entries':
                row_number = i 
                break

        for j in range(1, col_number):
            if sheet.cell_value(1, j) == 'Exits':
                col_number = j 
                break

        for i in range(2, row_number - 1):
            exitstation = sheet.cell_value(i, 0)
            for j in range(1, col_number - 1):
                startstation = sheet.cell_value(1, j)
                countppl = sheet.cell_value(i, j)
                # print(f"{startstation} - {exitstation} -> {countppl}")
                file_data.append((str(month), str(year), str(sheet_name), str(startstation)[:2], str(exitstation)[:2], str(countppl)))

    return file_data


def load_excel_files(tmpDir):
    """
    Given a direcory, it will load all excel files in the directory by walking 
    it and returns a list with the file paths.
    """

    # [
    #   (MONTH, YEAR, DAY_TYPE, START, TERM, RIDERS)
    # ]
    all_data = []
    for root, dirs, files in os.walk(tmpDir):
        for file in files:
            if file.split('.')[-1] != 'xls' and file.split('.')[-1] != 'xlsx':
                continue

            some_path = os.path.join(root, file)
            print("Processing file " + some_path)
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
    csv_file_name = tmpDir + "toLoad.csv"
    with open(csv_file_name, 'w') as csv_file:
        # Header
        csv_file.write("mon,yr,daytype,start,term,riders\n")

        for line_tuple in all_data:
            # print(line_tuple)
            csv_line = ",".join(list(line_tuple))
            csv_file.write(csv_line + "\n")

    return csv_file_name

def load_csv(csv_file_name, schema, table, SQLConn):
    """
    Given a CSV file name, schema, table and SQLConn, it will load the CSV
    to Postgres.
    """
    SQLCursor = SQLConn.cursor()
    SQLCursor.execute("""COPY %s.%s FROM '%s' CSV HEADER;""" % (schema, table, os.path.abspath(csv_file_name)))
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
        return

    empty_directory(tmpDir)
    unzip_all(dataDir, tmpDir)
    all_data = load_excel_files(tmpDir)

    csv_file_name = save_data_as_csv(all_data, tmpDir)
    load_csv(csv_file_name, schema, table, SQLConn)

## Testing

LCLconnR = psycopg2.connect("dbname='donya' user='donya' host='localhost' password=''")
ProcessBart('/tmp/', './data/', SQLConn=LCLconnR, schema='cls', table='bart')
