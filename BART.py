import zipfile
import xlrd 
import os
import glob

def unzip_all(current_directory, new_directory):
    '''
    Goes through directories and subdirectories and unzips files.
    '''
    for filename in glob.glob(os.path.join(current_directory, '*.zip')):
        f = zipfile.ZipFile(filename, 'r')
        f.extractall(new_directory)
        zipfile.close()
    return 


def normalize_sheet_name(name):
    if name == 'Weekday OD' or name == 'Wkdy Adj OD':
        #...
    else
        return 'unkown' 

def normalize_month(month):
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

    return month_dict[month.lower()]

def get_month_year_from_name(file):
    file_parts = file.split()
    return (normalize_month(file_parts[0]), file_parts[1])

def load_xls(file):     
    content = xlrd.open_workbook(file)
    sheets = content.sheets()
    month, year = get_month_year_from_name(file)

    # (START, TERM, EXITS, DAY_TYPE, MONTH, YEAR)
    file_data = []
    for sheet in sheets:
        sheet_name = normalize_sheet_name(sheet.name())

        if (sheet_name == 'unknown'):
            continue;

        ## FIXME
        for row in sheet.rows():
            for col in sheet.cols():
                file_data.append((month, year, sheet_name, col, row, sheet[row][col]))

    return file_data


def load_excel_files(tmpDir):

    # [
    #   (START, TERM, EXITS, DAY_TYPE, MONTH, YEAR),
    # ]
    all_data = []
    for root, dirs, files in os.walk(tmpDir):
        for file in files:
            file_data = load_xls(root + '/' + file)
            all_data += file_data

    return all_data

def create_table(schema, table, SQLConn):
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
          );""" %(schema, table))
        SQLCursor.commit()

        return True
    except:
        return False

def save_data_as_csv(all_data, tmpDir):
    csv_file_name = tmpDir + "/toLoad.csv"
    with open(csv_file_name, 'w') as csv_file:
        # Header
        csv_file.write("mon,yr,daytype,start,term,riders")

        for tuple in all_data:
            csv_line = ",".join(list(tuple))
            csv_file.write(csv_line)

    return csv_file_name

def load_csv(csv_file_name, schema, table, SQLConn):
    SQLCursor = SQLConn.cursor()
    SQLCursor.execute("""COPY %s.%s FROM '%s' CSV;"""
          % (schema, table, csv_file_name))
    SQLConn.commit()

    return


def ProcessBart(tmpDir, dataDir, SQLConn=None, schema='cls', table='bart'):
    if (create_table(schema, table, SQLConn) == False):
        print("Table already exists")
        return

    unzip_all(dataDir, tmpDir)
    all_data = load_excel_files(tmpDir)

    csv_file_name = save_data_as_csv(all_data)
    load_csv(csv_file_name, schema, table, SQLConn)

## Testing
LCLconnR = psycopg2.connect("dbname='donya' user='donya' host='localhost' password=''")
ProcessBart('./tmp/', './data/', SQLConn=LCLconnR, schema='cls', table='bart')
