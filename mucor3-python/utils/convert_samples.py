""" Files in a given .xslx sheet may contain either a column with 
Avatar IDs or a header. Either of the situations may occur; however,
the situations may not exist silumtaneously. Identifiers may also 
sometimes be linked to multiple Avatar IDs which will be represented 
as two ID seperated by a ';'.

Author: Kekananen, Charles Gregory
"""""
import sys
import pandas as pd
import argparse as ap
import numpy as np

def read_dataframe(path: str):
    """ Reads in a file object to a pandas' frame.
    file is assumed to have a single header row.
    Input:
        path - a string path to the file
    Output:
        out - pd.DataFrame
    """
    frame = None
    # Read in an xslx, tsv, or csv
    if path.endswith(".xlsx"):
        frame=pd.read_excel(path, sheet_name=0, header=0)
    elif path.endswith(".tsv"):
        frame=pd.read_csv(path, sep='\t')
    elif path.endswith(".csv"):
        frame=pd.read_csv(path)
    else:
        raise Exception("Unknown filetype")
    return frame

def expand_field_by_delimiter(column: pd.Series, delim: str):
    """ Splits file rows by a ';' delimiter
    Input:
        column - the pandas series to split
        delim - the delimiter to split on
    Output:
        pd.Series - input series converted to series of lists
            split by delim 
    """
    return column.str.split(delim)

def remap_expanded_value(x, mapping: dict, delim: str):
    """ remaps value in column that has been expanded with 
        expand_field_by_delimiter. Meant to be used in lambda
        i.e series.apply(lambda x: remap_expanded_value(x, mapping))
    Input:
        x - value in pandas series
        mapping - dictionary of sample/id remappings
        delim - the delimiter to join values on
    Output:
        pd.Series - series with remapped ids combined by delimiter 
    """
    if type(x) == list:
        return delim.join([str(mapping.get(e, None)) for e in x])
    else:
        return mapping.get(x, None)

def make_remapping(df: pd.DataFrame, from_col: str, to_col: str):
    """ creates dictionary for remapping ids from a dataframe and two column names
    Input:
        df - Key pd.DataFrame with id conversions
        from_col - column name of initial ids
        to_col - column name of remapped ids
    Output:
        dict - dictionary of id conversions
    """
    return dict(zip(df[from_col],df[to_col]))

def convert_dataframe_ids_in_rows(df: pd.DataFrame, mapping: dict, args: ap.ArgumentParser):
    """ converts ids in dataframe rows based on args and id conversion mapping 
    Input:
        df - data pd.DataFrame with ids to be converted
        mapping - dictionary of id conversions
        args - program arguments
    """
    for col in args.column:
        df[col] = expand_field_by_delimiter(df[col], args.delim) \
            .apply(lambda x: remap_expanded_value(x, mapping, args.delim))

def convert_dataframe_ids_in_columns(df: pd.DataFrame, mapping: dict, args: ap.ArgumentParser):
    """ converts ids in dataframe column names based on args and id conversion mapping 
    Input:
        df - data pd.DataFrame with ids to be converted
        mapping - dictionary of id conversions
        args - program arguments
    """
    remap = [mapping.get(x, None) for x in list(df.columns)]
    start_samples = -1

    # find first converted sample value
    for i, e in enumerate(remap):
        if e is not None:
            start_samples = i
            break

    # error if no ids converted    
    if start_samples == -1:
        raise Exception("No columns names were convertable with id mapping!")

    print("Assuming these columns are index cols and not ids: {}".format(list(df.columns)[0:start_samples]),file = sys.stderr)

    # check for unconverted ids
    for i,e in enumerate(remap[start_samples::]):
        if e is None:
            print("Warning: Column '{}' has no conversion".format(list(df.columns)[start_samples + i]),file = sys.stderr)

    df.columns = list(df.columns)[0:start_samples] + remap[start_samples::]


def convert_dataframe_ids(data_df: pd.DataFrame, key_df: pd.DataFrame, args: ap.ArgumentParser):
    """ converts ids in dataframe based on args and a dataframe containing id conversions
    Input:
        data_df - data pd.DataFrame with ids to be converted
        key_df - Key pd.DataFrame with id conversions
        args - program arguments
    """
    mapping = dict()
    for pair in args.mapping.split(","):
        vals = pair.split("=")
        if len(vals) != 2:
            raise Exception("id column pair not valid: {}".format(pair))
        f = vals[0]
        t = vals[1]
        mapping.update(make_remapping(key_df, f, t))

    if args.column is None:
        convert_dataframe_ids_in_columns(data_df, mapping, args)
    else:
        convert_dataframe_ids_in_rows(data_df, mapping, args)
    
# def expandFrameOnDelimiter(frame, column):
#     """ Splits file rows by a ';' delimiter
#     Input:
#         frame - the pandas frame to split
#         column - the column name the is being split by the ';'
#     Output:
#         expandedFrame - a frame with every value that contained 
#             a ';' character in the given column on new rows. 
#             This frame has all other values still present.
#     """
#     # Convert the string to a list by taking a dictionary of key-value pairs and
#     # unpacking into keyword arguments in a function call. The frame's internals 
#     # are thus split by the ';' character.
#     expandedFrame = frame
#     if not type(column) is list:
#         column = [column]
#     for c in column:
#         listColumn = expandedFrame.assign(**{c:frame[c].str.split(';')})
#         # Created a vectorized funcion which uses numpy's repeat. This function to 
#         # transform each element of a list-like to a row, replicating index values. 
#         # Indexes will be duplicated for the rows that are expanded.
#         # Todo - Convert to explode pandas function later since it's easier to read
#         expandedFrame = pd.DataFrame({
#                             col:np.repeat(listColumn[col].values, listColumn[c].str.len())
#                             for col in listColumn.columns.difference([c])
#                         }).assign(**{c:np.concatenate(listColumn[c].values)})[listColumn.columns.tolist()]
#     return expandedFrame

# def replaceValuesWithDelimiter(rawFile, column, accName, index):
#     """ Converts all ID on the same line. This retains the 
#     condensed format of a file if desired.
#     Input: 
#         rawFile - the file to convert
#         accName - nameof accession column
#         column - the name of the column to convert
#         index - The index file to get the conversion table from
#     Output:
#          A log file of Id that couldn't be converted
#          A converted pandas frame
#     """
#     # Compare the lists to the column in pandas.
#     accessions = list(index[accName])
#     ids = list(index[column])
#     rawIDs = list(rawFile[column])
#     # Create a log for the id not found.
#     failLog = open("idNotConverted.log", 'w')
#     failLog.write("id_or_accession_not_converted\n")

#     # Replace the values in the rawIDs list with the values in the index.
#     replacement=rawIDs
#     for i in range(0,len(ids)):
#         if not pd.isna(ids[i]) and not pd.isna(accessions[i]):
#             replacement=[x.replace(ids[i],accessions[i]) for x in replacement]
#         else:
#             failLog.write(str(ids[i])+'_'+str(accessions[i])+'\n')
    
#     # Double check everything was converted, should never fail.
#     assert(len(rawIDs) == len(replacement))
#     failLog.close()

#     rawFile[column] = replacement

#     return rawFile

def main():
    parser = ap.ArgumentParser()
    # Import the arguments from the user
    parser.add_argument("datafile",type=str, help="Input file containing Id to be converted")
    parser.add_argument("keyfile",type=str, help="File containing columns with id conversions")
    parser.add_argument("--column","-c", default=None, nargs="+", help="Name of column/s that the ids exist in the data file")
    parser.add_argument("--mapping","-m", required=True, help="Column name pairs in keyfile to create id conversion mapping from, e.g \"sample=accession\"")
    parser.add_argument("--delim","-d", default=";")
    args = parser.parse_args()
    
    # Read files intp pandas dataframe
    data = read_dataframe(args.datafile)
    keys = read_dataframe(args.keyfile)
    convert_dataframe_ids(data, keys, args)
    path = args.datafile
    if path.endswith(".xlsx"):
        data.to_excel(sys.stdout, index=False)
    elif path.endswith(".tsv"):
        data.to_csv(sys.stdout, index=False, sep="\t")
    elif path.endswith(".csv"):
        data.to_csv(sys.stdout, index=False)
    else:
        raise Exception("Unknown filetype")


if __name__ == "__main__":
    main()





