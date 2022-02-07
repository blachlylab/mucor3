""" Files in a given .xslx sheet may contain either a column with 
Avatar IDs or a header. Either of the situations may occur; however,
the situations may not exist silumtaneously. Identifiers may also 
sometimes be linked to multiple Avatar IDs which will be represented 
as two ID seperated by a ';'.

Author: Kekananen
"""""
import sys
import pandas as pd
import argparse as ap
import numpy as np

def __readInFile(path):
    """ Reads in a file object to a pandas' frame.
    file is assumed to have a single header row.
    Input:
        path - a string path to the file
    Output:
        out - pandas frame with header
    """
    frame = None
    # Read in an xslx, tsv, or csv
    if path.endswith(".xlsx"):
        frame=pd.read_excel(path, sheet_name=0, header=0)
    elif path.endswith(".tsv"):
        frame=pd.read_csv(path, sep='\t')
    elif path.endswith(".csv"):
        frame=pd.read_csv(path)
    return frame

def __expandFrameOnDelimiter(frame, column):
    """ Splits file rows by a ';' delimiter
    Input:
        frame - the pandas frame to split
        column - the column name the is being split by the ';'
    Output:
        expandedFrame - a frame with every value that contained 
            a ';' character in the given column on new rows. 
            This frame has all other values still present.
    """
    # Convert the string to a list by taking a dictionary of key-value pairs and
    # unpacking into keyword arguments in a function call. The frame's internals 
    # are thus split by the ';' character.
    listColumn = frame.assign(**{column:frame[column].str.split(';')})
    # Created a vectorized funcion which uses numpy's repeat. This function to 
    # transform each element of a list-like to a row, replicating index values. 
    # Indexes will be duplicated for the rows that are expanded.
    # Todo - Convert to explode pandas function later since it's easier to read
    expandedFrame = pd.DataFrame({
                        col:np.repeat(listColumn[col].values, listColumn[column].str.len())
                        for col in listColumn.columns.difference([column])
                    }).assign(**{column:np.concatenate(listColumn[column].values)})[listColumn.columns.tolist()]
    return expandedFrame

def __replaceValuesWithDelimiter(rawFile, column, accName, index):
    """ Converts all ID on the same line. This retains the 
    condensed format of a file if desired.
    Input: 
        rawFile - the file to convert
        accName - nameof accession column
        column - the name of the column to convert
        index - The index file to get the conversion table from
    Output:
         A log file of Id that couldn't be converted
         A converted pandas frame
    """
    # Compare the lists to the column in pandas.
    accessions = list(index[accName])
    ids = list(index[column])
    rawIDs = list(rawFile[column])
    # Create a log for the id not found.
    failLog = open("idNotConverted.log", 'w')
    failLog.write("id_or_accession_not_converted\n")

    # Replace the values in the rawIDs list with the values in the index.
    replacement=rawIDs
    for i in range(0,len(ids)):
        if not pd.isna(ids[i]) and not pd.isna(accessions[i]):
            replacement=[x.replace(ids[i],accessions[i]) for x in replacement]
        else:
            failLog.write(str(ids[i])+'_'+str(accessions[i])+'\n')
    
    # Double check everything was converted, should never fail.
    assert(len(rawIDs) == len(replacement))
    failLog.close()

    rawFile[column] = replacement

    return rawFile

def main(args):
    # Read files intp pandas dataframe
    index = __readInFile(args.index)
    rawFile = __readInFile(args.file)
    if not args.column is None:
        # Get the column that has the sample ID in them
        columnFrame = rawFile[args.column]
        # Rename the column for the index to match
        index.rename(columns = {args.idName:args.column}, inplace = True)
        # explode the semicolon seperators onto new lines. The assign function
        # allows for the columns with a ';' to be converted into a list and then
        # exploded out onto different lines.
        expandedIndex = __expandFrameOnDelimiter(index, args.accName)
        expandedRaw = rawFile
        if args.split == "True":
            expandedRaw = __expandFrameOnDelimiter(rawFile, args.column)
            # Merge the data frames together and convert the series into dataframe
            bigDataFile = pd.merge(expandedIndex, expandedRaw, on=args.column)
            bigDataFile[args.column] = bigDataFile[args.accName]
            # May need to drop all header columns but args.idName here to get same file out
            out = bigDataFile.drop(columns=[args.accName])
            # Write out the file
            out.to_csv(args.output, index=False, sep='\t')
        else:
            expandedRaw = __replaceValuesWithDelimiter(rawFile, args.column, args.accName, expandedIndex)
            expandedRaw.to_csv(args.output, index=False, sep='\t')
    else:
        # No column name provided
        print("No column name was provided to filter on, assuming values in header")
        header = [x for x in list(rawFile.columns)]
        samples = [x for x in header if x in list(index[args.idName])]
        # Subset by samples found in header
        overlapFrame = index[index[args.idName].isin(samples)]
        # Get only a list of the accessions and replace the values in the pandas frame
        # header to be the accessions.
        accessions = list(overlapFrame[args.accName])
        header=header[0:abs(len(accessions)-len(header))] + accessions
        rawFile.columns = header
        # Write out the file
        rawFile.to_csv(args.output, index=False, sep='\t')
    print("Done")

if __name__ == "__main__":
    parser = ap.ArgumentParser()
    # Import the arguments from the user
    parser.add_argument("--file","-f", type=str, required=True, 
        help="Input file containing the unindexed IDs")
    parser.add_argument("--index","-i", type=str, required=True,
        help="File containing the column with a set of indentifers to be linked")
    parser.add_argument("--column","-c", type=str, required=False,
                help="Name of column that the ids exist in the input file")
    parser.add_argument("--idName","-in", type=str, required=False,
        help="Name of column that the ids exist in the index file")
    parser.add_argument("--accName","-an", type=str, required=False,
        help="Name of column that Accessions exist in the index file")
    parser.add_argument("--output","-o", type=str, required=True,
        help="Name of file to be output")
    parser.add_argument("--split","-s", type=str, required=False, default="Flase",
            help="Boolean for if ID representing on value should be kept condensed (False) or split onto seperate likes (True)")
    args = parser.parse_args()
    main(args)





