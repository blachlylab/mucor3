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

def readInFile(path):
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

def expandFrameOnDelimiter(frame, column):
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
    expandedFrame = frame
    if not type(column) is list:
        column = [column]
    for c in column:
        listColumn = expandedFrame.assign(**{c:frame[c].str.split(';')})
        # Created a vectorized funcion which uses numpy's repeat. This function to 
        # transform each element of a list-like to a row, replicating index values. 
        # Indexes will be duplicated for the rows that are expanded.
        # Todo - Convert to explode pandas function later since it's easier to read
        expandedFrame = pd.DataFrame({
                            col:np.repeat(listColumn[col].values, listColumn[c].str.len())
                            for col in listColumn.columns.difference([c])
                        }).assign(**{c:np.concatenate(listColumn[c].values)})[listColumn.columns.tolist()]
    return expandedFrame

def replaceValuesWithDelimiter(rawFile, column, accName, index):
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

def main():
    parser = ap.ArgumentParser()
    # Import the arguments from the user
    parser.add_argument("--file","-f", type=str, required=True, 
        help="Input file containing the unindexed IDs")
    parser.add_argument("--index","-i", type=str, required=True,
        help="File containing the column with a set of indentifers to be linked")
    parser.add_argument("--column","-c", type=str, required=False,
                help="Name of column/s that the ids exist in the input file")
    parser.add_argument("--idName","-in", type=str, required=False,
        help="Name of column that the ids exist in the index file")
    parser.add_argument("--accName","-an", type=str, required=False,
        help="Name of column/s that Accessions exist in the index file")
    parser.add_argument("--output","-o", type=str, required=True,
        help="Name of file to be output")
    parser.add_argument("--split","-s", type=str, required=False, default="Flase",
            help="Boolean for if ID representing on value should be kept condensed (False) or split onto seperate likes (True)")
    args = parser.parse_args()
    
    # Read files intp pandas dataframe
    index = readInFile(args.index)
    rawFile = readInFile(args.file)
    if not args.column is None:
        # Get the column that has the sample ID in them
        columnFrame = rawFile[args.column]
        idNameLST = args.idName.split(',')
        accNameLST = args.accName.split(',')
        # explode the semicolon seperators onto new lines. The assign function
        # allows for the columns with a ';' to be converted into a list and then
        # exploded out onto different lines.
        expandedIndex = expandFrameOnDelimiter(index, accNameLST)
        # Rename the columns for the index to match so that they may be combined below if
        # if they're are multiple columns being used for the index row.
        for idName in idNameLST:
            expandedIndex.rename(columns = {idName:args.column}, inplace = True)
        for accName in accNameLST:
            expandedIndex.rename(columns = {accName:"accession"}, inplace = True)
        # De-duplicates the columns by adding a unique id then stack the data. 
        if len(idNameLST) > 1 or len(accNameLST) > 1:
            expandedIndex = (expandedIndex.set_axis(pd.MultiIndex.from_arrays([expandedIndex.columns,
                                                                                expandedIndex.groupby(level=0, axis=1).cumcount()
                                                                                ]), axis=1)
                                .stack(level=1)
                                .sort_index(level=1)
                                .droplevel(1)
                                .drop_duplicates(subset=expandedIndex.columns[expandedIndex.columns.duplicated()])
                            )
        expandedIndex = expandedIndex[[args.column, "accession"]]

        expandedRaw = rawFile
        if args.split == "True":
            expandedRaw = expandFrameOnDelimiter(rawFile, args.column)
            # Merge the data frames together and convert the series into dataframe    
            bigDataFile = pd.merge(expandedIndex, expandedRaw, on=args.column)
            bigDataFile[args.column] = bigDataFile["accession"]
            out = bigDataFile.drop(columns=["accession"])
            # Write out the file
            out.to_csv(args.output, index=False, sep='\t')
        else:
            replaceValuesWithDelimiter(rawFile, args.column, "accession", expandedIndex)
            expandedRaw.to_csv(args.output, index=False, sep='\t')
    else:
        rawFile.reset_index(inplace=True)
        # No column name provided
        print("No column name was provided to filter on, assuming values in header")
        idNameLST = args.idName.split(',')
        accNameLST = args.accName.split(',')
        header = [x for x in list(rawFile.columns)]
        samples = []
        for idName in idNameLST:
            samples = samples + [x for x in header if x in list(index[idName])]
        samples = list(set(samples))
        # Subset by samples found in header
        overlapFrame = index
        for idName in idNameLST:
            overlapFrame = overlapFrame[overlapFrame[idName].isin(samples)]
        # Get only a list of the accessions and replace the values in the pandas frame
        # header to be the accessions.
        accessions = overlapFrame
        for accName in accNameLST:
            accessions = list(overlapFrame[accName])
        header=header[0:abs(len(accessions)-len(header))] + accessions
        rawFile.columns = header
        # Write out the file
        rawFile.to_csv(args.output, index=False, sep='\t')
    print("Done")

if __name__ == "__main__":
    main()





