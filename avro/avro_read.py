from avro import schema, datafile, io
import pprint
 
OUTFILE_NAME = 'product.avro'
rec_reader = io.DatumReader()
 
df_reader = datafile.DataFileReader(
    open(OUTFILE_NAME),
    rec_reader
)

# Read all records stored inside
pp = pprint.PrettyPrinter()
 
for record in df_reader:
    print("--------")
    pp.pprint(record)