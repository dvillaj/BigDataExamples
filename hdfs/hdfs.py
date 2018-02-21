from hdfs3 import HDFileSystem
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('directory', help="Directorio donde se ubican los datos en HDFS")
args = parser.parse_args()

if args.directory is None:
    parser.error("Es necesario especificar la ruta donde se encuentran los datos en HDFS!")
    sys.exit(1)

hdfs = HDFileSystem(host='localhost', port=8020)
for file in hdfs.ls(args.directory):
    print("File %s" % file)
