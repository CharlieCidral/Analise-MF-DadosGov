import os
import shutil
import concurrent.futures

# specify the directory you want to decompress
or_path = "D:/AnaliseGov/Faz/"
des_path = "D:/AnaliseGov/Faz_Decompress/"

# function to decompress a file
def decompress_file(filename):
    # construct full item path
    item_path = os.path.join(or_path, filename)
    print(item_path)
    # construct output path
    if filename[0:2] == 'Em':
        print(f"EM: {filename[0:2]}")
        output_path = os.path.join(des_path + 'Empresas/', filename[:-4] + "_decompressed")
        print(output_path)
    elif filename[0:2] == 'Es':
        print(f"ES: {filename[0:2]}")
        output_path = os.path.join(des_path + 'Estabelecimentos/', filename[:-4] + "_decompressed")
        print(output_path)
    elif filename[0:2] == 'So':
        output_path = os.path.join(des_path + 'Socios/', filename[:-4] + "_decompressed")
        print(output_path)
    else:
        output_path = os.path.join(des_path, filename[:-4] + "_decompressed")
        print(output_path)
    # create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    # check if the file is a compressed file (e.g., .zip, .tar, .gz, etc.)
    if filename.endswith(".zip"):
        # decompress the file
        shutil.unpack_archive(item_path, output_path)

# create a pool of worker threads
with concurrent.futures.ThreadPoolExecutor() as executor:
    # get list of files in the directory
    filenames = os.listdir(or_path)
    # submit the function to the thread pool executor for each file
    executor.map(decompress_file, filenames)
