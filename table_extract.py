import pandas as pd
import os

or_path = "D:/AnaliseGov/Faz_Decompress/"

def print_column_names(or_path):
    for folder in os.listdir(or_path):
        print(f"folder: {folder}")
        folder_paths = os.path.join(or_path, folder)
        print(f"folder_paths: {folder_paths}")
        for file in os.listdir(folder_paths):
            print(f"file: {file}")
            item_path = os.path.join(folder_paths, file)
            print(f"item_path: {item_path}")
            try:
                df = pd.read_csv(item_path, nrows=0, sep=';', encoding='ISO-8859-1' )  # read only the header
                columns_list = df.columns.tolist() #older comment code
                print(f"Column names for {item_path}: {columns_list}") #older comment code
                for column in df.columns:
                    print(f"Column {column} type: {df[column].dtype}")
                # items = columns_list[0].split(',')
                # print(items)
                # for item in items:
                #     print(item)
                #     print(type(item))
            except UnicodeDecodeError:
                print(f"Could not read file {item_path} with encoding 'ISO-8859-1'")

print_column_names(or_path)
