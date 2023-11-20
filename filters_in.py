from tqdm import tqdm
import pandas as pd
import os
import concurrent.futures
import time
import matplotlib.pyplot as plt
import seaborn as sns

company_path = "D:/AnaliseGov/Faz_Decompress/Empresas/"
estab_path = "D:/AnaliseGov/Faz_Decompress/Estabelecimentos/"
cnae_path = "D:/AnaliseGov/Faz_Decompress/Cnaes_decompressed/F.K03200$Z.D31014.CNAECSV"

def get_df_estab(estab_path):
    column_names = ['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV', 'IDENTIFICADOR MATRIZ', 'NOME FANTASIA', 'SITUAÇÃO CADASTRAL', 'DATA SITUAÇÃO CADASTRAL', 
                    'MOTIVO SITUAÇÃO CADASTRAL', 'NOME DA CIDADE NO EXTERIOR', 'PAIS', 'DATA DE INÍCIO ATIVIDADE', 'CNAE', 'CNAE FISCAL SECUNDÁRIA',
                    'TIPO DE LOGRADOURO', 'LOGRADOURO', 'NÚMERO', 'COMPLEMENTO', 'BAIRRO', 'CEP', 'UF', 'MUNICÍPIO', 'DDD 1', 'TELEFONE 1', 'DDD 2', 'TELEFONE 2',
                    'DDD DO FAX', 'FAX', 'CORREIO ELETRÔNICO', 'SITUAÇÃO ESPECIAL', 'DATA DA SITUAÇÃO ESPECIAL']
    column_types = {'CNPJ BÁSICO': str, 'CNPJ ORDEM': str, 'CNPJ DV': str, 'IDENTIFICADOR MATRIZ': str, 'NOME FANTASIA': str, 'SITUAÇÃO CADASTRAL': str, 'DATA SITUAÇÃO CADASTRAL': str,
                    'MOTIVO SITUAÇÃO CADASTRAL': str, 'NOME DA CIDADE NO EXTERIOR': str, 'PAIS': str, 'DATA DE INÍCIO ATIVIDADE': str, 'CNAE': str,
                    'CNAE FISCAL SECUNDÁRIA': str, 'TIPO DE LOGRADOURO': str, 'LOGRADOURO': str, 'NÚMERO': str, 'COMPLEMENTO': str, 'BAIRRO': str, 'CEP': str, 'UF': str,
                    'MUNICÍPIO': str, 'DDD 1': str, 'TELEFONE 1': str, 'DDD 2': str, 'TELEFONE 2': str, 'DDD DO FAX': str, 'FAX': str, 'CORREIO ELETRÔNICO': str,
                    'SITUAÇÃO ESPECIAL': str, 'DATA DA SITUAÇÃO ESPECIAL': str}
    for folder_estab in tqdm(os.listdir(estab_path)):
        # print(f"folder: {folder_estab[0:2]}")
        if folder_estab[0:2] == "Es":
            folder_estab_paths = os.path.join(estab_path, folder_estab)
            # print(f"folder_paths: {folder_estab_paths}")
            for file_estab in os.listdir(folder_estab_paths):
                # print(f"file: {file_estab}")
                estab_path_file = os.path.join(folder_estab_paths, file_estab)
                # print(f"item_path: {estab_path_file}")
                try:
                    chunks = []
                    chunksize = 10 ** 6  # adjust this value depending on your available memory
                    for chunk in pd.read_csv(estab_path_file, header=None, encoding='ISO-8859-1', on_bad_lines='warn', sep=';', chunksize=chunksize, usecols=['CNPJ BÁSICO', 'NOME FANTASIA', 'CEP', 'UF', 'CNAE', 'SITUAÇÃO CADASTRAL'], names=column_names, dtype=column_types):  # read only the header
                        chunks.append(chunk)
                        df_uf = pd.concat(chunks, axis=0)
                        # df_cnpj_estab = pd.to_numeric(df_estab['CNPJ BÁSICO'], errors='coerce')  # convert column to numeric CNPJ
                        # print(df_uf)
                        # COUNT ALL LINES
                        # count_bef = df_uf['CNPJ BÁSICO'].count()
                        # count_total_estab += count_bef
                except UnicodeDecodeError:
                    print(f"Could not read file {estab_path_file} with encoding 'ISO-8859-1'")
    # print(f"Total CNPJ Before Estab: {count_total_estab}")
    return df_uf

def get_df_company(company_path):
    column_names = ['CNPJ BÁSICO', 'RAZÃO SOCIAL', 'NATUREZA JURÍDICA', 'QUALIFICAÇÃO DO RESPONSÁVEL', 'CAPITAL SOCIAL DA EMPRESA', 'PORTE DA EMPRESA', 'ENTE FEDERATIVO RESPONSÁVEL']
    column_types = {'CNPJ BÁSICO': str,'RAZÃO SOCIAL': str,'NATUREZA JURÍDICA': str,'QUALIFICAÇÃO DO RESPONSÁVEL': str, 'CAPITAL SOCIAL DA EMPRESA': str, 'PORTE DA EMPRESA': str,'ENTE FEDERATIVO RESPONSÁVEL': str}
    for folder_company in tqdm(os.listdir(company_path)):
        # print(f"folder: {folder_company[0:2]}")
        if folder_company[0:2] == "Em":
            folder_company_paths = os.path.join(company_path, folder_company)
            for file_company in os.listdir(folder_company_paths):
                company_path_file = os.path.join(folder_company_paths, file_company)
                # print(f"item_path: {company_path_file}")
                try:
                    chunks = []
                    chunksize = 10 ** 6  # adjust this value depending on your available memory
                    for chunk in pd.read_csv(company_path_file, header=None, encoding='ISO-8859-1', on_bad_lines='warn', sep=';', chunksize=chunksize, usecols=['CNPJ BÁSICO', 'CAPITAL SOCIAL DA EMPRESA'], names=column_names, dtype=column_types):  # read the csv
                        # Concat the values
                        chunks.append(chunk)
                        df_company = pd.concat(chunks, axis=0)
                        # count_bef = df_company['CNPJ BÁSICO'].count()
                        # count_total += count_bef
                        # Convert the column to numeric
                        # df_company['CAPITAL SOCIAL DA EMPRESA'] = pd.to_numeric(df_company['CAPITAL SOCIAL DA EMPRESA'], errors='coerce')
                except UnicodeDecodeError:
                    print(f"Could not read file {company_path_file} with encoding 'ISO-8859-1'")        
    # print(f"Total CNPJ Before Company: {count_total}")
    return df_company

def get_df_cnae(cnae_path):
    column_names = ['CNAE', 'CNAE DESCRIÇÃO']
    column_types = {'CNAE': str, 'CNAE DESCRIÇÃO': str}
    try:
        df_cnae = pd.read_csv(cnae_path, header=None, encoding='ISO-8859-1', on_bad_lines='warn', sep=';', names=column_names, dtype=column_types)
    except UnicodeDecodeError:
        print(f"Could not read file {cnae_path} with encoding 'ISO-8859-1'")
    return df_cnae

def merge_comp_estab_cnae(df_company, df_uf, df_cnae):

    # Conta CEP por CNPJ
    df_company['ESTAB_COUNT'] = df_company.groupby('CNPJ BÁSICO')['CEP'].transform('count')

    # Assuming df_company duplicada?
    #duplicates = df_company[df_company.duplicated('CNPJ BÁSICO', keep=False)]

    # Agrupa os valores
    df_company_unique = df_company.drop_duplicates('CNPJ BÁSICO')
    print(f"The group: \n{df_company_unique}")

    # Calculate the total ESTAB_COUNT
    total_estab_count = df_company_unique['ESTAB_COUNT'].sum()
    print(f"Soma dos Estab.: \n{total_estab_count}")

    # Select the 10 largest values
    top_10 = df_company_unique.nlargest(10, 'ESTAB_COUNT')[['CNPJ BÁSICO', 'ESTAB_COUNT']]

    print(f"Top 10 : \n{top_10}")

    # Create a new column for the percentage
    top_10 = top_10.reset_index()
    top_10['ESTAB_PERCENTAGE'] = (top_10['ESTAB_COUNT'] * 100) / total_estab_count

    # Display the updated DataFrame
    print(top_10)

    top_10.reset_index(drop=True)

    # Gráfico de barras para top_10
    plt.figure(figsize=(10,5))
    plt.bar(top_10['CNPJ BÁSICO'], top_10['ESTAB_COUNT'])
    plt.title('Top 10 CNPJ com Maior numero de Estabelecimentos')
    plt.xlabel('CNPJ BÁSICO')
    plt.ylabel('ESTAB_COUNT')
    plt.show()

    # df_uf_filtered = df_uf[df_uf['SITUAÇÃO CADASTRAL'] == 2]
    df_merged = df_uf.merge(df_company, on='CNPJ BÁSICO', how='left')

    num_rows_uf = len(df_uf)
    num_comp_to_uf_merged = len(df_merged)
    num_rows_comp = len(df_company)

    print(f"uf: {num_rows_uf}, company: {num_rows_comp} and merged: {num_comp_to_uf_merged}")

    # merge with cnae
    df_merged = df_merged.merge(df_cnae, on='CNAE', how='left')

    # Check the number of rows after the merge
    num_rows_cnae_merge = len(df_merged)

    print(f"CNAE len merged: \n{num_rows_cnae_merge}")

    df_capital = df_merged[['CNPJ BÁSICO', 'CAPITAL SOCIAL DA EMPRESA','UF']]

    print(f"Depuração df_merged: Antes do conversão \n{df_capital}")

    # Converta a coluna 'CAPITAL SOCIAL DA EMPRESA' para números, se necessário
    df_capital['CAPITAL SOCIAL DA EMPRESA'] = pd.to_numeric(df_capital['CAPITAL SOCIAL DA EMPRESA'].str.replace(',', '.'), errors='coerce', downcast='float')

    print(f"Verificando após a conversão: \n{df_capital['CAPITAL SOCIAL DA EMPRESA']}")

    total_duplicatas  = len(df_capital) - len(df_capital.drop_duplicates('CNPJ BÁSICO'))

    with tqdm(total=total_duplicatas, desc="Dropping duplicates", unit="row") as pbar:
        # Dropar duplicatas com base na coluna especificada
        total_por_cnpj = df_capital.drop_duplicates('CNPJ BÁSICO').copy()
        # Atualizar a barra de progresso
        pbar.update(total_duplicatas)

    print(f"Depois do drop: \n{total_por_cnpj}")

    # Calcula a média de 'CAPITAL SOCIAL DA EMPRESA' por 'UF'
    df_merged_mean = total_por_cnpj.groupby('UF')['CAPITAL SOCIAL DA EMPRESA'].sum().reset_index()

    # Cria o scatterplot
    sns.scatterplot(data=df_merged_mean, x='UF', y='CAPITAL SOCIAL DA EMPRESA')
    plt.title('Relação entre o Total de CAPITAL SOCIAL por UF')
    plt.show()

    # Converta o objeto groupby em um DataFrame para visualização
    df_total_capital = total_por_cnpj['CAPITAL SOCIAL DA EMPRESA'].sum()

    print(f"Soma total_por_cnpj: \n{df_total_capital}")

    num_rows_total_por_cnpj = len(total_por_cnpj)
    print(f"Agrupado total_por_cnpj: \n{num_rows_total_por_cnpj}")

    # Pegue os 10 maiores valores de 'Total Capital Social' para cada grupo
    top_10_por_cnpj = total_por_cnpj.nlargest(10,'CAPITAL SOCIAL DA EMPRESA')[['CNPJ BÁSICO', 'CAPITAL SOCIAL DA EMPRESA']]

    print(f"top_10_por_cnpj: \n{top_10_por_cnpj}")

    top_10_por_cnpj = top_10_por_cnpj.reset_index()
    
    # Calcule o percentual em relação ao 'Total Capital Social' para cada linha
    top_10_por_cnpj['Percentual Top 10'] = (top_10_por_cnpj['CAPITAL SOCIAL DA EMPRESA'] * 100) / df_total_capital

    # Exiba o resultado
    print("Top 10 maiores valores de CAPITAL SOCIAL DA EMPRESA por CNPJ BÁSICO:")
    print(top_10_por_cnpj)

    # Gráfico de barras para top_10_por_cnpj
    plt.figure(figsize=(10,5))
    plt.bar(top_10_por_cnpj['CNPJ BÁSICO'], top_10_por_cnpj['CAPITAL SOCIAL DA EMPRESA'])
    plt.title('Top 10 CAPITAL SOCIAL por CNPJ')
    plt.xlabel('CNPJ BÁSICO')
    plt.ylabel('CAPITAL SOCIAL DA EMPRESA')
    plt.show()

# create groups for graphics 

# create plots for better resume analisys

with concurrent.futures.ThreadPoolExecutor() as executor:
    start_time = time.time()
    # Get the dataframes
    df_uf = get_df_estab(estab_path)
    df_company = get_df_company(company_path)
    df_cnae = get_df_cnae(cnae_path)

    executor.map(merge_comp_estab_cnae(df_uf, df_company, df_cnae))

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"The script took {execution_time/60} minutes to run.")