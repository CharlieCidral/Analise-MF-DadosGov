# Projeto de Análise do Cadastro Nacional da Pessoa Jurídica (CNPJ)
Este projeto é uma ferramenta de análise de dados que lê arquivos CSV do Cadastro Nacional da Pessoa Jurídica (CNPJ) disponíveis no portal de dados abertos do governo brasileiro dados.gov.br.

O código contém uma lógica para extrair todos os dados de forma automática do arquivo `decompress.py`. Em seguida, os dados são extraídos de três bases diferentes:

1.A função `get_df_estab` busca as informações gerais das empresas, como ‘CNPJ BÁSICO’, ‘NOME FANTASIA’, ‘CEP’, ‘UF’, ‘CNAE’ e ‘SITUAÇÃO CADASTRAL’.

2.A função `get_df_company` busca informações como ‘CNPJ BÁSICO’ e ‘CAPITAL SOCIAL DA EMPRESA’.

3.A função `get_df_cnae` busca a descrição do CNAE.

Após a extração dos dados, a função `merge_comp_estab_cnae` realiza a transformação dos dados e carrega-os em gráficos para uma visualização mais intuitiva.

### As bibliotecas utilizadas neste projeto incluem:

- `tqdm` para a barra de progresso
- `pandas` para manipulação e análise de dados
- `os` para interação com o sistema operacional
- `concurrent.futures` para execução paralela
- `time` para operações de tempo
- `matplotlib.pyplot` e `seaborn` para visualização de dados
- `shutil` para operações de arquivo de alto nível

## Analises Iniciais:

![Top 10 Capital Social por CNPJ](https://github.com/CharlieCidral/Analise-MF-DadosGov/assets/69029099/8f35a26c-d2c3-4dff-b7be-3b036aa5ef02)

![Capital Social por UF](https://github.com/CharlieCidral/Analise-MF-DadosGov/assets/69029099/4dc46252-d48f-41e0-b58f-ba740faa7b51)

![top 10 estabelecimentos por CNPJ](https://github.com/CharlieCidral/Analise-MF-DadosGov/assets/69029099/695e0574-fa1e-49f3-b3c8-10e868f19762)


Este projeto é uma ferramenta valiosa para qualquer pessoa interessada em analisar dados relacionados a empresas brasileiras de maneira eficiente e eficaz. Ele automatiza muitas das tarefas tediosas associadas à limpeza e transformação de dados, permitindo que os usuários se concentrem na análise e interpretação dos resultados.
