import pandas
import duckdb
import pandas as pd

PIB_municipios = pandas.read_excel("/Users/marcoshernan/Documents/Pessoal/Carreira/Orbital "
                                   "- engenheiro de dados/orbital_teste_engenheiro_de_dados/01_Pib_Municipios_tabelas_completas.xlsx",
                                   sheet_name=None)

state_slugs = {"Brasil": "Brasil",
              "Norte": "Norte",
              "Rondônia": "RO",
              "Acre": "AC",
              "Amazonas": "AM",
              "Roraima": "RR",
              "Pará": "PA",
              "Amapá": "AP",
              "Tocantins": "TO",
              "Nordeste": "Nordeste",
              "Maranhão": "MA",
              "Piauí": "PI",
              "Ceará": "CE",
              "Rio Grande do Norte": "RN",
              "Paraíba": "PB",
              "Pernambuco": "PE",
              "Alagoas": "AL",
              "Sergipe": "SE",
              "Bahia": "BA",
              "Sudeste": "Sudeste",
              "Minas Gerais": "MG",
              "Espírito Santo": "ES",
              "Rio de Janeiro": "RJ",
              "São Paulo": "SP",
              "Sul": "SP",
              "Paraná": "PR",
              "Santa Catarina": "SC",
              "Rio Grande do Sul": "RS",
              "Centro-Oeste": "Centro-Oeste",
              "Mato Grosso do Sul": "MS",
              "Mato Grosso": "MT",
              "Goiás": "GO",
              "Distrito Federal": "DF"
              }
region_slug = {"Brasil": "Brasil",
               "Norte": "Norte",
               "RO": "Norte",
               "AC": "Norte",
               "AM": "Norte",
               "RR": "Norte",
               "PA": "Norte",
               "AP": "Norte",
               "TO": "Norte",
               "Nordeste": "Nordeste",
               "MA": "Nordeste",
               "PI": "Nordeste",
               "CE": "Nordeste",
               "RN": "Nordeste",
               "PB": "Nordeste",
               "PE": "Nordeste",
               "AL": "Nordeste",
               "SE": "Nordeste",
               "BA": "Nordeste",
               "Sudeste": "Sudeste",
               "MG": "Sudeste",
               "ES": "Sudeste",
               "RJ": "Sudeste",
               "SP": "Sudeste",
               "Sul": "Sul",
               "PR": "Sul",
               "SC": "Sul",
               "RS": "Sul",
               "Centro-Oeste": "Centro-Oeste",
               "MS": "Centro-Oeste",
               "MT": "Centro-Oeste",
               "GO": "Centro-Oeste",
               "DF": "Centro-Oeste"
               }


def create_treated_dataframes_PIB_municipios():
    general_header = ["city", "ranking", "GDP", "region_participation", "region_participation_accumulated"]
    table_4_header = ["city", "ranking", "GDP_per_capita", "population"]
    table_9_header = ["state"] + [f'(%) {i}' for i in range(2002, 2020)] + \
                     ["participation_state_cities", "participation_population"]

    sheet_name_map = {"Tabela 1": "Top city's gdp",
                      "Tabela 2": "Top city's gdp by region",
                      "Tabela 3": "Lowest city's gdp by region",
                      "Tabela 4": "Top per capita GDP by region",
                      "Tabela 5": "Top cities - Cropping_Livestock",
                      "Tabela 6": "Top cities - Industry",
                      "Tabela 7": "Top cities - Services",
                      "Tabela 8": "Top cities - Others",
                      "Tabela 9": "Top 5 cities over state GDP"}

    # writer = pd.ExcelWriter("/Users/marcoshernan/Documents/Pessoal/Carreira/Orbital - "
    #                         "engenheiro de dados/orbital_teste_engenheiro_de_dados/Após tratamento/"
    #                         "Tabelas 1/PIB_municipios.xlsx", engine='xlsxwriter')

    path = "/Users/marcoshernan/Documents/Pessoal/Carreira/Orbital - " \
           "engenheiro de dados/orbital_teste_engenheiro_de_dados/Após tratamento/Tabelas 1"

    for table in PIB_municipios:
        if table != "Tabela 4" and table != "Tabela 9":
            df = PIB_municipios[table]  # Retira os títulos
            df = df.set_axis(general_header, axis=1)  # Adiciona o header
            df["GDP"] = pd.to_numeric(df['GDP'], errors='coerce')  # Linhas não numéricas -> NaN
            df = df[df["GDP"].notna()]  # Elimina linhas desnecessárias
            df = df[df["city"].str.contains(
                "Nordeste") == False]  # Nordeste não apresentava valores NaN como outras regiões
            df = df.reset_index(drop=True)

            df['state_slug'] = df['city'].str.extract(r'\((\w{2})\)')
            df["region"] = df["state_slug"].map(region_slug)
            df.insert(1, 'state_slug', df.pop('state_slug'))
            df.insert(2, 'region', df.pop('region'))

            df.to_csv(path + f'/{sheet_name_map[table]}.csv', index=False)

        elif table == "Tabela 4":
            df = PIB_municipios[table]  # Retira os títulos
            df = df.set_axis(table_4_header, axis=1)  # Adiciona o header
            df["GDP_per_capita"] = pd.to_numeric(df['GDP_per_capita'], errors='coerce')  # Linhas não numéricas -> NaN
            df = df[df["GDP_per_capita"].notna()]  # Elimina linhas desnecessárias
            df = df.reset_index(drop=True)

            df['state_slug'] = df['city'].str.extract(r'\((\w{2})\)')
            df["region"] = df["state_slug"].map(region_slug)
            df.insert(1, 'state_slug', df.pop('state_slug'))
            df.insert(2, 'region', df.pop('region'))

            df.to_csv(path + f'/{sheet_name_map[table]}.csv', index=False)

        elif table == "Tabela 9":
            df = PIB_municipios[table]  # Retira os títulos
            df = df.set_axis(table_9_header, axis=1)  # Adiciona o header
            df["participation_state_cities"] = pd.to_numeric(df["participation_state_cities"], errors='coerce')  # Linhas não numéricas -> NaN
            df = df[df["participation_state_cities"].notna()]  # Elimina linhas desnecessárias
            df = df.reset_index(drop=True)

            df['state_only'] = df['state'].str.replace(r'\(.*\)', '').str.strip()
            df['state_slug'] = df['state_only'].map(state_slugs)
            df["region"] = df["state_slug"].map(region_slug)
            df.insert(1, 'state_slug', df.pop('state_slug'))
            df.insert(2, 'region', df.pop('region'))

            df.to_csv(path + f'/{sheet_name_map[table]}.csv', index=False)

    # writer.save()


create_treated_dataframes_PIB_municipios()
