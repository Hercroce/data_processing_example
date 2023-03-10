import pandas

import pandas as pd

path = "//Users/marcoshernan/Documents/Pessoal/Carreira/Orbital - " \
       "engenheiro de dados/orbital_teste_engenheiro_de_dados"

sheet_name_map = {"Tab 5_20210817": "Property - City size",
                  "Tab 6_20210817": "Property - State",
                  "Tab 7": "Tax - City size",
                  "Tab 8": "Tax - State",
                  "Tab 9": "Incentives - City size",
                  "Tab 10": "Incentives - State",
                  "Tab 11": "Restrictions - City size",
                  "Tab 12": "Restrictions - State",
                  "Tab 13": "Destatization - City size",
                  "Tab 14": "Destatization - State"}

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
              "Sul": "Sul",
              "Paraná": "PR",
              "Santa Catarina": "SC",
              "Rio Grande do Sul": "RS",
              "Centro-Oeste": "Centro-Oeste",
              "Mato Grosso do Sul": "MS",
              "Mato Grosso": "MT",
              "Goiás": "GO",
              "Distrito Federal": "DF"
              }

docs_list_real_estate = ["Tab 5_20210817", "Tab 6_20210817"]
docs_list_tax = ["Tab 7", "Tab 8"]
docs_list_incentive = ["Tab 9", "Tab 10"]
docs_list_restrictions = ["Tab 11", "Tab 12"]
docs_list_destatization = ["Tab 13", "Tab 14"]

header_real_estate = ["Region_Size", "nº_cities", "registered", "computerized", "geo_referenced", "public_access",
                      "update_1_year_or_less", "update_1_year_or_less", "update_on_demand",
                      "properties", "residential_properties", "non_residential", "with_iptu", "value_plant",
                      "value_plant_informatized", "value_plant_last_10_years", "isqn", "issqn_informatized"]

header_tax = ["Region_Size", "nº_cities", "light", "trash", "fire", "cleaning", "police", "other", "no_tax"]

header_incentive = ["Region_Size", "nº_cities", "applied_incentives", "iptu_reduction", "iptu_exemption",
                    "issqn_reduction", "issqn_exemption", "fee_exemption", "property_cession", "property_donation",
                    "other_incentives", "industrial", "commerce_services", "turism_sports_leisure",
                    "cropping_livestock", "other_sectors",
                    "2018_industrial", "2018_commerce_services", "2018_turism_sports_leisure",
                    "2018_cropping_livestock", "2018_other_sectors",
                    "no_sector"]

header_restrictions = ["Region_Size", "nº_cities", "applied_restrictions", "legislation", "taxation",
                       "other_mechanisms", "industry", "extractive_industry", "commerce_services",
                       "turism_sports_leisure", "environmental_impact_industries", "other",
                       "2018_industry", "2018_extractive_industry", "2018_commerce_services",
                       "2018_turism_sports_leisure", "2018_environmental_impact_industries", "2018_other",
                       "2018_no_enterprise"]

header_destatization = ["Region_Size", "nº_cities", "iniciatives_last_2_years", "property_sale",
                        "privatization", "concession", "commom_concession", "private_partnership",
                        "administrative_concession", "sponsored_concession", "culture_related", "education",
                        "maintenance", "sewer", "health", "funeral_services", "transportation", "others"]


def create_treated_dataframes():
    # writer = pd.ExcelWriter(path + "/Após tratamento/Tabelas 2/recursos_gestao.xlsx", engine='xlsxwriter')

    for table_name in sheet_name_map.keys():
        if table_name in docs_list_real_estate:
            df = pandas.read_excel(path + f'/02_Recursos_para_a_gestao_20210817/{table_name}.xls')
            df = df.set_axis(header_real_estate, axis=1)  # Adiciona o header
            df["nº_cities"] = pd.to_numeric(df["nº_cities"], errors='coerce')  # Linhas não numéricas -> NaN
            df = df[df["nº_cities"].notna()]  # Elimina linhas desnecessárias
            df = df.reset_index(drop=True)  # Reseta o index
            df["state_slugs"] = df["Region_Size"].map(state_slugs)  # Adiciona uma coluna com os slugs de estados
            df.insert(1, 'state_slugs', df.pop('state_slugs'))

            df.to_csv(path + f'/Após tratamento/Tabelas 2/{sheet_name_map[table_name]}.csv', index=False)
            # df.to_excel(writer, sheet_name=sheet_name_map[table_name])

        elif table_name in docs_list_tax:
            df = pandas.read_excel(path + f'/02_Recursos_para_a_gestao_20210817/{table_name}.xls')
            df = df.set_axis(header_tax, axis=1)  # Adiciona o header
            df["nº_cities"] = pd.to_numeric(df["nº_cities"], errors='coerce')  # Linhas não numéricas -> NaN
            df = df[df["nº_cities"].notna()]  # Elimina linhas desnecessárias
            df = df.reset_index(drop=True)
            df["state_slugs"] = df["Region_Size"].map(state_slugs)  # Adiciona uma coluna com os slugs de estados
            df.insert(1, 'state_slugs', df.pop('state_slugs'))

            df.to_csv(path + f'/Após tratamento/Tabelas 2/{sheet_name_map[table_name]}.csv', index=False)
            # df.to_excel(writer, sheet_name=sheet_name_map[table_name])

        elif table_name in docs_list_incentive:
            df = pandas.read_excel(path + f'/02_Recursos_para_a_gestao_20210817/{table_name}.xls')
            df = df.set_axis(header_incentive, axis=1)  # Adiciona o header
            df["nº_cities"] = pd.to_numeric(df["nº_cities"], errors='coerce')  # Linhas não numéricas -> NaN
            df = df[df["nº_cities"].notna()]  # Elimina linhas desnecessárias
            df = df.reset_index(drop=True)
            df["state_slugs"] = df["Region_Size"].map(state_slugs)  # Adiciona uma coluna com os slugs de estados
            df.insert(1, 'state_slugs', df.pop('state_slugs'))

            df.to_csv(path + f'/Após tratamento/Tabelas 2/{sheet_name_map[table_name]}.csv', index=False)
            # df.to_excel(writer, sheet_name=sheet_name_map[table_name])

        elif table_name in docs_list_restrictions:
            df = pandas.read_excel(path + f'/02_Recursos_para_a_gestao_20210817/{table_name}.xls')
            df = df.set_axis(header_restrictions, axis=1)  # Adiciona o header
            df["nº_cities"] = pd.to_numeric(df["nº_cities"], errors='coerce')  # Linhas não numéricas -> NaN
            df = df[df["nº_cities"].notna()]  # Elimina linhas desnecessárias
            df = df.reset_index(drop=True)
            df["state_slugs"] = df["Region_Size"].map(state_slugs)  # Adiciona uma coluna com os slugs de estados
            df.insert(1, 'state_slugs', df.pop('state_slugs'))

            df.to_csv(path + f'/Após tratamento/Tabelas 2/{sheet_name_map[table_name]}.csv', index=False)
            # df.to_excel(writer, sheet_name=sheet_name_map[table_name])

        elif table_name in docs_list_destatization:
            df = pandas.read_excel(path + f'/02_Recursos_para_a_gestao_20210817/{table_name}.xls')
            df = df.set_axis(header_destatization, axis=1)  # Adiciona o header
            df["nº_cities"] = pd.to_numeric(df["nº_cities"], errors='coerce')  # Linhas não numéricas -> NaN
            df = df[df["nº_cities"].notna()]  # Elimina linhas desnecessárias
            df = df.reset_index(drop=True)
            df["state_slugs"] = df["Region_Size"].map(state_slugs)  # Adiciona uma coluna com os slugs de estados
            df.insert(1, 'state_slugs', df.pop('state_slugs'))

            df.to_csv(path + f'/Após tratamento/Tabelas 2/{sheet_name_map[table_name]}.csv', index=False)
            # df.to_excel(writer, sheet_name=sheet_name_map[table_name])

    # writer.save()


create_treated_dataframes()
